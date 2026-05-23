# backend/src/routes.py
import asyncio
import io
import logging
import os
import re
import zipfile
from typing import Optional

import boto3
import httpx
from botocore.exceptions import ClientError
from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, Request, UploadFile, status
from pydantic import BaseModel

from src.auth import get_current_user, validate_token
from src.aws import generate_presigned_url, get_task_status, run_user_task, stop_user_task
from src.db import (
    cleanup_expired_sessions,
    create_session,
    get_active_session,
    get_session_by_arn,
    heartbeat_session,
    mark_session_stopped,
    update_session_status,
)

logger = logging.getLogger(__name__)
router = APIRouter()

# =============================================================================
# Config
# =============================================================================
S3_PRESIGN_EXPIRES_SECONDS = int(os.getenv("S3_PRESIGN_EXPIRES_SECONDS", "300"))

ALLOWED_S3_BUCKETS = [
    b.strip()
    for b in os.getenv("ALLOWED_S3_BUCKETS", "notepadppfiles,cloudramsaas-vscode").split(",")
    if b.strip()
]

ALLOWED_PRESIGN_CONTENT_TYPES = [
    ct.strip()
    for ct in os.getenv(
        "ALLOWED_PRESIGN_CONTENT_TYPES",
        "application/octet-stream,application/zip,text/plain,application/json",
    ).split(",")
    if ct.strip()
]

AWS_REGION = os.getenv("AWS_REGION")
s3_client = boto3.client("s3", region_name=AWS_REGION) if AWS_REGION else boto3.client("s3")

VNC_PASSWORD = os.getenv("VNC_PW", "cloudramsaas_vnc")
VM_HTTP_TIMEOUT = int(os.getenv("VM_HTTP_TIMEOUT", "60"))
VM_API_KEY = os.getenv("VM_API_KEY", "")

MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", str(500 * 1024 * 1024)))  # 500 MB

_SAFE_PROJECT_NAME = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._\- ]{0,127}$")


def _validate_project_name(name: str) -> str:
    name = name.strip()
    if not name or not _SAFE_PROJECT_NAME.match(name):
        raise HTTPException(
            status_code=400,
            detail="Invalid project name. Use letters, digits, dots, hyphens, underscores, or spaces (max 128 chars).",
        )
    if ".." in name:
        raise HTTPException(status_code=400, detail="Invalid project name.")
    return name

_allocate_lock = asyncio.Lock()


def _build_novnc_url(ip: str, port: int) -> str:
    return f"http://{ip}:{port}/vnc.html?autoconnect=true&password={VNC_PASSWORD}&resize=scale"


async def _vm_request(session: dict, path: str, method: str = "POST", json_body: dict = None):
    vm_ip = session.get("private_ip")
    api_port = session.get("api_port", 7000)
    if not vm_ip:
        raise HTTPException(status_code=503, detail="VM IP not available yet")
    url = f"http://{vm_ip}:{api_port}{path}"
    headers = {}
    if VM_API_KEY:
        headers["X-VM-API-KEY"] = VM_API_KEY
    async with httpx.AsyncClient(timeout=VM_HTTP_TIMEOUT) as client:
        if method == "GET":
            resp = await client.get(url, headers=headers)
        else:
            resp = await client.post(url, json=json_body, headers=headers)
    if resp.status_code >= 400:
        try:
            detail = resp.json().get("detail", resp.text)
        except Exception:
            detail = resp.text
        raise HTTPException(status_code=resp.status_code, detail=detail)
    return resp.json()


def _require_user_scoped_key(user_id: str, key: str):
    expected_prefix = f"users/{user_id}/"
    if not key or not key.startswith(expected_prefix):
        raise HTTPException(
            status_code=403,
            detail=f"Invalid key scope. Key must start with '{expected_prefix}'",
        )


def _require_allowed_bucket(bucket: str):
    if bucket not in ALLOWED_S3_BUCKETS:
        raise HTTPException(
            status_code=403,
            detail=f"Bucket not allowed. Allowed: {', '.join(ALLOWED_S3_BUCKETS)}",
        )


def _require_allowed_content_type(content_type: str):
    ct = (content_type or "").strip()
    if ct not in ALLOWED_PRESIGN_CONTENT_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"content_type not allowed. Allowed: {', '.join(ALLOWED_PRESIGN_CONTENT_TYPES)}",
        )


# =============================================================================
# ECS poll helper
# =============================================================================
async def _poll_task_until_running(task_arn: str, user_id: str, max_attempts: int = 40):
    for attempt in range(max_attempts):
        await asyncio.sleep(5)
        info = await get_task_status(task_arn)
        if not info:
            continue

        ecs_status = info["status"]
        reachable_ip = info.get("public_ip") or info.get("private_ip")

        await update_session_status(
            task_arn=task_arn,
            status=ecs_status,
            private_ip=reachable_ip,
            novnc_port=info.get("novnc_port"),
            api_port=info.get("api_port"),
        )

        logger.info("Task %s → %s (attempt %d)", task_arn, ecs_status, attempt)

        if ecs_status == "RUNNING":
            await _restore_projects_from_s3(user_id, task_arn)
            return
        if ecs_status in ("STOPPED", "DEPROVISIONING"):
            await mark_session_stopped(task_arn, reason="ecs_stopped_unexpectedly")
            return

    await mark_session_stopped(task_arn, reason="provision_timeout")


async def _restore_projects_from_s3(user_id: str, task_arn: str):
    """Scan S3 for user's previously uploaded projects and restore them."""
    try:
        prefix = f"users/{user_id}/vscode/"
        response = await asyncio.to_thread(
            s3_client.list_objects_v2,
            Bucket="cloudramsaas-vscode",
            Prefix=prefix,
        )

        contents = response.get("Contents", [])
        project_keys = [
            obj["Key"] for obj in contents
            if obj["Key"].endswith(".zip") and "/_" not in obj["Key"] and "/exports/" not in obj["Key"]
        ]

        if not project_keys:
            logger.info("No projects to restore for user %s", user_id)
            return

        session = await get_session_by_arn(task_arn)
        if not session or not session.get("private_ip"):
            return

        vm_ip = session["private_ip"]
        api_port = session.get("api_port", 7000)
        headers = {}
        if VM_API_KEY:
            headers["X-VM-API-KEY"] = VM_API_KEY

        for key in project_keys:
            project_name = key.rsplit("/", 1)[-1].replace(".zip", "")
            config_key = f"users/{user_id}/vscode/_empty_config.zip"

            try:
                async with httpx.AsyncClient(timeout=VM_HTTP_TIMEOUT) as client:
                    resp = await client.post(
                        f"http://{vm_ip}:{api_port}/setup_ide",
                        json={
                            "user_id": user_id,
                            "project_name": project_name,
                            "ide": "vscode",
                            "project_s3_bucket": "cloudramsaas-vscode",
                            "project_s3_key": key,
                            "config_s3_bucket": "cloudramsaas-vscode",
                            "config_s3_key": config_key,
                        },
                        headers=headers,
                    )
                logger.info("Restored project '%s' for user %s (status %d)", project_name, user_id, resp.status_code)
            except Exception as e:
                logger.warning("Failed to restore project '%s' for user %s: %s", project_name, user_id, e)

    except Exception as e:
        logger.warning("S3 project restore failed for user %s: %s", user_id, e)


# =============================================================================
# Models
# =============================================================================
class SessionResponse(BaseModel):
    session_id: str
    task_arn: str
    status: str
    private_ip: Optional[str] = None

    novnc_port: int = 6080
    api_port: Optional[int] = None

    novnc_url: Optional[str] = None
    api_url: Optional[str] = None

    message: str = ""


class PresignedUrlRequest(BaseModel):
    filename: str
    operation: str = "get_object"


class S3SignPutRequest(BaseModel):
    user_id: str
    bucket: str
    key: str
    content_type: str = "application/octet-stream"


class S3SignGetRequest(BaseModel):
    user_id: str
    bucket: str
    key: str


# =============================================================================
# Sessions API
# =============================================================================
@router.post("/sessions/allocate", response_model=SessionResponse, status_code=201)
async def allocate_session(
    background_tasks: BackgroundTasks,
    user: dict = Depends(get_current_user),
):
    user_id = user["user_id"]

    async with _allocate_lock:
        return await _do_allocate(user_id, background_tasks)


async def _do_allocate(user_id: str, background_tasks: BackgroundTasks) -> SessionResponse:
    existing = await get_active_session(user_id)
    if existing:
        info = await get_task_status(existing["task_arn"])
        ecs_status = info["status"] if info else "UNKNOWN"

        if ecs_status in ("PROVISIONING", "PENDING", "RUNNING"):
            reachable_ip = (info.get("public_ip") or info.get("private_ip")) if info else existing.get("private_ip")
            novnc_port = (info.get("novnc_port") if info else None) or existing.get("novnc_port", 6080)
            api_port = (info.get("api_port") if info else None) or existing.get("api_port")

            novnc_url = (
                _build_novnc_url(reachable_ip, novnc_port)
                if ecs_status == "RUNNING" and reachable_ip
                else None
            )
            api_url = (
                f"http://{reachable_ip}:{api_port}"
                if ecs_status == "RUNNING" and reachable_ip and api_port
                else None
            )

            return SessionResponse(
                session_id=str(existing["id"]),
                task_arn=existing["task_arn"],
                status=ecs_status,
                private_ip=reachable_ip,
                novnc_port=novnc_port,
                api_port=api_port,
                novnc_url=novnc_url,
                api_url=api_url,
                message="Existing session returned",
            )

        await mark_session_stopped(existing["task_arn"], reason="ecs_task_gone")

    # Launch new task with random ports
    try:
        task_info = await run_user_task(user_id)
    except RuntimeError as e:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(e))

    task_arn = task_info["task_arn"]
    novnc_port = task_info.get("novnc_port", 6080)
    api_port = task_info.get("api_port")

    session = await create_session(user_id=user_id, task_arn=task_arn, novnc_port=novnc_port, api_port=api_port)
    background_tasks.add_task(_poll_task_until_running, task_arn, user_id)

    return SessionResponse(
        session_id=str(session["id"]),
        task_arn=task_arn,
        status="PROVISIONING",
        novnc_port=novnc_port,
        api_port=api_port,
        message="Session provisioning started. Poll /sessions/status for updates.",
    )


@router.get("/sessions/status", response_model=SessionResponse)
async def session_status(user: dict = Depends(get_current_user)):
    session = await get_active_session(user["user_id"])
    if not session:
        raise HTTPException(status_code=404, detail="No active session found")

    info = await get_task_status(session["task_arn"])
    if info:
        reachable_ip = info.get("public_ip") or info.get("private_ip") or session.get("private_ip")
        novnc_port = info.get("novnc_port") or session.get("novnc_port", 6080)
        api_port = info.get("api_port") or session.get("api_port")
        ecs_status = info["status"]

        await update_session_status(
            task_arn=session["task_arn"],
            status=ecs_status,
            private_ip=reachable_ip,
            novnc_port=novnc_port,
            api_port=api_port,
        )
    else:
        reachable_ip = session.get("private_ip")
        novnc_port = session.get("novnc_port", 6080)
        api_port = session.get("api_port")
        ecs_status = session["status"]

    novnc_url = (
        _build_novnc_url(reachable_ip, novnc_port)
        if ecs_status == "RUNNING" and reachable_ip
        else None
    )
    api_url = (
        f"http://{reachable_ip}:{api_port}"
        if ecs_status == "RUNNING" and reachable_ip and api_port
        else None
    )

    return SessionResponse(
        session_id=str(session["id"]),
        task_arn=session["task_arn"],
        status=ecs_status,
        private_ip=reachable_ip,
        novnc_port=novnc_port,
        api_port=api_port,
        novnc_url=novnc_url,
        api_url=api_url,
    )


@router.post("/sessions/heartbeat", status_code=204)
async def session_heartbeat(user: dict = Depends(get_current_user)):
    session = await get_active_session(user["user_id"])
    if not session:
        raise HTTPException(status_code=404, detail="No active session")
    await heartbeat_session(session["task_arn"])


@router.delete("/sessions", status_code=204)
async def stop_session(user: dict = Depends(get_current_user)):
    session = await get_active_session(user["user_id"])
    if not session:
        raise HTTPException(status_code=404, detail="No active session")
    stopped = await stop_user_task(session["task_arn"], reason="User requested stop")
    if not stopped:
        raise HTTPException(status_code=500, detail="Failed to stop ECS task")
    await mark_session_stopped(session["task_arn"], reason="user_requested")


@router.post("/sessions/beacon-stop", status_code=204)
async def beacon_stop_session(request: Request):
    """Stop session via navigator.sendBeacon (no Authorization header)."""
    try:
        body = await request.body()
        token = body.decode("utf-8").strip()
    except Exception:
        raise HTTPException(status_code=400, detail="Missing token")

    if not token:
        raise HTTPException(status_code=400, detail="Missing token")

    user = await validate_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid token")

    session = await get_active_session(user["user_id"])
    if not session:
        return

    await stop_user_task(session["task_arn"], reason="tab_closed")
    await mark_session_stopped(session["task_arn"], reason="tab_closed")


# =============================================================================
# S3 Presigned URL routes (Local Agent uses these)
#   POST /api/v1/s3/sign_put
#   POST /api/v1/s3/sign_get
# =============================================================================
@router.post("/s3/sign_put")
async def s3_sign_put(req: S3SignPutRequest, user: dict = Depends(get_current_user)):
    token_user_id = user.get("user_id")
    if not token_user_id:
        raise HTTPException(status_code=401, detail="Invalid user payload (missing user_id)")

    if req.user_id != token_user_id:
        raise HTTPException(status_code=403, detail="user_id mismatch")

    _require_allowed_bucket(req.bucket)
    _require_user_scoped_key(req.user_id, req.key)
    _require_allowed_content_type(req.content_type)

    try:
        url = s3_client.generate_presigned_url(
            ClientMethod="put_object",
            Params={
                "Bucket": req.bucket,
                "Key": req.key,
                "ContentType": req.content_type,
            },
            ExpiresIn=S3_PRESIGN_EXPIRES_SECONDS,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to presign PUT URL: {str(e)}")

    return {
        "url": url,
        "bucket": req.bucket,
        "key": req.key,
        "expires_in": S3_PRESIGN_EXPIRES_SECONDS,
    }


@router.post("/s3/sign_get")
async def s3_sign_get(req: S3SignGetRequest, user: dict = Depends(get_current_user)):
    token_user_id = user.get("user_id")
    if not token_user_id:
        raise HTTPException(status_code=401, detail="Invalid user payload (missing user_id)")

    if req.user_id != token_user_id:
        raise HTTPException(status_code=403, detail="user_id mismatch")

    _require_allowed_bucket(req.bucket)
    _require_user_scoped_key(req.user_id, req.key)

    try:
        url = s3_client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": req.bucket, "Key": req.key},
            ExpiresIn=S3_PRESIGN_EXPIRES_SECONDS,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to presign GET URL: {str(e)}")

    return {"url": url, "expires_in": S3_PRESIGN_EXPIRES_SECONDS}


# =============================================================================
# VM proxy routes  (browser → backend → container API, avoids CORS)
# =============================================================================
SUPPORTED_IDES = ["vscode", "sublime", "eclipse", "intellij", "pycharm"]


@router.post("/vm/upload_project")
async def vm_upload_project(
    file: UploadFile = File(...),
    project_name: str = Form(...),
    ide: str = Form("vscode"),
    user: dict = Depends(get_current_user),
):
    user_id = user["user_id"]
    project_name = _validate_project_name(project_name)

    if ide not in SUPPORTED_IDES:
        raise HTTPException(status_code=400, detail=f"Unsupported IDE '{ide}'. Supported: {SUPPORTED_IDES}")

    size = 0
    chunk = await file.read(MAX_UPLOAD_BYTES + 1)
    size = len(chunk)
    if size > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail=f"File exceeds {MAX_UPLOAD_BYTES // (1024*1024)} MB limit")
    await file.seek(0)

    s3_key = f"users/{user_id}/vscode/{project_name}.zip"
    config_key = f"users/{user_id}/vscode/_empty_config.zip"

    try:
        await asyncio.to_thread(
            s3_client.upload_fileobj,
            file.file,
            "cloudramsaas-vscode",
            s3_key,
            {"ContentType": "application/zip"},
        )
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w"):
            pass
        buf.seek(0)
        await asyncio.to_thread(
            s3_client.upload_fileobj,
            buf,
            "cloudramsaas-vscode",
            config_key,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3 upload failed: {e}")

    session = await get_active_session(user_id)
    if not session:
        raise HTTPException(status_code=404, detail="No active session")

    return await _vm_request(session, "/setup_ide", json_body={
        "user_id": user_id,
        "project_name": project_name,
        "ide": ide,
        "project_s3_bucket": "cloudramsaas-vscode",
        "project_s3_key": s3_key,
        "config_s3_bucket": "cloudramsaas-vscode",
        "config_s3_key": config_key,
    })


@router.post("/vm/setup_ide")
async def vm_setup_ide(request: Request, user: dict = Depends(get_current_user)):
    body = await request.json()
    session = await get_active_session(user["user_id"])
    if not session:
        raise HTTPException(status_code=404, detail="No active session")
    body["user_id"] = user["user_id"]
    return await _vm_request(session, "/setup_ide", json_body=body)


# Backward-compatible alias
@router.post("/vm/setup_vscode")
async def vm_setup_vscode(request: Request, user: dict = Depends(get_current_user)):
    body = await request.json()
    body["ide"] = "vscode"
    session = await get_active_session(user["user_id"])
    if not session:
        raise HTTPException(status_code=404, detail="No active session")
    body["user_id"] = user["user_id"]
    return await _vm_request(session, "/setup_ide", json_body=body)


@router.get("/vm/setup_status/{job_id}")
async def vm_setup_status(job_id: str, user: dict = Depends(get_current_user)):
    session = await get_active_session(user["user_id"])
    if not session:
        raise HTTPException(status_code=404, detail="No active session")
    return await _vm_request(session, f"/ide_setup_status/{job_id}", method="GET")


@router.get("/vm/available_ides")
async def vm_available_ides(user: dict = Depends(get_current_user)):
    session = await get_active_session(user["user_id"])
    if not session:
        raise HTTPException(status_code=404, detail="No active session")
    return await _vm_request(session, "/available_ides", method="GET")


@router.get("/vm/projects")
async def vm_list_projects(user: dict = Depends(get_current_user)):
    session = await get_active_session(user["user_id"])
    if not session:
        raise HTTPException(status_code=404, detail="No active session")
    return await _vm_request(session, f"/list_projects/{user['user_id']}", method="GET")


@router.post("/vm/export_project")
async def vm_export_project(request: Request, user: dict = Depends(get_current_user)):
    body = await request.json()
    session = await get_active_session(user["user_id"])
    if not session:
        raise HTTPException(status_code=404, detail="No active session")
    user_id = user["user_id"]
    project_name = _validate_project_name(body.get("project_name", ""))
    bucket = body.get("bucket", "cloudramsaas-vscode")
    s3_key = f"users/{user_id}/exports/{project_name}.zip"

    try:
        await asyncio.to_thread(
            s3_client.head_object, Bucket=bucket, Key=s3_key,
        )
        return {"bucket": bucket, "key": s3_key}
    except ClientError:
        pass

    body["user_id"] = user_id
    return await _vm_request(session, "/export_project", json_body=body)


# =============================================================================
# IDE config persistence
# =============================================================================
@router.post("/vm/save_ide_config")
async def vm_save_ide_config(request: Request, user: dict = Depends(get_current_user)):
    body = await request.json()
    session = await get_active_session(user["user_id"])
    if not session:
        raise HTTPException(status_code=404, detail="No active session")
    ide = body.get("ide", "vscode")
    if ide not in SUPPORTED_IDES:
        raise HTTPException(status_code=400, detail=f"Unsupported IDE '{ide}'")
    return await _vm_request(session, "/save_ide_config", json_body={
        "user_id": user["user_id"],
        "ide": ide,
    })


@router.post("/vm/save_all_ide_configs")
async def vm_save_all_ide_configs(user: dict = Depends(get_current_user)):
    session = await get_active_session(user["user_id"])
    if not session:
        raise HTTPException(status_code=404, detail="No active session")
    vm_ip = session.get("private_ip")
    api_port = session.get("api_port", 7000)
    if not vm_ip:
        raise HTTPException(status_code=503, detail="VM IP not available yet")
    url = f"http://{vm_ip}:{api_port}/save_all_ide_configs?user_id={user['user_id']}"
    headers = {}
    if VM_API_KEY:
        headers["X-VM-API-KEY"] = VM_API_KEY
    async with httpx.AsyncClient(timeout=VM_HTTP_TIMEOUT) as client:
        resp = await client.post(url, headers=headers)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()


# =============================================================================
# Existing: /files/url (kept)
# =============================================================================
@router.post("/files/url")
async def get_file_url(body: PresignedUrlRequest, user: dict = Depends(get_current_user)):
    if body.operation not in ("get_object", "put_object"):
        raise HTTPException(status_code=400, detail="Invalid operation")
    try:
        url = await generate_presigned_url(user["user_id"], body.filename, body.operation)
        return {"url": url, "expires_in": 3600}
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))