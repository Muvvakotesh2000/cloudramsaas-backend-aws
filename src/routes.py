# backend/src/routes.py
import asyncio
import logging
import os
from typing import Optional

import boto3
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from pydantic import BaseModel

from src.auth import get_current_user
from src.aws import generate_presigned_url, get_task_status, run_user_task, stop_user_task
from src.db import (
    cleanup_expired_sessions,
    create_session,
    get_active_session,
    heartbeat_session,
    mark_session_stopped,
    update_session_status,
)

logger = logging.getLogger(__name__)
router = APIRouter()

# =============================================================================
# S3 presign configuration (for Local Agent)
# =============================================================================
S3_PRESIGN_EXPIRES_SECONDS = int(os.getenv("S3_PRESIGN_EXPIRES_SECONDS", "300"))

# Example: ALLOWED_S3_BUCKETS="notepadfiles,cloudramsaas-vscode"
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


@router.get("/debug/aws_identity")
async def debug_aws_identity(user: dict = Depends(get_current_user)):
    sts = boto3.client("sts")
    ident = sts.get_caller_identity()
    return {
        "account": ident.get("Account"),
        "arn": ident.get("Arn"),
        "user_id": user.get("user_id"),
    }


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
async def _poll_task_until_running(task_arn: str, max_attempts: int = 40):
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
            return
        if ecs_status in ("STOPPED", "DEPROVISIONING"):
            await mark_session_stopped(task_arn, reason="ecs_stopped_unexpectedly")
            return

    await mark_session_stopped(task_arn, reason="provision_timeout")


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

    # Return existing active session if any
    existing = await get_active_session(user_id)
    if existing:
        info = await get_task_status(existing["task_arn"])
        ecs_status = info["status"] if info else "UNKNOWN"

        if ecs_status in ("PROVISIONING", "PENDING", "RUNNING"):
            reachable_ip = (info.get("public_ip") or info.get("private_ip")) if info else existing.get("private_ip")
            novnc_port = (info.get("novnc_port") if info else None) or existing.get("novnc_port", 6080)
            api_port = (info.get("api_port") if info else None) or existing.get("api_port")

            novnc_url = (
                f"http://{reachable_ip}:{novnc_port}/vnc.html"
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
    background_tasks.add_task(_poll_task_until_running, task_arn)

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
        f"http://{reachable_ip}:{novnc_port}/vnc.html"
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