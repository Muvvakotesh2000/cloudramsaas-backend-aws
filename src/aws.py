import logging
import os
import random
from typing import Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

AWS_REGION            = os.environ["AWS_REGION"]
ECS_CLUSTER           = os.environ["ECS_CLUSTER"]
ECS_TASK_DEFINITION   = os.environ["ECS_TASK_DEFINITION"]
ECS_CAPACITY_PROVIDER = os.environ["ECS_CAPACITY_PROVIDER"]
S3_BUCKET             = os.getenv("S3_BUCKET", "")

NOVNC_PORT_RANGE  = (6100, 6999)
VNC_PORT_RANGE    = (5910, 5999)
API_PORT_RANGE    = (7000, 7999)
DISPLAY_RANGE     = (10, 99)       # :10 to :99

ecs = boto3.client("ecs", region_name=AWS_REGION)
ec2 = boto3.client("ec2", region_name=AWS_REGION)
s3  = boto3.client("s3",  region_name=AWS_REGION)


def _pick_random(start: int, end: int, exclude: list) -> int:
    available = [p for p in range(start, end + 1) if p not in exclude]
    if not available:
        raise RuntimeError(f"No available values in range {start}-{end}")
    return random.choice(available)


async def get_used_resources() -> tuple[list, list, list, list]:
    """Return lists of used novnc ports, vnc ports, api ports, display numbers."""
    used_novnc   = []
    used_vnc     = []
    used_api     = []
    used_display = []
    try:
        resp = ecs.list_tasks(cluster=ECS_CLUSTER, desiredStatus="RUNNING")
        task_arns = resp.get("taskArns", [])
        if not task_arns:
            return [], [], [], []

        tasks_resp = ecs.describe_tasks(cluster=ECS_CLUSTER, tasks=task_arns)
        for task in tasks_resp.get("tasks", []):
            for override in task.get("overrides", {}).get("containerOverrides", []):
                for env in override.get("environment", []):
                    name = env.get("name")
                    val  = env.get("value")

                    if name == "NOVNC_PORT":
                        used_novnc.append(int(val))
                    elif name == "VNC_PORT":
                        used_vnc.append(int(val))
                    elif name == "API_PORT":
                        used_api.append(int(val))
                    elif name == "DISPLAY":
                        try:
                            used_display.append(int(val.replace(":", "")))
                        except Exception:
                            pass
    except Exception:
        pass

    return used_novnc, used_vnc, used_api, used_display


async def run_user_task(user_id: str) -> dict:
    try:
        used_novnc, used_vnc, used_api, used_display = await get_used_resources()

        novnc_port  = _pick_random(*NOVNC_PORT_RANGE, exclude=used_novnc)
        vnc_port    = _pick_random(*VNC_PORT_RANGE,   exclude=used_vnc)
        api_port    = _pick_random(*API_PORT_RANGE,   exclude=used_api)
        display_num = _pick_random(*DISPLAY_RANGE,    exclude=used_display)
        display     = f":{display_num}"

        logger.info(
            "Allocating user %s — novnc=%d vnc=%d api=%d display=%s",
            user_id, novnc_port, vnc_port, api_port, display
        )

        resp = ecs.run_task(
            cluster=ECS_CLUSTER,
            taskDefinition=ECS_TASK_DEFINITION,
            capacityProviderStrategy=[
                {"capacityProvider": ECS_CAPACITY_PROVIDER, "weight": 1, "base": 0}
            ],
            overrides={
                "containerOverrides": [{
                    "name": "gui",
                    "environment": [
                        {"name": "USER_ID",        "value": user_id},
                        {"name": "WORKSPACE_PATH", "value": f"/workspaces/{user_id}"},
                        {"name": "NOVNC_PORT",     "value": str(novnc_port)},
                        {"name": "VNC_PORT",       "value": str(vnc_port)},
                        {"name": "API_PORT",       "value": str(api_port)},
                        {"name": "DISPLAY",        "value": display},
                    ],
                }]
            },
            tags=[
                {"key": "Service",   "value": "CloudRAMSaaS"},
                {"key": "UserId",    "value": user_id},
                {"key": "ManagedBy", "value": "backend"},
            ],
            startedBy=f"cloudramsaas-{user_id[:8]}",
        )

        failures = resp.get("failures", [])
        if failures:
            reason = failures[0].get("reason", "unknown")
            logger.error("ECS RunTask failure for user %s: %s", user_id, reason)
            raise RuntimeError(f"ECS task launch failed: {reason}")

        task = resp["tasks"][0]
        return {
            "task_arn":   task["taskArn"],
            "status":     task["lastStatus"],
            "novnc_port": novnc_port,
            "vnc_port":   vnc_port,
            "api_port":   api_port,
            "display":    display,
        }

    except ClientError as e:
        logger.exception("AWS ClientError launching task for user %s", user_id)
        raise RuntimeError(f"AWS error: {e.response['Error']['Message']}") from e


async def get_task_status(task_arn: str) -> Optional[dict]:
    try:
        resp  = ecs.describe_tasks(cluster=ECS_CLUSTER, tasks=[task_arn])
        tasks = resp.get("tasks", [])
        if not tasks:
            return None

        task       = tasks[0]
        status     = task["lastStatus"]
        public_ip:  Optional[str] = None
        private_ip: Optional[str] = None

        novnc_port: int = 6080
        api_port:   int = 5000

        # Get ports from task override env
        for override in task.get("overrides", {}).get("containerOverrides", []):
            for env in override.get("environment", []):
                if env["name"] == "NOVNC_PORT":
                    novnc_port = int(env["value"])
                elif env["name"] == "API_PORT":
                    api_port = int(env["value"])

        # Get EC2 host public IP
        container_instance_arn = task.get("containerInstanceArn")
        if container_instance_arn:
            try:
                ci_resp = ecs.describe_container_instances(
                    cluster=ECS_CLUSTER,
                    containerInstances=[container_instance_arn]
                )
                ec2_id = ci_resp["containerInstances"][0].get("ec2InstanceId")
                if ec2_id:
                    ec2_resp   = ec2.describe_instances(InstanceIds=[ec2_id])
                    inst       = ec2_resp["Reservations"][0]["Instances"][0]
                    public_ip  = inst.get("PublicIpAddress")
                    private_ip = inst.get("PrivateIpAddress")
            except Exception:
                pass

        return {
            "task_arn":   task_arn,
            "status":     status,
            "public_ip":  public_ip,
            "private_ip": private_ip,
            "novnc_port": novnc_port,
            "api_port":   api_port,
            "health":     task.get("healthStatus", "UNKNOWN"),
        }

    except ClientError:
        logger.exception("Failed to describe task %s", task_arn)
        return None


async def stop_user_task(task_arn: str, reason: str = "User requested stop") -> bool:
    try:
        ecs.stop_task(cluster=ECS_CLUSTER, task=task_arn, reason=reason)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "InvalidParameterException":
            return True
        logger.exception("Failed to stop task %s", task_arn)
        return False


async def generate_presigned_url(
    user_id: str, filename: str, operation: str = "get_object", expires: int = 3600
) -> str:
    if not S3_BUCKET:
        raise RuntimeError("S3_BUCKET not configured")
    key = f"workspaces/{user_id}/{filename}"
    try:
        return s3.generate_presigned_url(
            ClientMethod=operation,
            Params={"Bucket": S3_BUCKET, "Key": key},
            ExpiresIn=expires,
        )
    except ClientError as e:
        raise RuntimeError("Could not generate file URL") from e