import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "")
SESSION_TIMEOUT_MINUTES = int(os.getenv("SESSION_TIMEOUT_MINUTES", 60))

_pool = None
_in_memory_sessions: dict = {}


async def init_db():
    global _pool
    if not DATABASE_URL:
        logger.warning("DATABASE_URL not set — using in-memory session store (not production-safe)")
        return

    import asyncpg
    _pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=2, max_size=10, command_timeout=10)
    await _migrate()
    logger.info("Database pool initialized")


async def _migrate():
    async with _pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id         TEXT NOT NULL,
                task_arn        TEXT NOT NULL UNIQUE,
                status          TEXT NOT NULL DEFAULT 'PROVISIONING',
                private_ip      TEXT,
                novnc_port      INTEGER DEFAULT 6080,
                api_port        INTEGER DEFAULT 5000,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                stopped_at      TIMESTAMPTZ,
                stop_reason     TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
            CREATE INDEX IF NOT EXISTS idx_sessions_status  ON sessions(status);
        """)


def _now():
    return datetime.now(timezone.utc)


def _mem_create(user_id: str, task_arn: str, novnc_port: int = 6080, api_port: Optional[int] = 5000) -> dict:
    session = {
        "id": str(uuid.uuid4()),
        "user_id": user_id,
        "task_arn": task_arn,
        "status": "PROVISIONING",
        "private_ip": None,
        "novnc_port": novnc_port,
        "api_port": api_port,
        "created_at": _now(),
        "last_heartbeat": _now(),
        "stopped_at": None,
        "stop_reason": None,
    }
    _in_memory_sessions[task_arn] = session
    return session


def _mem_active(user_id: str) -> Optional[dict]:
    stopped = {"STOPPED", "DEPROVISIONING", "DELETED"}
    matches = [s for s in _in_memory_sessions.values()
               if s["user_id"] == user_id and s["status"] not in stopped]
    return sorted(matches, key=lambda s: s["created_at"], reverse=True)[0] if matches else None


async def recover_sessions_from_ecs():
    """On startup, scan ECS for running CloudRAMSaaS tasks and rebuild in-memory state."""
    try:
        from src.aws import get_used_resources
        import boto3

        ecs_cluster = os.environ.get("ECS_CLUSTER")
        if not ecs_cluster:
            return

        ecs = boto3.client("ecs", region_name=os.environ.get("AWS_REGION"))
        ec2 = boto3.client("ec2", region_name=os.environ.get("AWS_REGION"))

        resp = ecs.list_tasks(cluster=ecs_cluster, desiredStatus="RUNNING")
        task_arns = resp.get("taskArns", [])
        if not task_arns:
            logger.info("ECS recovery: no running tasks found")
            return

        tasks_resp = ecs.describe_tasks(cluster=ecs_cluster, tasks=task_arns)
        recovered = 0

        for task in tasks_resp.get("tasks", []):
            task_arn = task["taskArn"]
            user_id = None
            novnc_port = 6080
            api_port = 7000

            for override in task.get("overrides", {}).get("containerOverrides", []):
                for env in override.get("environment", []):
                    if env["name"] == "USER_ID":
                        user_id = env["value"]
                    elif env["name"] == "NOVNC_PORT":
                        novnc_port = int(env["value"])
                    elif env["name"] == "API_PORT":
                        api_port = int(env["value"])

            if not user_id:
                continue

            existing = await get_session_by_arn(task_arn)
            if existing:
                continue

            private_ip = None
            container_instance_arn = task.get("containerInstanceArn")
            if container_instance_arn:
                try:
                    ci_resp = ecs.describe_container_instances(
                        cluster=ecs_cluster, containerInstances=[container_instance_arn]
                    )
                    ec2_id = ci_resp["containerInstances"][0].get("ec2InstanceId")
                    if ec2_id:
                        ec2_resp = ec2.describe_instances(InstanceIds=[ec2_id])
                        inst = ec2_resp["Reservations"][0]["Instances"][0]
                        private_ip = inst.get("PublicIpAddress") or inst.get("PrivateIpAddress")
                except Exception:
                    pass

            session = await create_session(user_id, task_arn, novnc_port, api_port)
            await update_session_status(
                task_arn=task_arn,
                status=task["lastStatus"],
                private_ip=private_ip,
                novnc_port=novnc_port,
                api_port=api_port,
            )
            recovered += 1
            logger.info("ECS recovery: restored session for user %s (task %s)", user_id, task_arn[:30])

        logger.info("ECS recovery: restored %d session(s)", recovered)

    except Exception as e:
        logger.warning("ECS recovery failed (non-fatal): %s", e)


async def create_session(user_id: str, task_arn: str, novnc_port: int = 6080, api_port: Optional[int] = 5000) -> dict:
    if _pool is None:
        return _mem_create(user_id, task_arn, novnc_port, api_port)
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO sessions (user_id, task_arn, status, novnc_port, api_port) VALUES ($1,$2,'PROVISIONING',$3,$4) RETURNING *",
            user_id, task_arn, novnc_port, api_port
        )
    return dict(row)


async def get_active_session(user_id: str) -> Optional[dict]:
    if _pool is None:
        return _mem_active(user_id)
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT * FROM sessions WHERE user_id=$1
               AND status NOT IN ('STOPPED','DEPROVISIONING','DELETED')
               ORDER BY created_at DESC LIMIT 1""",
            user_id,
        )
    return dict(row) if row else None


async def get_session_by_arn(task_arn: str) -> Optional[dict]:
    if _pool is None:
        return _in_memory_sessions.get(task_arn)
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM sessions WHERE task_arn=$1", task_arn)
    return dict(row) if row else None


async def update_session_status(task_arn: str, status: str,
                                 private_ip: Optional[str] = None,
                                 novnc_port: Optional[int] = None,
                                 api_port: Optional[int] = None) -> None:
    if _pool is None:
        s = _in_memory_sessions.get(task_arn)
        if s:
            s["status"] = status
            s["last_heartbeat"] = _now()
            if private_ip: s["private_ip"] = private_ip
            if novnc_port: s["novnc_port"] = novnc_port
            if api_port: s["api_port"] = api_port
        return
    async with _pool.acquire() as conn:
        await conn.execute(
            """UPDATE sessions SET status=$1,
               private_ip=COALESCE($2,private_ip),
               novnc_port=COALESCE($3,novnc_port),
               api_port=COALESCE($4,api_port),
               last_heartbeat=NOW() WHERE task_arn=$5""",
            status, private_ip, novnc_port, api_port, task_arn,
        )


async def heartbeat_session(task_arn: str) -> None:
    if _pool is None:
        s = _in_memory_sessions.get(task_arn)
        if s: s["last_heartbeat"] = _now()
        return
    async with _pool.acquire() as conn:
        await conn.execute(
            "UPDATE sessions SET last_heartbeat=NOW() WHERE task_arn=$1", task_arn)


async def mark_session_stopped(task_arn: str, reason: str = "user") -> None:
    if _pool is None:
        s = _in_memory_sessions.get(task_arn)
        if s:
            s["status"] = "STOPPED"
            s["stopped_at"] = _now()
            s["stop_reason"] = reason
        return
    async with _pool.acquire() as conn:
        await conn.execute(
            "UPDATE sessions SET status='STOPPED',stopped_at=NOW(),stop_reason=$1 WHERE task_arn=$2",
            reason, task_arn,
        )


async def cleanup_expired_sessions() -> int:
    """Stop sessions that haven't sent a heartbeat within the timeout window."""
    from src.aws import stop_user_task

    cutoff = _now() - timedelta(minutes=SESSION_TIMEOUT_MINUTES)
    stopped_count = 0

    if _pool is None:
        for task_arn, s in list(_in_memory_sessions.items()):
            if s["status"] in ("STOPPED", "DEPROVISIONING", "DELETED"):
                continue
            if s["last_heartbeat"] < cutoff:
                logger.info("Idle reaper: stopping task %s (user %s, idle since %s)",
                            task_arn[:30], s["user_id"], s["last_heartbeat"])
                try:
                    await stop_user_task(task_arn, reason="idle_timeout")
                except Exception as e:
                    logger.warning("Idle reaper: failed to stop ECS task %s: %s", task_arn[:30], e)
                s["status"] = "STOPPED"
                s["stopped_at"] = _now()
                s["stop_reason"] = "idle_timeout"
                stopped_count += 1
        return stopped_count

    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT task_arn, user_id FROM sessions
               WHERE status NOT IN ('STOPPED','DEPROVISIONING','DELETED')
               AND last_heartbeat < NOW() - INTERVAL '1 minute' * $1""",
            SESSION_TIMEOUT_MINUTES,
        )
        for row in rows:
            logger.info("Idle reaper: stopping task %s (user %s)", row["task_arn"][:30], row["user_id"])
            try:
                await stop_user_task(row["task_arn"], reason="idle_timeout")
            except Exception as e:
                logger.warning("Idle reaper: failed to stop ECS task: %s", e)
            stopped_count += 1

        if rows:
            await conn.execute(
                """UPDATE sessions SET status='STOPPED',stopped_at=NOW(),stop_reason='idle_timeout'
                   WHERE status NOT IN ('STOPPED','DEPROVISIONING','DELETED')
                   AND last_heartbeat < NOW() - INTERVAL '1 minute' * $1""",
                SESSION_TIMEOUT_MINUTES,
            )

    return stopped_count
