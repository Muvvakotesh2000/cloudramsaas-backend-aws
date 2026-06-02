# CloudRAMSaaS Backend

FastAPI backend for CloudRAMSaaS — a cloud desktop platform that provisions on-demand GUI environments via AWS ECS. Handles authentication, session orchestration, file transfer, and VM proxy operations.

## Architecture

```
Browser / Frontend
       |
       v  (Bearer JWT)
+----------------------------------------------+
|  FastAPI Backend                              |
|                                               |
|  Auth (Supabase)  -->  Sessions (Postgres)    |
|                            |                  |
|                      ECS Orchestration        |
|                    (RunTask / StopTask)        |
|                            |                  |
|  S3 Presign (PUT/GET)    VM Proxy             |
|  (file upload/download)  (IDE setup, export)  |
+----------------------------------------------+
         |                     |
    AWS S3 Buckets        ECS Tasks (EC2)
                          noVNC + IDE + API
```

## Tech Stack

- **Framework:** FastAPI 0.111
- **Server:** Uvicorn (ASGI)
- **Database:** PostgreSQL via asyncpg (in-memory fallback for dev)
- **Auth:** Supabase JWT validation
- **Cloud:** AWS ECS, EC2, S3 (boto3)
- **Python:** 3.11

## API Endpoints

### Health
| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/health` | No | Health check |

### Sessions
| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/sessions/allocate` | Allocate or return existing cloud desktop |
| `GET` | `/api/v1/sessions/status` | Get session state, IP, and noVNC URL |
| `POST` | `/api/v1/sessions/heartbeat` | Keep session alive (call every 30s) |
| `DELETE` | `/api/v1/sessions` | Stop and deallocate session |

### S3 Presigned URLs
| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/s3/sign_put` | Presigned PUT URL (user-scoped) |
| `POST` | `/api/v1/s3/sign_get` | Presigned GET URL (user-scoped) |
| `POST` | `/api/v1/files/url` | Presigned URL for workspace files |

### VM Proxy (browser -> backend -> container)
| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/vm/upload_project` | Upload project zip to S3 and trigger IDE setup |
| `POST` | `/api/v1/vm/setup_ide` | Forward IDE setup request to container |
| `POST` | `/api/v1/vm/setup_vscode` | Alias for setup_ide with ide=vscode |
| `GET` | `/api/v1/vm/setup_status/{job_id}` | Poll IDE setup progress |
| `GET` | `/api/v1/vm/available_ides` | List available IDEs on container |
| `GET` | `/api/v1/vm/projects` | List projects on cloud desktop |
| `POST` | `/api/v1/vm/export_project` | Export project from container to S3 |
| `POST` | `/api/v1/vm/save_ide_config` | Save single IDE settings to S3 |
| `POST` | `/api/v1/vm/save_all_ide_configs` | Save all IDE settings to S3 |

### Debug
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/debug/aws_identity` | Show AWS caller identity |

## Project Structure

```
├── app.py                 # FastAPI app, CORS, lifespan, idle session reaper
├── src/
│   ├── auth.py            # Supabase JWT validation (Bearer token -> user dict)
│   ├── aws.py             # ECS RunTask/StopTask, S3 presign, random port allocation
│   ├── db.py              # Session CRUD (asyncpg or in-memory), ECS recovery on startup
│   └── routes.py          # All API route handlers, VM proxy, S3 signing
├── requirements.txt
├── .python-version        # Python 3.11 (for Render)
└── .env                   # Environment variables (not committed)
```

## Environment Variables

```env
# Supabase
SUPABASE_URL=https://<project>.supabase.co
SUPABASE_SERVICE_ROLE_KEY=<service-role-key>

# AWS
AWS_REGION=us-west-2
AWS_ACCESS_KEY_ID=<access-key>
AWS_SECRET_ACCESS_KEY=<secret-key>
ECS_CLUSTER=<cluster-name>
ECS_TASK_DEFINITION=<task-definition:revision>
ECS_CAPACITY_PROVIDER=<capacity-provider-name>
ECS_SUBNETS=<subnet-id-1>,<subnet-id-2>
ECS_SECURITY_GROUPS=<security-group-id>

# App
PORT=8000
ALLOWED_ORIGINS=https://cloudramsaas-frontend-aws.onrender.com
DATABASE_URL=postgres://...           # omit for in-memory sessions (dev only)
SESSION_TIMEOUT_MINUTES=60
CLOUDRAM_BACKEND_URL=http://localhost:8000
VM_API_KEY=<secret>
VNC_PW=<vnc-password>

# S3
ALLOWED_S3_BUCKETS=notepadppfiles,cloudramsaas-vscode
S3_PRESIGN_EXPIRES_SECONDS=300
S3_BUCKET=<workspace-bucket>          # for /files/url endpoint
```

## Session Lifecycle

```
POST /sessions/allocate
  -> Check for existing active session (return if found)
  -> Pick random ports: noVNC (6100-6999), VNC (5910-5999), API (7000-7999)
  -> ECS RunTask with port overrides
  -> DB: status=PROVISIONING
  -> Background poll ECS every 5s until RUNNING (max 40 attempts)

GET /sessions/status
  -> Refresh from ECS describe_tasks
  -> Return noVNC URL when RUNNING

POST /sessions/heartbeat (browser calls every 30s)
  -> Updates last_heartbeat timestamp

DELETE /sessions
  -> ECS StopTask -> DB: status=STOPPED

Idle Reaper (background, every 120s)
  -> Stop sessions with no heartbeat for SESSION_TIMEOUT_MINUTES
```

## Database

Auto-created `sessions` table on startup:

```sql
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
```

On startup, the backend also recovers sessions by scanning ECS for running tasks (`recover_sessions_from_ecs`).

## Local Development

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in credentials
python app.py
# or: uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```

Without `DATABASE_URL`, sessions are stored in-memory (dev only).

## Deployment (Render)

**Build command:** `pip install -r requirements.txt`
**Start command:** `uvicorn app:app --host 0.0.0.0 --port $PORT`

## Security

- All endpoints (except `/health`) require a valid Supabase Bearer token
- S3 presigned URLs are scoped to `users/<user_id>/` prefix
- Only allowlisted S3 buckets and content types are permitted
- CORS restricted to configured origins
- VM proxy requests forward an API key header to containers
- Supported IDEs are allowlisted server-side
