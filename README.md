# CloudRAMSaaS Backend

FastAPI backend service for the CloudRAMSaaS cloud desktop platform. Manages user sessions, ECS task orchestration, and secure S3 file transfer via presigned URLs.

## Architecture

```
Frontend / Local Agent
        │
        ▼ (Bearer JWT)
┌─────────────────────────────────────────────────┐
│  FastAPI Backend (this service)                   │
│                                                   │
│  ┌─────────┐  ┌──────────┐  ┌───────────────┐  │
│  │  Auth   │  │ Sessions │  │  S3 Presign   │  │
│  │(Supabase)│  │(Postgres)│  │  (PUT/GET)    │  │
│  └─────────┘  └──────────┘  └───────────────┘  │
│                      │                           │
│              ┌───────▼────────┐                  │
│              │  AWS ECS       │                  │
│              │  RunTask/Stop  │                  │
│              └────────────────┘                  │
└─────────────────────────────────────────────────┘
```

## Tech Stack

- **Framework:** FastAPI 0.111.0
- **Server:** Uvicorn (ASGI)
- **Database:** PostgreSQL via asyncpg (in-memory fallback for dev)
- **Auth:** Supabase (validates JWT via `/auth/v1/user`)
- **Cloud:** AWS ECS, EC2, S3, EFS (boto3)
- **Python:** 3.11

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check |
| `POST` | `/api/v1/sessions/allocate` | Allocate or return existing cloud desktop |
| `GET` | `/api/v1/sessions/status` | Get session state and noVNC URL |
| `POST` | `/api/v1/sessions/heartbeat` | Keep session alive (call every 30s) |
| `DELETE` | `/api/v1/sessions` | Stop and deallocate session |
| `POST` | `/api/v1/s3/sign_put` | Generate S3 presigned PUT URL |
| `POST` | `/api/v1/s3/sign_get` | Generate S3 presigned GET URL |
| `POST` | `/api/v1/files/url` | Generate presigned URL for workspace files |
| `GET` | `/api/v1/debug/aws_identity` | Debug: show AWS caller identity |

## Project Structure

```
├── app.py                 # FastAPI app, CORS, lifespan (auto-runs DB migration)
├── src/
│   ├── auth.py            # Supabase JWT validation (Bearer token → user dict)
│   ├── aws.py             # ECS RunTask/StopTask/Describe, S3 presign, port allocation
│   ├── db.py              # Session CRUD (asyncpg pool or in-memory dict)
│   └── routes.py          # All API route handlers
├── applications/
│   └── CloudRAMSaaS-Agent.exe   # Local agent binary (served for download)
├── requirements.txt       # Python dependencies
├── .python-version        # Python 3.11 pin (for Render)
└── .env                   # Environment variables (not committed)
```

## Environment Variables

```env
# Supabase
SUPABASE_URL=https://<project>.supabase.co
SUPABASE_SERVICE_ROLE_KEY=<service-role-key>

# AWS
AWS_REGION=us-west-2
ECS_CLUSTER=<cluster-name>
ECS_TASK_DEFINITION=<task-definition-arn>
ECS_CAPACITY_PROVIDER=<capacity-provider-name>
S3_BUCKET=<workspace-bucket>          # optional, for /files/url endpoint

# App
PORT=8000
ALLOWED_ORIGINS=http://localhost:5000,https://cloudramsaas-frontend-aws.onrender.com
DATABASE_URL=postgres://...            # optional; omit for in-memory sessions (dev)
SESSION_TIMEOUT_MINUTES=60

# S3 Presign (for Local Agent)
ALLOWED_S3_BUCKETS=notepadppfiles,cloudramsaas-vscode
S3_PRESIGN_EXPIRES_SECONDS=300
```

## Session Lifecycle

```
POST /sessions/allocate
  → ECS RunTask (random ports: noVNC 6100-6999, VNC 5910-5999, API 7000-7999)
  → DB: status=PROVISIONING
  → Background poll ECS until RUNNING (every 5s, max 40 attempts)

GET /sessions/status
  → Refresh from ECS describe_tasks
  → Return novnc_url when RUNNING

POST /sessions/heartbeat (every 30s from browser)
  → Updates last_heartbeat timestamp

DELETE /sessions
  → ECS StopTask
  → DB: status=STOPPED
```

## Database Schema

The `sessions` table is auto-created on startup:

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

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Copy environment file
cp ../.env.example .env
# Fill in Supabase and AWS credentials

# Run
python app.py
# or
uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```

Without `DATABASE_URL` set, sessions are stored in-memory (suitable for dev only).

## Deployment (Render)

This service is deployed on Render. The `.python-version` file pins Python 3.11.

**Build command:** `pip install -r requirements.txt`  
**Start command:** `uvicorn app:app --host 0.0.0.0 --port $PORT`

## Security

- All endpoints (except `/health`) require a valid Supabase Bearer token
- S3 presigned URLs are scoped to `users/<user_id>/` prefix (enforced server-side)
- Only allowlisted S3 buckets and content types are permitted
- CORS restricted to configured origins
- ECS tasks run in private subnets (no public IP)
