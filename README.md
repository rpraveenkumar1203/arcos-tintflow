# TintFlow

TintFlow is Tint's independent durable workflow service. It has its own Rust
crate and PostgreSQL database. The main application communicates with it over
HTTP; gRPC remains a possible future contract.

## What Works

- Workflow CRUD with JSON step definitions.
- `http`, `log`, `delay`, and `approval` step kinds.
- Background runs with durable execution and step logs.
- Resumption from a persisted cursor.
- Five-field cron schedules evaluated by a periodic scheduler.
- Webhook tokens that start workflows with a trigger payload.
- Approval steps that pause and resume executions.
- Tenant and global workflow templates.
- Optional NATS events for run success, run failure, and approval requests.

## Run Locally

```powershell
$env:TINTFLOW_DATABASE_URL = "postgres://tintflow:tintflow@localhost:5433/tintflow"
cargo run --manifest-path services/tintflow/Cargo.toml
```

Or use the complete stack:

```powershell
docker compose -f docker-compose.microservices.yml up -d --build tintflow
```

## API

Tenant-specific direct service calls use the `X-Tenant-Id` header.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Liveness |
| `POST`, `GET` | `/workflows` | Create or list workflows |
| `GET`, `PUT`, `DELETE` | `/workflows/{id}` | Read, update, or delete a workflow |
| `POST` | `/workflows/{id}/run` | Start a background run |
| `POST` | `/workflows/{id}/webhooks` | Create a webhook token |
| `POST` | `/workflows/{id}/schedules` | Add a cron schedule |
| `DELETE` | `/schedules/{id}` | Delete a schedule |
| `GET` | `/runs?workflow_id=&limit=` | List runs |
| `GET` | `/runs/{id}` | Read a run and its step log |
| `POST` | `/hooks/{token}` | Trigger a workflow by webhook |
| `GET`, `POST` | `/approvals/{id}` | Inspect or resolve an approval |
| `GET`, `POST` | `/templates` | List or create templates |

## Tests

```powershell
cargo test --manifest-path services/tintflow/Cargo.toml
```

The default suite covers cron matching and step executors. Four ignored tests
exercise the PostgreSQL-backed workflow lifecycle:

```powershell
$env:TINTFLOW_DATABASE_URL = "postgres://tintflow:tintflow@localhost:5433/tintflow"
cargo test --manifest-path services/tintflow/Cargo.toml -- --ignored
```

Use an isolated test database because these tests create and remove data.

## Roadmap

- A versioned gRPC or equivalent service contract.
- Dedicated worker-pool and queue controls.
- Branch, loop, and native connector step kinds.
- Dead-letter handling and event-consumer observability.
