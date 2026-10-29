# High-Throughput Asynchronous AI Event Analytics Engine

**Live demo:** https://ai-augmented-analytics-engine1.onrender.com/
**API docs (Swagger):** https://ai-augmented-analytics-engine1.onrender.com/docs

> Hosted on Render's free tier. If the service has been idle, the first request can take 30 to 60 seconds while it wakes up.

A decoupled, message-driven pipeline that accepts text events over HTTP, returns immediately, and analyses them asynchronously in a background worker using a Redis queue. It is built around non-blocking concurrency and horizontal scalability.

## Architecture

```
[ Client / Live Console ] ──( POST /api/v1/ingest )──> [ FastAPI Gateway ]
                                                              │
                                                  ( instant 202 Accepted + task_id )
                                                              │
                                                              ▼
                                                    [ Redis Queue (list) ]
                                                              │
                                                    ( BLPOP, blocking pop )
                                                              │
                                                              ▼
                              [ Task state in Redis ] <── [ Async Worker ] ──> [ AI Inference API ]
                                        ▲                                       (local fallback)
                                        │
                      ( GET /api/v1/tasks/{task_id} )
```

## How it works

1. **Ingest.** The client sends `POST /api/v1/ingest` with `{"data_stream": "..."}` (at least 10 characters).
2. **Buffer.** The gateway generates a `task_id`, stores the task as `QUEUED` in Redis, pushes the ID onto the `ai_analytics_queue` list in one atomic transaction, and returns **202 Accepted** straight away. The client never waits for the AI step.
3. **Process.** An async worker blocks on `BLPOP`, picks up the task, marks it `PROCESSING`, and runs the analysis.
4. **AI step.** If a real `AI_API_TOKEN` is configured, the worker calls the AI inference API (15 second timeout). If no token is set, or the provider errors or times out, it falls back to a local keyword and sentiment analyser. Fallback results are labelled `FALLBACK_LOCAL_ANALYSIS` in `execution_logs`, so they are never presented as model output.
5. **Result.** The task becomes `COMPLETED` (or `FAILED`) with `analytics_output` attached. Clients poll `GET /api/v1/tasks/{task_id}`.

Task states: `QUEUED` → `PROCESSING` → `COMPLETED` / `FAILED`. Tasks expire from Redis after 24 hours.

## Live console

Opening the root URL shows a dashboard with three panels (Gateway, Queue, Worker) and a live event feed:

- Send your own text, or pick a preset.
- See the gateway's real 202 response time (average and p95).
- **Send 25 at once** fires 25 parallel requests so you can watch the queue absorb the burst while the worker catches up.
- Click any row to see the request, the gateway response, and the worker's analysis.

## API reference

| Method | Path | Description |
|---|---|---|
| GET | `/` | Live console |
| GET | `/health` | Gateway status and Redis connectivity |
| POST | `/api/v1/ingest` | Submit text; returns 202 with `task_id` |
| GET | `/api/v1/tasks/{task_id}` | Task status and analysis result |
| GET | `/api/v1/stats` | Queue depth, processed count, failed count |
| GET | `/docs` | Interactive Swagger UI |

## How to test it

### 1. In the browser (easiest)
1. Open the live link and wait for **Gateway online** at the top.
2. Choose a preset (or type text) and click **Send event**. The row should go Queued, Processing, then Processed within a second or two.
3. Click the row to see the analysis JSON.
4. Click **Send 25 at once**. All 25 should be accepted instantly, and the counters should rise as the worker finishes them.

### 2. With the command line

Health check:
```bash
curl https://ai-augmented-analytics-engine1.onrender.com/health
```
Expected: `{"status":"ONLINE","gateway":"AI_Augmented_Analytics_Engine","redis":true}`

Submit an event (macOS / Linux / Git Bash):
```bash
curl -X POST https://ai-augmented-analytics-engine1.onrender.com/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{"data_stream":"The checkout page is slow and payments failed with a timeout."}'
```

Submit an event (Windows PowerShell):
```powershell
Invoke-RestMethod -Method Post `
  -Uri "https://ai-augmented-analytics-engine1.onrender.com/api/v1/ingest" `
  -ContentType "application/json" `
  -Body '{"data_stream":"The checkout page is slow and payments failed with a timeout."}'
```
Expected: HTTP 202 with a `task_id` and `"status": "QUEUED"`.

Fetch the result (replace `<task_id>`):
```bash
curl https://ai-augmented-analytics-engine1.onrender.com/api/v1/tasks/<task_id>
```
Expected: `"status": "COMPLETED"` with `analytics_output`.

Check pipeline stats:
```bash
curl https://ai-augmented-analytics-engine1.onrender.com/api/v1/stats
```

### 3. Error cases to try
- Send `{"data_stream":"short"}` → **422** (validation: minimum 10 characters).
- Request `/api/v1/tasks/does-not-exist` → **404**.

## Run locally

Requires Python 3.11+ and Docker (for Redis).

```bash
docker run -d -p 6379:6379 redis
pip install -r requirements.txt
uvicorn app.main:app --reload
```
Open http://localhost:8000.

Or build the container:
```bash
docker build -t analytics-engine .
docker run -p 8000:8000 -e REDIS_URL=redis://host.docker.internal:6379/0 analytics-engine
```

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection string |
| `AI_API_URL` | Hugging Face BART summarisation endpoint | AI inference endpoint |
| `AI_API_TOKEN` | `mock_token_for_validation` | Provider token; if unset or mock, the local analyser is used |
| `RUN_WORKER_IN_API` | `true` | Run the worker inside the API process; set `false` to run `python -m app.worker` as a separate process |
| `WORKER_DELAY_SECONDS` | `0` | Optional per-task delay to make queue depth visible in demos |

## Deployment (Render)

- **Web Service** (Docker runtime) running the API and the embedded worker.
- **Key Value** (Redis-compatible) instance in the same region, connected through its internal URL as `REDIS_URL`.
- `PORT=8000` set to match the Dockerfile.

## Design decisions and trade-offs

- **202 + queue instead of a synchronous call:** keeps API latency flat regardless of how slow the AI step is.
- **BLPOP:** the worker sleeps until work arrives, with no busy polling.
- **Atomic pipeline write:** the task record and the queue push succeed or fail together.
- **Fallback analyser:** the pipeline stays useful when the external AI provider is down or throttled.
- **Scaling path:** run `python -m app.worker` as separate worker instances (`RUN_WORKER_IN_API=false` on the API), all consuming the same queue.

## Known limitations

- On the free tier the worker shares a process with the API. In production it would be its own service.
- The free Redis instance gives no durability guarantees.
- Results are fetched by polling. WebSockets or server-sent events would be a natural next step.    