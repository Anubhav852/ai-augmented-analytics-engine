import asyncio
import json
import os
import time
import uuid
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from app.config import settings
from app.worker import QUEUE, worker_orchestrator

redis_client: aioredis.Redis | None = None
STATIC_INDEX = os.path.join(os.path.dirname(__file__), "static", "index.html")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    worker_task = None
    try:
        redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        await redis_client.ping()
    except Exception as e:
        print(f"CRITICAL: Failed to connect to Redis: {e}")
    # Render's free tier has no background workers, so the worker runs inside the web process.
    # Set RUN_WORKER_IN_API=false to run it as a separate process instead.
    if os.getenv("RUN_WORKER_IN_API", "true").lower() == "true":
        worker_task = asyncio.create_task(worker_orchestrator())
    yield
    if worker_task:
        worker_task.cancel()
    if redis_client:
        await redis_client.aclose()


app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="High-Throughput Asynchronous AI-Augmented Analytics Gateway",
    lifespan=lifespan,
)


class AnalyticsPayload(BaseModel):
    data_stream: str = Field(..., min_length=10, description="Raw text data stream targeted for asynchronous AI ingestion.")
    use_ai: bool = True


class TaskTracker(BaseModel):
    task_id: str
    status: str
    message: str


@app.get("/", include_in_schema=False)
async def home():
    return FileResponse(STATIC_INDEX)


@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check():
    redis_ok = False
    if redis_client:
        try:
            redis_ok = bool(await redis_client.ping())
        except Exception:
            pass
    return {"status": "ONLINE", "gateway": settings.PROJECT_NAME, "redis": redis_ok}


@app.get("/api/v1/stats")
async def stats():
    if not redis_client:
        raise HTTPException(status_code=503, detail="State broker engine unavailable.")
    depth, processed, failed = await asyncio.gather(
        redis_client.llen(QUEUE), redis_client.get("stats:processed"), redis_client.get("stats:failed")
    )
    return {"queue_depth": depth, "processed": int(processed or 0), "failed": int(failed or 0)}


@app.post("/api/v1/ingest", response_model=TaskTracker, status_code=status.HTTP_202_ACCEPTED)
async def ingest_stream(payload: AnalyticsPayload):
    if not redis_client:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Ingestion network database engine disconnected.")
    try:
        task_id = str(uuid.uuid4())
        job = {
            "task_id": task_id,
            "status": "QUEUED",
            "payload": payload.data_stream,
            "use_ai": payload.use_ai,
            "created_at": time.time(),
        }
        async with redis_client.pipeline(transaction=True) as pipe:
            await pipe.set(f"task:{task_id}", json.dumps(job), ex=86400)  # auto-expire after 24h
            await pipe.rpush(QUEUE, task_id)
            await pipe.execute()
        return TaskTracker(task_id=task_id, status="QUEUED", message="Payload safely buffered. Async worker cluster notified.")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to isolate payload into execution queue: {e}")


@app.get("/api/v1/tasks/{task_id}", response_model=dict, status_code=status.HTTP_200_OK)
async def get_task_status(task_id: str):
    if not redis_client:
        raise HTTPException(status_code=503, detail="State broker engine unavailable.")
    task_data = await redis_client.get(f"task:{task_id}")
    if not task_data:
        raise HTTPException(status_code=404, detail="Requested tracking signature not found.")
    return json.loads(task_data)