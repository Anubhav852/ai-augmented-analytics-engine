import json
import uuid
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
import redis.asyncio as aioredis

from app.config import settings

# Initialize core FastAPI gateway instance
app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="High-Throughput Asynchronous AI-Augmented Analytics Gateway"
)

# Global holder for async Redis client pooling
redis_client: aioredis.Redis | None = None

class AnalyticsPayload(BaseModel):
    data_stream: str = Field(..., min_length=10, description="Raw text data stream targeted for asynchronous AI ingestion.")

class TaskTracker(BaseModel):
    task_id: str
    status: str
    message: str

@app.on_event("startup")
async def startup_event():
    """Establishes an optimized asynchronous connection pool to the Redis buffer layer."""
    global redis_client
    try:
        redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        # Ping check to mathematically verify routing connectivity on startup
        await redis_client.ping()
    except Exception as e:
        print(f"CRITICAL: Failed to bind to Redis memory buffer engine: {str(e)}")

@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check():
    """System health check endpoint indicating status of the ingestion gateway."""
    return {"status": "ONLINE", "gateway": settings.PROJECT_NAME}

@app.post("/api/v1/ingest", response_model=TaskTracker, status_code=status.HTTP_202_ACCEPTED)
async def ingest_stream(payload: AnalyticsPayload):
    """
    Ingests high-frequency unstructured stream text data. Decouples execution immediately 
    by atomic serialization into the Redis queue, delivering instantaneous 202 responses.
    """
    global redis_client
    if not redis_client:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, 
            detail="Ingestion network database engine disconnected."
        )
    
    try:
        # Generate an immutable tracking hash
        task_id = str(uuid.uuid4())
        
        job_metadata = {
            "task_id": task_id,
            "status": "QUEUED",
            "payload": payload.data_stream
        }
        
        # Atomically record state and append to our processing pipeline
        async with redis_client.pipeline(transaction=True) as pipe:
            await pipe.set(f"task:{task_id}", json.dumps(job_metadata))
            await pipe.rpush("ai_analytics_queue", task_id)
            await pipe.execute()
            
        return TaskTracker(
            task_id=task_id,
            status="QUEUED",
            message="Payload safely buffered. Async worker cluster notified."
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to isolate payload into execution queue: {str(e)}"
        )

@app.get("/api/v1/tasks/{task_id}", response_model=dict, status_code=status.HTTP_200_OK)
async def get_task_status(task_id: str):
    """Allows clients to pull current analytical status of their distributed computation jobs."""
    global redis_client
    if not redis_client:
        raise HTTPException(status_code=503, detail="State broker engine unavailable.")
        
    task_data = await redis_client.get(f"task:{task_id}")
    if not task_data:
        raise HTTPException(status_code=404, detail="Requested tracking signature not found.")
        
    return json.loads(task_data)