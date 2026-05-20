import asyncio
import json
import httpx
import redis.asyncio as aioredis

from app.config import settings

async def process_task_with_ai(client: httpx.AsyncClient, text_payload: str) -> dict:
    """
    Dispatches data streams to the centralized AI Inference layer. 
    Implements a resilient timeout strategy to ensure the worker layer never freezes.
    """
    headers = {"Authorization": f"Bearer {settings.AI_API_TOKEN}"} if settings.AI_API_TOKEN else {}
    
    # Simple structured payload for language processing models
    json_data = {"inputs": text_payload}
    
    # We use a 15-second explicit network window barrier
    response = await client.post(settings.AI_API_URL, headers=headers, json=json_data, timeout=15.0)
    
    if response.status_code == 200:
        return {"insights": response.json(), "engine_status": "SUCCESS"}
    else:
        # Fallback tracking if the external model is initializing or throttling
        return {"insights": None, "engine_status": f"AI_PROVIDER_ERROR_CODE_{response.status_code}"}

async def worker_orchestrator():
    """
    Continuous background loop polling tasks from the memory broker buffer,
    executing AI operations, and atomically saving states.
    """
    print("🚀 AI Worker Daemon initializing event loop...")
    redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    
    # Share a single connection pool across worker operations for maximum resource efficiency
    async with httpx.AsyncClient() as http_client:
        while True:
            try:
                # BLPOP blocks cleanly until an item appears in the queue (Zero CPU waste)
                result = await redis_client.blpop("ai_analytics_queue", timeout=5)
                if not result:
                    continue
                
                _, task_id = result
                print(f"📦 Worker pulled active payload signature: {task_id}")
                
                # Retrieve the raw task data from cache memory
                task_raw = await redis_client.get(f"task:{task_id}")
                if not task_raw:
                    continue
                    
                task_metadata = json.loads(task_raw)
                task_metadata["status"] = "PROCESSING"
                await redis_client.set(f"task:{task_id}", json.dumps(task_metadata))
                
                # Execute non-blocking AI system invocation
                ai_results = await process_task_with_ai(http_client, task_metadata["payload"])
                
                # Mutate task object to terminal completion state with analytical metrics
                task_metadata["status"] = "COMPLETED" if ai_results["engine_status"] == "SUCCESS" else "FAILED"
                task_metadata["analytics_output"] = ai_results["insights"]
                task_metadata["execution_logs"] = ai_results["engine_status"]
                
                # Save final computed values back into Redis
                await redis_client.set(f"task:{task_id}", json.dumps(task_metadata))
                print(f"✅ Finished execution matrix for job signature: {task_id}")
                
            except Exception as e:
                print(f"🚨 Worker Execution Engine Exception Loop encountered: {str(e)}")
                await asyncio.sleep(2) # Prevent rapid-fire logging panic loops

if __name__ == "__main__":
    asyncio.run(worker_orchestrator())