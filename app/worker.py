import asyncio
import json
import os
import re
from collections import Counter

import httpx
import redis.asyncio as aioredis

from app.config import settings

QUEUE = "ai_analytics_queue"
# Optional demo knob: artificial delay per task so queue depth is visible during a burst.
WORKER_DELAY_SECONDS = float(os.getenv("WORKER_DELAY_SECONDS", "0"))

STOPWORDS = set("the a an and or but if of to in on at for with is are was were be been it this that as by from "
                "we you they i our your their its not no so than then there have has had will would can could".split())
POSITIVE = {"great", "good", "excellent", "love", "fast", "smooth", "happy", "success", "improved", "stable", "win"}
NEGATIVE = {"bad", "slow", "error", "fail", "failed", "timeout", "crash", "broken", "down", "outage", "angry", "churn"}


def local_analysis(text: str) -> dict:
    """Deterministic fallback so the pipeline always returns insights, even if the AI provider is unavailable."""
    words = re.findall(r"[a-zA-Z']+", text.lower())
    keywords = [w for w, _ in Counter(w for w in words if w not in STOPWORDS and len(w) > 3).most_common(5)]
    pos = sum(w in POSITIVE for w in words)
    neg = sum(w in NEGATIVE for w in words)
    sentiment = "positive" if pos > neg else "negative" if neg > pos else "neutral"
    first = re.split(r"(?<=[.!?])\s+", text.strip())[0]
    return {
        "summary": first[:160],
        "word_count": len(words),
        "top_keywords": keywords,
        "sentiment": sentiment,
        "source": "local heuristic analyzer (fallback)",
    }


async def process_task_with_ai(client: httpx.AsyncClient, text_payload: str) -> dict:
    """Calls the external AI provider; on missing token, error or timeout, falls back to local analysis."""
    token = settings.AI_API_TOKEN
    if token and token != "mock_token_for_validation":
        try:
            response = await client.post(
                settings.AI_API_URL,
                headers={"Authorization": f"Bearer {token}"},
                json={"inputs": text_payload},
                timeout=15.0,
            )
            if response.status_code == 200:
                return {"insights": response.json(), "engine_status": "SUCCESS"}
            reason = f"AI_PROVIDER_ERROR_CODE_{response.status_code}"
        except Exception as e:  # timeouts, DNS, TLS, etc.
            reason = f"AI_PROVIDER_EXCEPTION_{type(e).__name__}"
    else:
        reason = "NO_AI_TOKEN_CONFIGURED"
    return {"insights": local_analysis(text_payload), "engine_status": f"FALLBACK_LOCAL_ANALYSIS ({reason})"}


async def worker_orchestrator():
    """Background loop: BLPOP from Redis, run analysis, store the result."""
    print("AI worker daemon starting...")
    redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)

    async with httpx.AsyncClient() as http_client:
        while True:
            try:
                result = await redis_client.blpop(QUEUE, timeout=5)
                if not result:
                    continue

                _, task_id = result
                print(f"Worker pulled task: {task_id}")

                task_raw = await redis_client.get(f"task:{task_id}")
                if not task_raw:
                    continue

                task = json.loads(task_raw)
                task["status"] = "PROCESSING"
                await redis_client.set(f"task:{task_id}", json.dumps(task))

                if WORKER_DELAY_SECONDS:
                    await asyncio.sleep(WORKER_DELAY_SECONDS)

                ai = await process_task_with_ai(http_client, task["payload"])
                ok = ai["engine_status"] == "SUCCESS" or ai["engine_status"].startswith("FALLBACK")
                task["status"] = "COMPLETED" if ok else "FAILED"
                task["analytics_output"] = ai["insights"]
                task["execution_logs"] = ai["engine_status"]

                await redis_client.set(f"task:{task_id}", json.dumps(task))
                await redis_client.incr("stats:processed" if ok else "stats:failed")
                print(f"Finished task: {task_id}")

            except asyncio.CancelledError:
                await redis_client.aclose()
                raise
            except Exception as e:
                print(f"Worker loop exception: {e}")
                await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(worker_orchestrator())