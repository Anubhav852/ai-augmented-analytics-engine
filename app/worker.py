import asyncio
import json
import os
import re
import time
from collections import Counter

import httpx
import redis.asyncio as aioredis

from app.config import settings

QUEUE = "ai_analytics_queue"
# Optional demo knob: artificial delay per task. Leave at 0 for the AI vs rule-based comparison.
WORKER_DELAY_SECONDS = float(os.getenv("WORKER_DELAY_SECONDS", "0"))

SYSTEM_PROMPT = (
    "You analyse customer and system text. Reply with ONLY a JSON object with these keys: "
    "summary (one sentence), sentiment (positive, negative or neutral), "
    "top_keywords (list of up to 5 strings), urgency (low, medium or high)."
)

STOPWORDS = set(
    "the a an and or but if of to in on at for with is are was were be been it this that as by from "
    "we you they i our your their its not no so than then there have has had will would can could".split()
)
POSITIVE = {"great", "good", "excellent", "love", "fast", "smooth", "happy", "success", "improved", "stable", "win"}
NEGATIVE = {"bad", "slow", "error", "fail", "failed", "timeout", "crash", "broken", "down", "outage", "angry", "churn"}


def local_analysis(text: str) -> dict:
    """Rule-based analysis: word counting and keyword matching. No AI involved."""
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
        "source": "rule-based analyzer (not an AI model)",
    }


def parse_model_json(content: str):
    cleaned = re.sub(r"^```(?:json)?|```$", "", content.strip(), flags=re.MULTILINE).strip()
    try:
        return json.loads(cleaned)
    except Exception:
        return {"raw_output": content}


async def process_task_with_ai(client: httpx.AsyncClient, text: str, use_ai: bool) -> dict:
    """Real AI call when enabled and configured; otherwise (or on any failure) rule-based analysis."""
    if not use_ai:
        reason = "AI_DISABLED_FOR_THIS_REQUEST"
    elif not settings.AI_API_TOKEN:
        reason = "NO_AI_TOKEN_CONFIGURED"
    else:
        try:
            response = await client.post(
                settings.AI_API_URL,
                headers={"Authorization": f"Bearer {settings.AI_API_TOKEN}"},
                json={
                    "model": settings.AI_MODEL,
                    "messages": [
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": text},
                    ],
                    "temperature": 0.2,
                    "max_tokens": 250,
                },
                timeout=15.0,
            )
            if response.status_code == 200:
                content = response.json()["choices"][0]["message"]["content"]
                insights = parse_model_json(content)
                insights["source"] = f"AI model: {settings.AI_MODEL}"
                return {"insights": insights, "engine_status": f"AI_SUCCESS ({settings.AI_MODEL})"}
            reason = f"AI_PROVIDER_ERROR_CODE_{response.status_code}"
        except Exception as e:
            reason = f"AI_PROVIDER_EXCEPTION_{type(e).__name__}"
    return {"insights": local_analysis(text), "engine_status": f"FALLBACK_LOCAL_ANALYSIS ({reason})"}


async def worker_orchestrator():
    """Background loop: BLPOP from Redis, analyse, store the result with timings."""
    print("AI worker daemon starting...")
    redis_client = aioredis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
        socket_timeout=30,  # must be longer than the 5s BLPOP wait
        socket_keepalive=True,
        health_check_interval=30,
    )

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

                started = time.perf_counter()
                ai = await process_task_with_ai(http_client, task["payload"], task.get("use_ai", True))
                processing_ms = (time.perf_counter() - started) * 1000

                ok = ai["engine_status"].startswith(("AI_SUCCESS", "FALLBACK"))
                task["status"] = "COMPLETED" if ok else "FAILED"
                task["analytics_output"] = ai["insights"]
                task["execution_logs"] = ai["engine_status"]
                task["processing_ms"] = round(processing_ms, 1)
                task["total_ms"] = round((time.time() - task.get("created_at", time.time())) * 1000, 1)

                await redis_client.set(f"task:{task_id}", json.dumps(task))
                await redis_client.incr("stats:processed" if ok else "stats:failed")
                print(f"Finished task: {task_id} in {processing_ms:.0f} ms ({ai['engine_status']})")

            except asyncio.CancelledError:
                await redis_client.aclose()
                raise
            except aioredis.TimeoutError:
                continue
            except Exception as e:
                print(f"Worker loop exception: {e}")
                await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(worker_orchestrator())