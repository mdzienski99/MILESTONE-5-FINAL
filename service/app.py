from fastapi import FastAPI, Query
from fastapi.responses import Response
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from collections import Counter as PyCounter
from pathlib import Path
from typing import Optional
import pandas as pd
import json
import os
import time
import uuid

from service.config import (
    MODEL_VERSION,
    POPULARITY_PATH,
    ITEM_CF_PATH,
    SNAPSHOT_PATH,
    REGISTRY_PATH,
    TRACE_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_RECO_REQUESTS,
    TOPIC_RECO_RESPONSES,
    GIT_SHA,
    CONTAINER_IMAGE_DIGEST,
)

try:
    from confluent_kafka import Producer
    print("confluent_kafka import OK")
except ImportError as e:
    print("confluent_kafka import FAILED:", e)
    Producer = None

app = FastAPI()

reqs = Counter("recommend_requests_total", "requests", ["status"])
errors = Counter("recommend_errors_total", "errors")
lat = Histogram("recommend_latency_seconds", "latency")

PRODUCER = None
START_TIME = time.time()

Path("artifacts").mkdir(exist_ok=True)


def load_json(path):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)


def load_registry():
    if os.path.exists(REGISTRY_PATH):
        return load_json(REGISTRY_PATH)

    default_registry = {
        "active_version": MODEL_VERSION,
        "versions": {
            MODEL_VERSION: {
                "created_at": "local",
                "models": {
                    "popularity": POPULARITY_PATH,
                    "item_cf": ITEM_CF_PATH,
                },
                "data_snapshot_id": os.path.basename(SNAPSHOT_PATH),
                "pipeline_git_sha": GIT_SHA,
                "container_image_digest": CONTAINER_IMAGE_DIGEST,
            }
        },
    }
    write_json(REGISTRY_PATH, default_registry)
    return default_registry


def get_version_info(version: Optional[str] = None):
    registry = load_registry()
    active_version = version or registry.get("active_version", MODEL_VERSION)
    versions = registry.get("versions", {})
    if active_version not in versions:
        raise ValueError(f"Unknown model version: {active_version}")
    return registry, active_version, versions[active_version]


def choose_model(user_id: int, requested_model: str):
    if requested_model in {"popularity", "item_cf"}:
        return requested_model, "manual"
    if requested_model == "auto":
        if user_id % 2 == 0:
            return "popularity", "A"
        return "item_cf", "B"
    raise ValueError("model must be 'popularity', 'item_cf', or 'auto'")


def get_user_history(user_id: int):
    if not os.path.exists(SNAPSHOT_PATH):
        return []

    df = pd.read_csv(SNAPSHOT_PATH)
    if "user_id" not in df.columns:
        return []

    movie_col = "movie_id" if "movie_id" in df.columns else "item_id"
    if movie_col not in df.columns:
        return []

    return df[df["user_id"] == user_id][movie_col].tolist()


def get_popularity_recs(k: int, pop_path: str):
    artifact = load_json(pop_path)
    if not artifact:
        raise FileNotFoundError(f"Missing popularity artifact: {pop_path}")
    return artifact["items"][:k]


def get_item_cf_recs(user_id: int, k: int, pop_path: str, cf_path: str):
    artifact = load_json(cf_path)
    if not artifact:
        raise FileNotFoundError(f"Missing item_cf artifact: {cf_path}")

    history = get_user_history(user_id)
    if not history:
        return get_popularity_recs(k, pop_path)

    counts = PyCounter()
    neighbors = artifact["neighbors"]

    for movie_id in history:
        for related in neighbors.get(str(movie_id), []):
            if related not in history:
                counts[related] += 1

    recs = [movie for movie, _ in counts.most_common(k)]
    if not recs:
        return get_popularity_recs(k, pop_path)

    return recs[:k]


def append_trace(trace: dict):
    with open(TRACE_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(trace) + "\n")


def _create_producer():
    global PRODUCER

    if PRODUCER is not None:
        return PRODUCER

    if Producer is None:
        return None

    try:
        PRODUCER = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
        print("Kafka producer created")
        return PRODUCER
    except Exception as e:
        print("Kafka not available:", e)
        return None


def _publish_event(topic, event):
    producer = _create_producer()

    if producer is None:
        return False

    try:
        producer.poll(0)
        producer.produce(topic, json.dumps(event).encode("utf-8"))
        producer.flush()
        return True
    except BufferError:
        print(f"Kafka buffer full, dropped event for topic={topic}")
        return False
    except Exception as e:
        print("Kafka publish failed:", topic, e)
        return False


@app.get("/healthz")
def healthz():
    registry = load_registry()
    return {
        "status": "ok",
        "active_version": registry.get("active_version", MODEL_VERSION),
        "uptime_seconds": round(time.time() - START_TIME, 2),
    }


@app.get("/switch")
def switch(version: str):
    registry = load_registry()
    if version not in registry.get("versions", {}):
        return {"error": f"Unknown version: {version}"}

    registry["active_version"] = version
    write_json(REGISTRY_PATH, registry)
    return {"status": "ok", "active_version": version}


@app.get("/trace/{request_id}")
def trace_lookup(request_id: str):
    if not os.path.exists(TRACE_PATH):
        return {"error": "No trace file yet"}

    with open(TRACE_PATH, "r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
                if obj.get("request_id") == request_id:
                    return obj
            except Exception:
                continue

    return {"error": "Trace not found"}


@app.get("/recommend/{user_id}")
@lat.time()
def recommend(
    user_id: int,
    k: int = 10,
    model: str = Query(default="auto"),
    version: Optional[str] = Query(default=None),
):
    request_id = str(uuid.uuid4())
    request_ts = time.time()

    try:
        registry, active_version, version_info = get_version_info(version)
        chosen_model, ab_group = choose_model(user_id, model)

        pop_path = version_info["models"]["popularity"]
        cf_path = version_info["models"]["item_cf"]

        request_event = {
            "request_id": request_id,
            "user_id": user_id,
            "k": k,
            "model_requested": model,
            "model_served": chosen_model,
            "ab_group": ab_group,
            "model_version": active_version,
            "event_ts": request_ts,
            "source": "api",
        }
        _publish_event(TOPIC_RECO_REQUESTS, request_event)

        if chosen_model == "popularity":
            recs = get_popularity_recs(k, pop_path)
        else:
            recs = get_item_cf_recs(user_id, k, pop_path, cf_path)

        trace = {
            "request_id": request_id,
            "user_id": user_id,
            "k": k,
            "model_requested": model,
            "model_served": chosen_model,
            "ab_group": ab_group,
            "model_version": active_version,
            "data_snapshot_id": version_info.get("data_snapshot_id", os.path.basename(SNAPSHOT_PATH)),
            "pipeline_git_sha": version_info.get("pipeline_git_sha", GIT_SHA),
            "container_image_digest": version_info.get("container_image_digest", CONTAINER_IMAGE_DIGEST),
            "recommendations": recs,
            "event_ts": time.time(),
            "source": "api",
        }

        append_trace(trace)
        _publish_event(TOPIC_RECO_RESPONSES, trace)

        reqs.labels("200").inc()
        return trace

    except Exception as e:
        errors.inc()
        reqs.labels("500").inc()
        return {"error": str(e), "request_id": request_id}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)