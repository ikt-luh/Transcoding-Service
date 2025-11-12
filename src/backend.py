import os
import yaml
import time
import asyncio
from pathlib import Path
from collections import defaultdict

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse

from transcoder_pool import TranscoderPool
from cache import LRUCache
from utils.media_setup import prepare_media
from utils.logger import CSVLogger

SERVER_CONFIG_PATH = os.getenv("SERVER_CONFIG_PATH", "/app/config.yaml")
MEDIA_DIR = os.getenv("MEDIA_DIR", "/media")
TMP_DIR = "/tmp/transcoded_segments"

#POLL_INTERVAL = 0.1    # seconds

app = FastAPI()

# Globals
worker_pool = None
cache = None
coding_config = None
sequence_config = None
sorted_reprs = None
segment_timeout = None
sever_logger = None

locks = defaultdict(asyncio.Lock)  


def get_lock(segment_id: str):
    """Per-segment lock to prevent duplicate transcoding."""
    if segment_id not in locks:
        locks[segment_id] = asyncio.Lock()
    return locks[segment_id]


@app.on_event("startup")
async def startup_event():
    """ Start up of the media server
    """
    global coding_config, sequence_config 
    global worker_pool, cache, sorted_reprs, prefetch_depth, segment_timeout
    global server_logger

    # Load configuration
    with open(SERVER_CONFIG_PATH, "r") as f:
        cfg = yaml.safe_load(f)

    gpu_plan = cfg.get("gpu_plan", {0: 1})
    coding_config = cfg["transcoder"]
    sequence_config = cfg["sequences"]
    cache_size = cfg["cache_size"] # in MB
    prefetch_depth = cfg.get("prefetch_depth", 0)
    if cache_size <= 0:
        prefetch_depth = 0 # No prefetching without a cache
    segment_timeout = 30
    print(prefetch_depth)

    sorted_reprs = sorted(
        coding_config.keys(),
        key=lambda r: coding_config[r]["bw_estimate_factor"]
    )
    
    # Logging
    results_dir = cfg["results_dir"]

    server_logger = CSVLogger(
        os.path.join(results_dir, "backend.csv"),
        ["request_timestamp", "resolve_timestamp", "segment_id", "media",
         "requested_repr", "served_repr", "event", "queue_length", "client_id"]
    )

    transcoder_logger = CSVLogger(
        os.path.join(results_dir, "transcoder.csv"),
        ["timestamp", "worker_gpu", "worker_id", "event", "job_id", "q_decode", "q_gpu"]
    )

    cache_logger = CSVLogger(
        os.path.join(results_dir, "cache.csv"),
        ["timestamp", "event", "key", "cache_items", "cache_bytes"]
    )

    worker_pool = TranscoderPool(gpu_plan, coding_config, transcoder_logger)
    worker_pool.start()

    cache = LRUCache(max_bytes = cache_size * 1024 * 1024, logger=cache_logger) # MB

    await prepare_media(cfg, worker_pool, Path(MEDIA_DIR))


@app.get("/{req_path:path}")
async def handle_request(req_path: str, request: Request):
    """ Handle a request coming from the nginx server.
    Check if the request is valid, schedule transcoding.
    """
    request_timestamp = time.time()
    client_id = request.client.host  # simplest stable identifier

    parts = req_path.strip("/").split("/")
    if len(parts) != 3:
        raise HTTPException(status_code=404, detail="Malformed request path")

    media_identifier, requested_repr, segment_file = parts
    segment_id = req_path

    # Direct file check (Should only happen for dev nginx.conf)
    media_path = os.path.join(MEDIA_DIR, req_path)
    if os.path.exists(media_path):
        if "init.bin" not in media_path:
            resolve_timestamp = time.time()
            server_logger.log(request_timestamp=request_timestamp, 
                            resolve_timestamp=resolve_timestamp,
                            segment_id=segment_id,
                            media=media_identifier,
                            requested_repr=requested_repr,
                            served_repr=requested_repr,
                            client_id=client_id,
                            event="storage hit",
                            queue_length=worker_pool.current_queue_length(),
                            )
        return FileResponse(media_path)

    # Serve from memore (Cache Hit)
    cached_path = cache.get(segment_id)
    if cached_path:
        create_prefetch_tasks(segment_id)

        resolve_timestamp = time.time()
        server_logger.log(request_timestamp=request_timestamp, 
                          resolve_timestamp=resolve_timestamp,
                          segment_id=segment_id,
                          media=media_identifier,
                          requested_repr=requested_repr,
                          served_repr=requested_repr,
                          event="cache hit",
                          client_id=client_id,
                          queue_length=worker_pool.current_queue_length(),
                          )
        return FileResponse(cached_path)

    # Check validity and get coding configuration
    src_path, config = get_config(req_path)
    if config is None:
        resolve_timestamp = time.time()
        server_logger.log(request_timestamp=request_timestamp, 
                          resolve_timestamp=resolve_timestamp,
                          segment_id=segment_id,
                          media=media_identifier,
                          requested_repr=requested_repr,
                          served_repr=None,
                          client_id=client_id,
                          event="Bad request",
                          queue_length=None,
                          )
        raise HTTPException(status_code=404, detail="Bad request")

    # Generate paths
    src_path = os.path.join(MEDIA_DIR, src_path)
    final_path = os.path.join(TMP_DIR, segment_id.replace("/", "_"))
    tmp_path = final_path + ".tmp"
    os.makedirs(os.path.dirname(final_path), exist_ok=True)

    if cache.max_bytes > 0:
        # Use lock to deduplicate work
        async with get_lock(segment_id):
            cached_path = cache.get(segment_id)
            if cached_path and os.path.exists(cached_path):
                return FileResponse(cached_path)

            return await transcode_segment(config["id"], src_path, tmp_path, final_path,
                                        segment_id, request_timestamp,
                                        media_identifier, requested_repr, client_id)
    else:
        # No cache: always transcode (no locking, no late check)
        return await transcode_segment(config["id"], src_path, tmp_path, final_path,
                                    segment_id, request_timestamp,
                                    media_identifier, requested_repr, client_id)


async def transcode_segment(config_id, src_path, tmp_path, final_path, segment_id, request_timestamp,
                            media_identifier, requested_repr, client_id):
    job_id = worker_pool.submit(config_id, src_path, tmp_path)

    ok = await worker_pool.wait_job(job_id, timeout=segment_timeout)
    if not ok:
        create_prefetch_tasks(segment_id)

        resolve_timestamp = time.time()
        server_logger.log(
            request_timestamp=request_timestamp,
            resolve_timestamp=resolve_timestamp,
            segment_id=segment_id,
            media=media_identifier,
            requested_repr=requested_repr,
            served_repr=None,
            client_id=client_id,
            event="timeout",
            queue_length=worker_pool.current_queue_length(),
        )
        raise HTTPException(status_code=504, detail="Segment transcoding timeout")

    if not os.path.exists(tmp_path):
        raise HTTPException(status_code=504, detail="Segment transcoding failed (no output)")

    os.replace(tmp_path, final_path)

    cache.add(segment_id, final_path)

    resolve_timestamp = time.time()
    server_logger.log(
        request_timestamp=request_timestamp,
        resolve_timestamp=resolve_timestamp,
        segment_id=segment_id,
        media=media_identifier,
        requested_repr=requested_repr,
        served_repr=requested_repr,
        event="transcoded",
        client_id=client_id,
        queue_length=worker_pool.current_queue_length(),
    )

    create_prefetch_tasks(segment_id)

    return FileResponse(final_path)


def create_prefetch_tasks(segment_id):
    # Speculative prefetch
    next_ids = next_segment_id(segment_id)
    if prefetch_depth > 0 and cache.max_bytes > 0:
        if next_ids:
            for next_id in next_ids:
                if cache.get(next_id):
                    continue

                asyncio.create_task(prefetch_segment(next_id))

def next_segment_id(current_segment_id):
    media, rep, filename = current_segment_id.split("/")
    if not filename.startswith("seg_") or not filename.endswith(".bin"):
        return None
    num = int(filename[4:-4])
    next_ids = []
    for i in range(prefetch_depth):
        num += 1
        next_file = f"seg_{num:03d}.bin"
        next_ids.append(f"{media}/{rep}/{next_file}")

    return next_ids

async def try_acquire(lock):
    if lock.locked():
        return False
    try:
        await asyncio.wait_for(lock.acquire(), timeout=0.01)
        return True
    except asyncio.TimeoutError:
        return False


async def prefetch_segment(segment_id):
    lock = get_lock(segment_id)

    if not await try_acquire(lock):
        return  # another request or prefetch is working

    try:
        if cache.get(segment_id):
            return

        src_path, config = get_config(segment_id)
        if config is None:
            return

        src_path = os.path.join(MEDIA_DIR, src_path)
        final_path = os.path.join(TMP_DIR, segment_id.replace("/", "_"))
        tmp_path = final_path + ".tmp"
        os.makedirs(os.path.dirname(final_path), exist_ok=True)

        if not os.path.exists(src_path):
            return #Might not have another segment

        job_id = worker_pool.submit(config["id"], src_path, tmp_path)
        ok = await worker_pool.wait_job(job_id, timeout=segment_timeout)

        if ok and os.path.exists(tmp_path):
            os.replace(tmp_path, final_path)
            cache.add(segment_id, final_path)
    finally:
        lock.release()


def get_config(media_path):
    path_elements = media_path.strip('/').split("/")
    
    if len(path_elements) != 3:
        print(f"Invalid path format. Expected 4 elements, got {len(path_elements)}")
        return None, None
    
    media_identifier = path_elements[0]  # redandblack,loot
    rate_identifier = path_elements[1]   # 1-5
    segment_file = path_elements[2]      # seg_XXX.bin
    
    #print(f"Parsed:  media={media_identifier}, rate={rate_identifier}, file={segment_file}")
    
    if media_identifier not in sequence_config:
        print(f"{media_identifier} not found in sequence_config")
        return None, None
    
    offered_configs = [str(c) for c in sequence_config[media_identifier]["offered_configs"]]
    encoding_config = str(sequence_config[media_identifier]["base_config"])
    
    if rate_identifier not in offered_configs:
        print(f"{rate_identifier} not a valid quality config for {media_identifier}.")
        return None, None
    
    if rate_identifier not in coding_config:
        print(f"{rate_identifier} not found in transcoder config")
        return None, None
    
    config = dict(coding_config[rate_identifier])
    config["id"] = rate_identifier
    
    src_path = f"{media_identifier}/{encoding_config}/{segment_file}"
    
    return src_path, config