import os
import yaml
import time
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
import asyncio
from pathlib import Path

from transcoder_pool import TranscoderPool
from cache import LRUCache
from utils.media_setup import prepare_media
from utils.logger import CSVLogger

SERVER_CONFIG_PATH = os.getenv("SERVER_CONFIG_PATH", "/app/config.yaml")
MEDIA_DIR = os.getenv("MEDIA_DIR", "/media")
TMP_DIR = "/tmp/transcoded_segments"
POLL_INTERVAL = 0.1    # seconds

app = FastAPI()
# Globals
worker_pool = None
cache = None
coding_config = None
sequence_config = None
locks = {}  

job_done_buffer = set()


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
    global worker_pool, cache, sorted_reprs, serve_lower, segment_timeout
    global server_logger

    # Load configuration
    with open(SERVER_CONFIG_PATH, "r") as f:
        cfg = yaml.safe_load(f)


    gpu_plan = cfg.get("gpu_plan", {0: 1})
    coding_config = cfg["transcoder"]
    sequence_config = cfg["sequences"]
    cache_size = cfg["cache_size"] # in MB
    serve_lower = cfg["serve_lower"]
    #segment_timeout = 3.0 * cfg["gop_size"] / 30
    segment_timeout = 30

    sorted_reprs = sorted(coding_config.keys(),
                      key=lambda r: coding_config[r]["bw_estimate_factor"])



    
    # Server logger
    path = os.path.join(cfg["results_dir"], "backend.csv")
    fields = [
        "request_timestamp",
        "resolve_timestamp",
        "segment_id",
        "media",
        "requested_repr",
        "served_repr",
        "event",
        "queue_length",
    ]
    server_logger = CSVLogger(path, fields)


    path = os.path.join(cfg["results_dir"], "transcoder.csv")
    fields = [
        "timestamp",
        "worker_gpu",
        "worker_id",
        "event",
        "job_id",
        "q_decode",
        "q_gpu",
    ]
    transcoder_logger = CSVLogger(path, fields)

    path = os.path.join(cfg["results_dir"], "cache.csv")
    fields = [
        "timestamp",
        "event",
        "key",
        "cache_items",
        "cache_bytes"
    ]
    cache_logger = CSVLogger(path, fields)

    # Initialize transcoder pool
    worker_pool = TranscoderPool(gpu_plan, coding_config, transcoder_logger)
    worker_pool.start()
    print(f"TranscoderPool initialized with {len(coding_config)} configs")

    # Prepare Cache
    cache = LRUCache(max_bytes = cache_size * 1024 * 1024, logger=cache_logger) # MB

    # Prepare media
    prepare_media(cfg, worker_pool, Path(MEDIA_DIR))



@app.get("/{req_path:path}")
async def handle_request(req_path: str):
    """ Handle a request coming from the nginx server.
    Check if the request is valid, schedule transcoding.
    """
    request_timestamp = time.time()
    segment_id = req_path
    parts = req_path.strip("/").split("/")
    if len(parts) != 3:
        raise HTTPException(status_code=404, detail="Malformed request path")

    media_identifier, requested_repr, segment_file = parts
    media_path = os.path.join(MEDIA_DIR, req_path)

    # Serve existing file (Should never happen)
    if os.path.exists(media_path):
        print(f"Serving from storage {media_path} - This should only happen for test, otherwise change nginx conf")
        if "init.bin" not in media_path:
            resolve_timestamp = time.time()
            server_logger.log(request_timestamp=request_timestamp, 
                            resolve_timestamp=resolve_timestamp,
                            segment_id=segment_id,
                            media=media_identifier,
                            requested_repr=requested_repr,
                            served_repr=requested_repr,
                            event="storage hit",
                            queue_length=worker_pool.current_queue_length(),
                            )
        return FileResponse(media_path)

    # Serve from memore (Cache Hit)
    cached_path = cache.get(segment_id)
    if cached_path:
        print(f"[Cache Hit] {segment_id}")
        resolve_timestamp = time.time()
        server_logger.log(request_timestamp=request_timestamp, 
                          resolve_timestamp=resolve_timestamp,
                          segment_id=segment_id,
                          media=media_identifier,
                          requested_repr=requested_repr,
                          served_repr=requested_repr,
                          event="cache hit",
                          queue_length=worker_pool.current_queue_length(),
                          )
        return FileResponse(cached_path)

    # Check validity and get coding configuration + Source bitstream
    src_path, config = get_config(req_path)
    src_path = os.path.join(MEDIA_DIR, src_path)
    if config is None:
        resolve_timestamp = time.time()
        server_logger.log(request_timestamp=request_timestamp, 
                          resolve_timestamp=resolve_timestamp,
                          segment_id=segment_id,
                          media=media_identifier,
                          requested_repr=requested_repr,
                          served_repr=None,
                          event="Bad request",
                          queue_length=None,
                          )
        raise HTTPException(status_code=404, detail="Bad request")


    #if worker_pool.queue_full():
    #    if serve_lower:
    #        # iterate over representations strictly LOWER in factor
    #        for rid in sorted_reprs:
    #            fallback_segment = f"{media_identifier}/{rid}/{segment_file}"

    #            cached_fallback = cache.get(fallback_segment)

    #            if cached_fallback:
    #                print(f"[Fallback Cache Hit] Using lower rate repr '{rid}'")
    #                resolve_timestamp = time.time()
    #                server_logger.log(request_timestamp=request_timestamp, 
    #                                resolve_timestamp=resolve_timestamp,
    #                                segment_id=segment_id,
    #                                media=media_identifier,
    #                                requested_repr=requested_repr,
    #                                served_repr=rid,
    #                                event="fallback cache hit",
    #                                queue_length=worker_pool.current_queue_length(),
    #                                )
    #                return FileResponse(cached_fallback)


    #    resolve_timestamp = time.time()
    #    server_logger.log(request_timestamp=request_timestamp, 
    #                    resolve_timestamp=resolve_timestamp,
    #                    segment_id=segment_id,
    #                    media=media_identifier,
    #                    requested_repr=requested_repr,
    #                    served_repr=None,
    #                    event="Queue full",
    #                    queue_length=worker_pool.current_queue_length(),
    #                    )
    #    raise HTTPException(status_code=503, detail="Segment transcoding failed - server busy")

    # Generate tmp path
    final_path = os.path.join(TMP_DIR, segment_id.replace("/", "_"))
    tmp_path = final_path + ".tmp"
    os.makedirs(os.path.dirname(final_path), exist_ok=True)

    async with get_lock(segment_id):
        # Maybe another cache check if another job finished the transcoding while waiting
        cached_path = cache.get(segment_id)
        if cached_path and os.path.exists(cached_path):
            print(f"[Cache Hit After Wait] {segment_id}")
            resolve_timestamp = time.time()
            server_logger.log(
                request_timestamp=request_timestamp,
                resolve_timestamp=resolve_timestamp,
                segment_id=segment_id,
                media=media_identifier,
                requested_repr=requested_repr,
                served_repr=requested_repr,
                event="cache hit (delayed)",
                queue_length=worker_pool.current_queue_length(),
            )
            return FileResponse(cached_path)

        # Enqueue transcoding
        job_id = worker_pool.submit(config["id"], src_path, tmp_path)
        print(f"[Submitted] {segment_id}")

        ok = await wait_for_job(job_id)

        if not ok or not os.path.exists(tmp_path):
            resolve_timestamp = time.time()
            server_logger.log(
                request_timestamp=request_timestamp,
                resolve_timestamp=resolve_timestamp,
                segment_id=segment_id,
                media=media_identifier,
                requested_repr=requested_repr,
                served_repr=None,
                event="timeout",
                queue_length=worker_pool.current_queue_length(),
            )
            raise HTTPException(status_code=504, detail="Segment transcoding timeout")
    
        # Atomic change
        os.replace(tmp_path, final_path)

        cache.add(segment_id, final_path)
        resolve_timestamp = time.time()
        server_logger.log(request_timestamp=request_timestamp, 
                        resolve_timestamp=resolve_timestamp,
                        segment_id=segment_id,
                        media=media_identifier,
                        requested_repr=requested_repr,
                        served_repr=requested_repr,
                        event="transcoded",
                        queue_length=worker_pool.current_queue_length(),
                        )
        return FileResponse(final_path)


async def wait_for_job(job_id):
    start = time.time()
    while time.time() - start < segment_timeout:
        # First check buffer
        if job_id in job_done_buffer:
            job_done_buffer.remove(job_id)
            return True

        try:
            finished_id = worker_pool.done_queue.get_nowait()
            if finished_id == job_id:
                return True
            else:
                job_done_buffer.add(finished_id)
        except Exception:
            pass

        await asyncio.sleep(POLL_INTERVAL)
    return False



def get_config(media_path):
    path_elements = media_path.strip('/').split("/")
    
    if len(path_elements) != 3:
        print(f"Invalid path format. Expected 4 elements, got {len(path_elements)}")
        return None, None
    
    media_identifier = path_elements[0]  # redandblack,loot
    rate_identifier = path_elements[1]   # 1-5
    segment_file = path_elements[2]      # seg_XXX.bin
    
    print(f"Parsed:  media={media_identifier}, rate={rate_identifier}, file={segment_file}")
    
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
    
    config = coding_config[rate_identifier]
    config["id"] = rate_identifier
    
    src_path = f"{media_identifier}/{encoding_config}/{segment_file}"
    
    return src_path, config