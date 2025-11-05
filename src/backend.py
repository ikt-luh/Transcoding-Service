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

SERVER_CONFIG_PATH = os.getenv("SERVER_CONFIG_PATH", "/app/config.yaml")
MEDIA_DIR = os.getenv("MEDIA_DIR", "/media")
TMP_DIR = "/tmp/transcoded_segments"
SEGMENT_TIMEOUT = 2.0  # seconds (≈ 1.5 × GOP duration)
POLL_INTERVAL = 0.1    # seconds

app = FastAPI()
# Globals
worker_pool = None
cache = None
coding_config = None
sequence_config = None
locks = {}  # per-segment asyncio locks

def get_lock(segment_id: str):
    """Per-segment lock to prevent duplicate transcoding."""
    if segment_id not in locks:
        locks[segment_id] = asyncio.Lock()
    return locks[segment_id]


@app.on_event("startup")
async def startup_event():
    """ Start up of the media server
    """
    global coding_config, sequence_config, worker_pool, cache

    # Load configuration
    with open(SERVER_CONFIG_PATH, "r") as f:
        cfg = yaml.safe_load(f)

    gpu_plan = cfg.get("gpu_plan", {0: 1})
    coding_config = cfg["transcoder"]
    sequence_config = cfg["sequences"]

    # Initialize transcoder pool
    worker_pool = TranscoderPool(gpu_plan, coding_config)
    worker_pool.start()
    print(f"TranscoderPool initialized with {len(coding_config)} configs")

    # Prepare media
    prepare_media(cfg, worker_pool, Path(MEDIA_DIR))

    # Prepare Cache
    cache = LRUCache(max_bytes = 32 * 1024 * 1024 ) # 52 MB



@app.get("/{req_path:path}")
async def handle_request(req_path: str):
    """ Handle a request coming from the nginx server.
    Check if the request is valid, schedule transcoding.
    """
    segment_id = req_path
    media_path = os.path.join(MEDIA_DIR, req_path)

    # Serve existing file (Should never happen)
    if os.path.exists(media_path):
        print(f"Serving from storage {media_path} - This should never happen?")
        return FileResponse(media_path)

    # Serve from memore (Cache Hit)
    cached_path = cache.get(segment_id)
    if cached_path:
        print(f"[Cache Hit] {segment_id}")
        return FileResponse(cached_path)

    # Check validity and get coding configuration + Source bitstream
    src_path, config = get_config(req_path)
    src_path = os.path.join(MEDIA_DIR, src_path)
    if config is None:
        raise HTTPException(status_code=404, detail="Bad request")

    # Schedule transcoding 
    lock = get_lock(segment_id)
    async with lock:
        # Double check
        if cache.get(segment_id):
            print(f"[Cache Hit] {segment_id}")
            return FileResponse(cache.get(segment_id))
        
        # Generate tmp path
        tmp_path = os.path.join(TMP_DIR, segment_id.replace("/", "_"))
        os.makedirs(os.path.dirname(tmp_path), exist_ok=True)

        # Enqueue transcoding
        try:
            job_id = worker_pool.submit(config["id"] if "id" in config else list(coding_config.keys())[0],
                               src_path, tmp_path)
            print(f"[Submitted] {segment_id}")
        except Exception as e:
            print(f"Failed to enqueue transcoding: {e}")
            raise HTTPException(status_code=500, detail="Failed to start transcoding")

        ok = await wait_for_job(job_id)

        if not ok or not os.path.exists(tmp_path):
            raise HTTPException(status_code=504, detail="Segment transcoding timeout")

        cache.add(segment_id, tmp_path)
        return FileResponse(tmp_path)


async def wait_for_job(job_id):
    start = time.time()
    while time.time() - start < SEGMENT_TIMEOUT:
        try:
            finished_id = worker_pool.done_queue.get_nowait()
            if finished_id == job_id:
                return True
        except Exception:
            pass
        await asyncio.sleep(POLL_INTERVAL)
    return False


def get_config(media_path):
    path_elements = media_path.strip('/').split("/")
    print(path_elements)
    
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
    
    src_path = f"{media_identifier}/{encoding_config}/{segment_file}"
    
    return src_path, config