import os
import yaml
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
import asyncio
from pathlib import Path

from transcoder_pool import TranscoderPool
from cache import LRUCache
from utils.media_setup import prepare_media

SERVER_CONFIG_PATH = os.getenv("SERVER_CONFIG_PATH", "/app/config.yaml")
MEDIA_DIR = os.getenv("MEDIA_DIR", "/app/media")

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    """ Start up of the media server
    """
    global coding_config, sequence_config, worker_pool

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

    # Prepare Cache?



@app.get("/{req_path:path}")
async def handle_request(req_path: str):
    """ Handle a request coming from the nginx server.
    Check if the request is valid, schedule transcoding.
    """
    media_path = os.path.join(MEDIA_DIR, req_path)

    # Serve existing file
    if os.path.exists(media_path):
        print(f"Serving from storage {media_path} - This should never happen?")
        return FileResponse(media_path)

    # Check validity and get coding configuration
    src_path, config = get_config(req_path)
    src_path = os.path.join(MEDIA_DIR, src_path)
    if config is None:
        raise HTTPException(status_code=404, detail="Bad request")

    worker_pool.submit(src_path, media_path, config)

    
    # Respond to the request (TODO: make config paramters)
    timeout = 2.0 
    interval = 0.1
    waited = 0.0
    while waited < timeout:
        # Maybe keep virtual storage of this!
        if os.path.exists(media_path):
            return FileResponse(media_path)
        await asyncio.sleep(interval)
        waited += interval

    # Nach Timeout → HTTP 504
    raise HTTPException(status_code=504, detail="Transcoding timeout")



def get_config(media_path):
    path_elements = media_path.strip('/').split("/")
    
    if len(path_elements) != 4:
        print(f"Invalid path format. Expected 4 elements, got {len(path_elements)}")
        return None, None
    
    duration_segment = path_elements[0]  # 1s_segments
    media_identifier = path_elements[1]  # redandblack,loot
    rate_identifier = path_elements[2]   # 1-5
    segment_file = path_elements[3]      # seg_XXX.bin
    
    print(f"Parsed: duration={duration_segment}, media={media_identifier}, rate={rate_identifier}, file={segment_file}")
    
    if media_identifier not in sequence_config:
        print(f"{media_identifier} not found in sequence_config")
        return None, None
    
    offered_configs = [str(c) for c in sequence_config[media_identifier]["offered_configs"]]
    encoding_config = str(sequence_config[media_identifier]["encoding_config"])
    
    if encoding_config not in offered_configs:
        offered_configs.append(encoding_config)
    
    if rate_identifier not in offered_configs:
        print(f"{rate_identifier} not a valid quality config for {media_identifier}.")
        return None, None
    
    if rate_identifier not in coding_config:
        print(f"{rate_identifier} not found in transcoder config")
        return None, None
    
    config = coding_config[rate_identifier]
    
    src_path = f"{duration_segment}/{media_identifier}/{encoding_config}/{segment_file}"
    
    return src_path, config