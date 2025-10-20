import os
import yaml
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
import asyncio

from transcoder_pool import TranscoderPool

MEDIA_DIR = os.environ.get("MEDIA_DIR", "/app/media")

def load_config():
    config_path = os.getenv("SERVER_CONFIG_PATH")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    coding_configs = config["transcoder"]
    sequence_configs = config["sequences"]
    return coding_configs, sequence_configs

app = FastAPI()

coding_config, sequence_config = load_config()

worker_pool = TranscoderPool(3)
worker_pool.start()


@app.get("/{req_path:path}")
async def handle_request(req_path: str):
    media_path = os.path.join(MEDIA_DIR, req_path)

    # Serve existing file
    if os.path.exists(media_path):
        print(f"Serving from storage {media_path} - This should never happen?")
        return FileResponse(media_path)

    # Check validity and get coding config
    src_path, config = get_config(req_path)
    src_path = os.path.join(MEDIA_DIR, src_path)
    if config is None:
        raise HTTPException(status_code=404, detail="Bad request")

    worker_pool.submit(src_path, media_path, config)

    timeout = 2.0 # test
    interval = 0.1
    waited = 0.0
    
    while waited < timeout:
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