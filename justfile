# Variables
TRANSCODER_IMAGE := "pyrabbit-transcoder"
TMC2_IMAGE := "tmc2"
DOCKER_TAG := "latest"

# Initialize git submodules
init-submodules:
    git submodule update --init --recursive

# Forward pyrabbit commands
pyrabbit *ARGS:
    @just --justfile spirit-pyrabbit/justfile {{ARGS}}

# Build the transcoder Docker image via submodule
build-pyrabbit:
    just pyrabbit build

build-pyrabbit-hw:
    just pyrabbit build-hw

# Run the transcoder container interactively
run-pyrabbit:
    docker run --rm -it \
        -v ./scripts:/scripts:z \
        -v ./media:/media:z \
        -v ./configs:/configs:z \
        -v ./results:/results:z \
        {{TRANSCODER_IMAGE}}:{{DOCKER_TAG}}

run-pyrabbit-hw:
    docker run --rm -it \
        --gpus all \
        --runtime=nvidia \
        -e NVIDIA_VISIBLE_DEVICES=all \
        -e NVIDIA_DRIVER_CAPABILITIES=all \
        -v ./scripts:/scripts:z \
        -v ./media:/media:z \
        -v ./configs:/configs:z \
        -v ./results:/results:z \
        {{TRANSCODER_IMAGE}}:{{DOCKER_TAG}}

# Start the transcoding server using Docker Compose
start-server:
    docker compose -f docker-compose.yaml --profile build-only up --build

# Download and extract the 8iVFBv2 dataset
download-8i:
    mkdir -p media
    cd media && \
    wget https://plenodb.jpeg.org/pc/8ilabs/8iVFBv2.7z && \
    unzip 8iVFBv2.7z && \
    rm 8iVFBv2.7z