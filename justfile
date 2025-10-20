# Optional: ensure submodules are ready
init-submodules:
    git submodule update --init --recursive

# Submodule forwards
pyrabbit *ARGS:
    @just --justfile spirit-pyrabbit/justfile {{ARGS}}

# Building the base transcoder submodule
build-pyrabbit:
    just pyrabbit build


# Start the transcoding server
start-server:
	docker compose -f docker-compose.yaml --profile build-only up --build
	#docker compose -f docker-compose.yaml up --build
