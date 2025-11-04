# SPIRIT PyRABBIT

This repository is part of the implementation for the SPIRIT project.

## Description
It contains code for the RABBIT transcoder in Python.

## Setup
Setup is easy using just.
```
    sudo apt install just
```

Further, install 7zip and some other tools:
```
    sudo apt update && sudo apt install -y p7zip-full wget
```

For some experiments, we patched NVIDIA Drivers to circumvent the 8 encoding session limit. Follow the (instrctions)[https://github.com/keylase/nvidia-patch.git]

### Encoding Data
First, we need to prepare data for the media server.
To encode the 8iVFBv2 dataset in quality R5 with a specified segment size to be hosted on the server, first, get the dataset:
```
    just download-8i
```
And then, build and run a transcoder-container which contains the mpeg-tmc2 test model for encoding:
```
    just build-transcoder
    just run-transcoder
```
In the container, run
```
    just encode-8i
```
to extend each sequence to 600 frames (looped) and encode them at R5 with segment lengths 1s, 2s and 4s.
If everything worked correctly, you should have a folder "/media/encoded/Xs_encodings" containing the .bin files.


## Experiments

### Latency/Transcoding Experiments
These experiments aim at testing configurations of experiments of the codec. An experiment can be done in the following way

Start a transcoder container (no service, just the tooling):
```
    just build-pyrabbit
    just run-pyrabbit
```

In the container, 
```
    cd /
    python3 scripts/transcoding_setting_experiment.py /configs/experiments/transcoding_times/settings.yaml
```


### Streaming Experiments
To prepare a streaming experiment (server-side), we will need to write a server configuration that describes the transcoding server behaviour.
Examples can be found in TODO.



## Usage
To run the container, start it interactively with
```
    just run-transcoder
```
