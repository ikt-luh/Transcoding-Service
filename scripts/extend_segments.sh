#!/usr/bin/env bash

SRC_BASE="media/encoded"
DST_BASE="media/encoded_80s"

# Create destination base if needed
mkdir -p "$DST_BASE"

for LENGTH_DIR in "$SRC_BASE"/*; do
  LENGTH=$(basename "$LENGTH_DIR")

  for SEQ_DIR in "$LENGTH_DIR"/*; do
    SEQ=$(basename "$SEQ_DIR")

    SRC="$SRC_BASE/$LENGTH/$SEQ/5"
    DST="$DST_BASE/$LENGTH/$SEQ/5"

    [ -d "$SRC" ] || continue

    mkdir -p "$DST"

    echo "Processing $SRC to $DST"

    # Copy original 20s segments
    cp "$SRC"/seg_*.bin "$DST"/

    # Count original segments (N)
    N=$(ls "$SRC"/seg_*.bin | wc -l)
    N=$((N))

    # Extend to 4×N segments
    for i in $(seq 0 $((N-1))); do
      src=$(printf "%s/seg_%03d.bin" "$SRC" "$i")

      dest1=$(printf "%s/seg_%03d.bin" "$DST" $((i+N)))
      dest2=$(printf "%s/seg_%03d.bin" "$DST" $((i+2*N)))
      dest3=$(printf "%s/seg_%03d.bin" "$DST" $((i+3*N)))

      cp "$src" "$dest1"
      cp "$src" "$dest2"
      cp "$src" "$dest3"
    done

  done
    done

  done
done
