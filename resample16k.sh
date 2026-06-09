#!/bin/bash

mkdir -p output_16k

for f in *.wav; do
  ffmpeg -y -i "$f" -ar 16000 "output_16k/$f"
done
