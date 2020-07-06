#!/bin/sh

SRC_BATCH=/home/hadoop/src
POSTS=/user/batch/posts.xml

echo "[X] Started top_tags batch: $(date)"
spark-submit $SRC_BATCH/batch_top_tags.py $POSTS
echo "[X] Finished top_tags batch: $(date)"

echo "[X] Started trend_tags batch: $(date)"
spark-submit $SRC_BATCH/batch_trend_tags.py $POSTS
echo "[X] Finished top_tags batch: $(date)"
