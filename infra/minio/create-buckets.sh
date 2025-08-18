#!/bin/sh
set -e

until mc alias set local http://minio:9000 minio minio123 >/dev/null 2>&1; do
  sleep 1
done

mc mb -p local/raw || true
mc mb -p local/out || true
mc mb -p local/expectations || true

mc anonymous set none local/raw || true
mc anonymous set none local/out || true
mc anonymous set none local/expectations || true

if [ -d "/project_data/raw" ]; then
  mc cp --recursive /project_data/raw/* local/raw/ || true
fi

echo "MinIO buckets ready: raw, out, expectations"