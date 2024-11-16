#!/bin/sh

echo "Container started at $(date)"

# 초기 실행
echo "Running initial analytics collection..."
python /app/cloudflare_analytics.py >> /app/logs/analytics.log 2>&1

# cron 시작
echo "Starting cron daemon..."
cron -f