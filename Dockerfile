FROM python:3.11-slim

WORKDIR /app

# 시스템 패키지 설치 및 정리
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    cron \
    tzdata && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 타임존 설정
ENV TZ=Asia/Seoul

# 필요한 Python 패키지만 설치
RUN pip install --no-install-recommends --no-cache-dir \
    google-api-python-client==2.108.0 \
    google-auth-httplib2==0.1.1 \
    google-auth-oauthlib==1.1.0 \
    pandas==2.1.3 \
    requests==2.31.0

# 로그 디렉토리 설정
RUN mkdir -p /app/logs && \
    touch /app/logs/analytics.log

# cron 작업 설정
RUN echo "0 0 1 * * python /app/cloudflare_analytics.py >> /app/logs/analytics.log 2>&1" > /etc/cron.d/analytics-cron && \
    chmod 0644 /etc/cron.d/analytics-cron && \
    crontab /etc/cron.d/analytics-cron

# 애플리케이션 파일 복사
COPY cloudflare_analytics.py .

# 시작 스크립트 생성
RUN echo '#!/bin/sh\n\
echo "Container started at $(date)"\n\
python /app/cloudflare_analytics.py >> /app/logs/analytics.log 2>&1\n\
echo "Starting cron..."\n\
cron -f' > /app/start.sh && \
    chmod +x /app/start.sh

# 컨테이너 시작 명령
CMD ["/app/start.sh"]