#!/bin/bash

INCRUIT_LOG_FILE="/home/ubuntu/job_crawling/incruit/log/app.log"

# 랜덤 시간 설정
sleep $((RANDOM % 120 * 60))
# sleep $((RANDOM % 200))
# Python 스크립트 실행 시간 기록
echo "$(date '+%Y-%m-%d %H:%M:%S') - Executing Python script." >> $INCRUIT_LOG_FILE

# Python 스크립트 실행
echo "======▶️======" >> $INCRUIT_LOG_FILE
/home/ubuntu/job_crawling/bin/python3 /home/ubuntu/job_crawling/incruit/incruit.py >> $INCRUIT_LOG_FILE 2>&1

# Python 스크립트 실행 완료 기록
echo "$(date '+%Y-%m-%d %H:%M:%S') - Python script execution finished." >> $INCRUIT_LOG_FILE
