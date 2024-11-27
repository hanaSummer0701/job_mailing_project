#!/bin/bash

WANTED_LOG_FILE="/home/ubuntu/job_crawling/wanted/crawling_log.log"

# 랜덤 시간 설정
sleep $((RANDOM % 120 * 60))

# Python 스크립트 실행 시간 기록
echo "$(date '+%Y-%m-%d %H:%M:%S') - Executing Python script." >> $WANTED_LOG_FILE

# Python 스크립트 실행
# /home/ubuntu/job_crawling/bin/python3 /home/ubuntu/job_crawling/wanted/wanted_pch.py >> $WANTED_LOG_FILE 2>&1
echo "======▶️======" >> $WANTED_LOG_FILE
/home/ubuntu/job_crawling/bin/python3 /home/ubuntu/job_crawling/wanted/wanted_pch.py >> $WANTED_LOG_FILE 2>&1

# Python 스크립트 실행 완료 기록
echo "$(date '+%Y-%m-%d %H:%M:%S') - Python script execution finished." >> $WANTED_LOG_FILE
