#!/bin/bash

BACKUP_LOG_FILE="/home/ubuntu/backup_files/backup_log.log"

# Python 스크립트 실행 시간 기록
echo "$(date '+%Y-%m-%d %H:%M:%S') - Executing Python script." >> $BACKUP_LOG_FILE

# 코드 백업 실행
echo "======▶️======" >> $BACKUP_LOG_FILE
/home/ubuntu/job_crawling/bin/python3 /home/ubuntu/backup_files/jobpost_pickle.py >> $BACKUP_LOG_FILE 2>&1
# 데이터 백업 실행
/home/ubuntu/backup_files/data_backup.sh >> $BACKUP_LOG_FILE 2>&1

# Python 스크립트 실행 완료 기록
echo "$(date '+%Y-%m-%d %H:%M:%S') - Python script execution finished." >> $BACKUP_LOG_FILE
