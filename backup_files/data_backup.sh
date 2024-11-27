#!/bin/bash
# bash 명령어로 실행하기!

# 현재 날짜를 기준으로 덤프 파일 이름 설정
DATE=$(date +%Y%m%d)

# db 유저 및 비밀번호
DB_USER='root'
DB_PASS='1231'

# 덤프할 데이터베이스 & 테이블 목록
DATABASES=('crawling')
TABLES=('incruit' 'jumpit' 'new_wanted' 'saramin')  # 덤프할 테이블 목록

# S3 버킷 이름
S3_BUCKET="@@@@"

# MySQL 서버 포트 번호
DB_PORT=@@@@

# 로그 파일 경로 설정 (하나의 로그 파일)
LOG_FILE="/home/ubuntu/backup_files/backup_log.log"

# 로그 함수 정의
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') : $1" | tee -a "$LOG_FILE"
}

# 백업 시작 로그
log "===== 데이터베이스 백업 시작 ====="

for DB_NAME in "${DATABASES[@]}"
do
    for TABLE_NAME in "${TABLES[@]}"
    do
        log "백업 시작: 데이터베이스 '$DB_NAME' 테이블 '$TABLE_NAME'"

        # 덤프 파일 경로 설정
        DUMP_FILE="/home/ubuntu/backup_files/${DB_NAME}_${TABLE_NAME}_backup_$DATE.sql"

        # MySQL 덤프 생성 (각 테이블별로 덤프)
        if mysqldump -u "$DB_USER" -p"$DB_PASS" -P "$DB_PORT" "$DB_NAME" "$TABLE_NAME" > "$DUMP_FILE" 2>> "$LOG_FILE"; then
            log "덤프 성공: $DUMP_FILE"
        else
            log "덤프 실패: $DB_NAME.$TABLE_NAME"
            continue  # 덤프에 실패하면 다음 테이블로 넘어감
        fi

        # S3에 업로드
        if aws s3 cp "$DUMP_FILE" "s3://$S3_BUCKET/data_backup/" >> "$LOG_FILE" 2>&1; then
            log "S3 업로드 성공: $DUMP_FILE"
        else
            log "S3 업로드 실패: $DUMP_FILE"
            # 필요에 따라 여기서 스크립트를 종료하거나 다른 처리를 할 수 있음
        fi

        # 덤프 파일 제거 (선택사항)
        if rm "$DUMP_FILE" >> "$LOG_FILE" 2>&1; then
            log "덤프 파일 삭제 성공: $DUMP_FILE"
        else
            log "덤프 파일 삭제 실패: $DUMP_FILE"
        fi

        log "백업 완료: 데이터베이스 '$DB_NAME' 테이블 '$TABLE_NAME'"
    done
done

# 백업 종료 로그
log "===== 데이터베이스 백업 종료 ====="
