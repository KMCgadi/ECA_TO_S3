데이터셋을 AWS S3에 Parquet 형태로 변환하여 업로드 하기위한 Program.

1. main 메서드에 서버 또는 로컬에 설치된 hadoop 디렉토리 경로 설정필요
2. application.properties에 서버 포트설정 및 DB설정 AWS S3 설정필요
3. 각 도메인의 스키마는 원하는 VO 형태로 커스터마이징
4. hadoop 3.3.6 다운로드 URL (https://hadoop.apache.org/release/3.3.6.html)
5. application.properties 예시

# Server Config
server.port=8085
server.servlet.encoding.charset=UTF-8
server.servlet.encoding.force=true
server.address=127.0.0.1
# DB
spring.datasource.url=jdbc:oracle:thin:@//172.16.11.75:1521/ecadb
spring.datasource.username= your_name
spring.datasource.password= your_password
# JPA
spring.jpa.hibernate.ddl-auto=validate
spring.jpa.show-sql=true
## AWS S3
#aws.s3.bucketName=gadi-s3-bukit-test
#aws.s3.region=ap-northeast-2
#aws.accessKey= your_access_key
#aws.secretKey= your_secret_key