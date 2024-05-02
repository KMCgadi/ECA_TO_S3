package com.s3.eca2.api.s3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.nio.file.Paths;


@Service
public class S3Service {
    private final String bucketName;
    private final String region;
    private final AmazonS3 s3Client;
    private static final Logger logger = LoggerFactory.getLogger(S3Service.class);

    public S3Service(@Value("${aws.s3.bucketName}") String bucketName,
                     @Value("${aws.s3.region}") String region,
                     @Value("${aws.accessKey}") String accessKey,
                     @Value("${aws.secretKey}") String secretKey) {
        this.bucketName = bucketName;
        this.region = region;
        this.s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(region)
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .build();
    }

    public void uploadFileToS3(String filePath, String s3Key) {
        try {
            File file = new File(filePath);
            this.s3Client.putObject(new PutObjectRequest(bucketName, s3Key, file));
            logger.info("File uploaded successfully to S3 bucket " + bucketName + " as " + s3Key);

            if (!file.delete())
                logger.error("Failed to delete local file: " + filePath);
        } catch (Exception e) {
            logger.error("Failed to upload file to S3", e);
        }
    }

    public String readParquetFromS3(String s3Key) {
        ParquetReader<Group> reader = null;
        String fileName = Paths.get(s3Key).getFileName().toString().replace("/", "_");
        String tempDirPath = Paths.get(System.getProperty("user.dir"), "temp").toString();
        File localFile = new File(tempDirPath, fileName);

        try {
            // S3에서 Parquet 파일을 로컬 시스템으로 다운로드
            S3Object s3Object = this.s3Client.getObject(new GetObjectRequest(bucketName, s3Key));
            FileUtils.copyInputStreamToFile(s3Object.getObjectContent(), localFile);

            // Parquet 파일 읽기
            Path path = new Path(localFile.getAbsolutePath());
            reader = ParquetReader.builder(new GroupReadSupport(), path).withConf(new Configuration()).build();

            Group group;
            StringBuilder sb = new StringBuilder();
            while ((group = reader.read()) != null) {
                logger.info(group.toString());
                sb.append(group);
                sb.append("\n");
            }

            return sb.toString();
        } catch (Exception e) {
            logger.error(String.valueOf(e));
            return "Failed to download the file from S3: " + e.getMessage();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception e) {
                    logger.error(String.valueOf(e));
                }
            }
            if (localFile.exists()) {
                boolean deleted = localFile.delete();
                if (!deleted) {
                    logger.info(localFile.getAbsolutePath());
                }
            }
        }
    }

}


