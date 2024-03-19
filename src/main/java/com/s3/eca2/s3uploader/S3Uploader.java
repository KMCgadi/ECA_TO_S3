package com.s3.eca2.s3uploader;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.io.FileUtils;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Service;
import java.util.logging.Logger;
import java.io.File;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;


@Service
public class S3Uploader {
    private final String bucketName = "gadi-s3-bukit-test";
    private final String region = "ap-northeast-2";
    private final String accessKey = "AKIAZVIU7KG7IXR2HIGT";
    private final String secretKey = "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";
    private final AmazonS3 s3Client;
    private static final Logger logger = Logger.getLogger(S3Uploader.class.getName());

    public S3Uploader() {
        this.s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(region)
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .build();
        System.out.println("AmazonS3 client initialized successfully");
    }

    public void listS3BucketContents() {
        try {
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName);

            ObjectListing objectListing;
            do {
                objectListing = this.s3Client.listObjects(listObjectsRequest);
                for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                    System.out.println(" - " + objectSummary.getKey() + "  (size = " + objectSummary.getSize() + ")");
                }
                listObjectsRequest.setMarker(objectListing.getNextMarker());
            } while (objectListing.isTruncated());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void uploadFileToS3(String filePath, String s3Key) {
        try {
            File file = new File(filePath);
            this.s3Client.putObject(new PutObjectRequest(bucketName, s3Key, file));
            System.out.println("File uploaded successfully to S3 bucket " + bucketName + " as " + s3Key);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String readParquetFromS3(String s3Key) {
        ParquetReader<Group> reader = null;
        File localFile = new File("D:\\Documents\\temp\\" + s3Key.replace("/", "_")); // 슬래시를 밑줄로 변경하여 경로 문제 방지
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
                sb.append(group.toString());
                sb.append("\n");
            }

            return sb.toString();
        } catch (Exception e) {
            logger.info(e.toString());
            return "Failed to download the file from S3: " + e.getMessage();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception e) {
                    logger.info(e.toString());
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


