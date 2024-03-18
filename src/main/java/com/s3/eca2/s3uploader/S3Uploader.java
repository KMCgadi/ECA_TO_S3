package com.s3.eca2.s3uploader;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.springframework.stereotype.Service;

import java.io.File;

@Service
public class S3Uploader {
    private final String bucketName = "gadi-s3-bukit-test";
    private final String region = "ap-northeast-2";
    private final String accessKey = "AKIAZVIU7KG7IXR2HIGT";
    private final String secretKey = "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";

    public void listS3BucketContents() {
        try {
            System.out.println("Initializing AWS Credentials");
            BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);

            System.out.println("Setting AWS region: " + region);
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(region)
                    .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                    .build();

            System.out.println("AmazonS3 client initialized successfully");

            ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName);
            System.out.println("listObjectsRequest initialized");

            ObjectListing objectListing;
            do {
                objectListing = s3Client.listObjects(listObjectsRequest);
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
            BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(region)
                    .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                    .build();

            File file = new File(filePath);
            s3Client.putObject(new PutObjectRequest(bucketName, s3Key, file));
            System.out.println("File uploaded successfully to S3 bucket " + bucketName + " as " + s3Key);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
