package com.example;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

public class S3SinkTask extends SinkTask {
    private AmazonS3 s3Client;
    private String bucketName;
    private List<String> recordsBuffer;
    private long lastFlushTime;
    private int batchSize;
    private long batchTimeMs;
    private String currentFileKey;

    @Override
    public void start(Map<String, String> props) {
        bucketName = props.get(S3SinkConfig.S3_BUCKET_NAME);

        s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.fromName(props.get(S3SinkConfig.S3_REGION)))
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();

        recordsBuffer = new ArrayList<>();
        lastFlushTime = System.currentTimeMillis();
        batchSize = Integer.parseInt(props.get(S3SinkConfig.S3_BATCH_SIZE));
        batchTimeMs = Long.parseLong(props.get(S3SinkConfig.S3_BATCH_TIME_MS));
        currentFileKey = "new-test-topic/current_file.txt";  // Fixed file name for appending text
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        for (SinkRecord record : records) {
            recordsBuffer.add(record.value().toString());
            if (recordsBuffer.size() >= batchSize || (System.currentTimeMillis() - lastFlushTime) >= batchTimeMs) {
                flushRecords();
            }
        }
    }

    private void flushRecords() {
        if (!recordsBuffer.isEmpty()) {
            try {
                StringBuilder fileContent = new StringBuilder();

                // Check if the file already exists in the S3 bucket
                if (s3Client.doesObjectExist(bucketName, currentFileKey)) {
                    S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, currentFileKey));
                    BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        fileContent.append(line).append("\n");
                    }
                }

                // Append new records to the file content
                for (String record : recordsBuffer) {
                    fileContent.append(record).append("\n");
                }

                s3Client.putObject(bucketName, currentFileKey, fileContent.toString());
                recordsBuffer.clear();
                lastFlushTime = System.currentTimeMillis();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void stop() {
        flushRecords();
    }

    @Override
    public String version() {
        return "1.0";
    }
}
