package com.example;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
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
        String accessKeyId = props.get(S3SinkConfig.AWS_ACCESS_KEY_ID);
        String secretAccessKey = props.get(S3SinkConfig.AWS_SECRET_ACCESS_KEY);
        bucketName = props.get(S3SinkConfig.S3_BUCKET_NAME);

        BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKeyId, secretAccessKey);
        s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.fromName(props.get(S3SinkConfig.S3_REGION)))
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .build();

        recordsBuffer = new ArrayList<>();
        lastFlushTime = System.currentTimeMillis();
        batchSize = Integer.parseInt(props.get(S3SinkConfig.S3_BATCH_SIZE));
        batchTimeMs = Long.parseLong(props.get(S3SinkConfig.S3_BATCH_TIME_MS));
        currentFileKey = generateFileKey("default-topic");
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        for (SinkRecord record : records) {
            recordsBuffer.add(record.value().toString());
            if (recordsBuffer.size() >= batchSize || (System.currentTimeMillis() - lastFlushTime) >= batchTimeMs) {
                flushRecords(record.topic());
                // Generate a new file key for the next batch after flushing
                currentFileKey = generateFileKey(record.topic());
            }
        }
    }

    private void flushRecords(String topic) {
        if (!recordsBuffer.isEmpty()) {
            try {
                String key = String.format("%s/event/%s", topic, currentFileKey);

                StringBuilder fileContent = new StringBuilder();

                // Check if the file already exists in the S3 bucket and append existing content
                if (s3Client.doesObjectExist(bucketName, key)) {
                    S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, key));
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

                s3Client.putObject(bucketName, key, fileContent.toString());
                recordsBuffer.clear();
                lastFlushTime = System.currentTimeMillis();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private String generateFileKey(String topic) {
        String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        return String.format("%s-%s-events-%s.txt", bucketName, topic, timestamp);
    }

    @Override
    public void stop() {
        // Ensure any remaining records are flushed before stopping
        if (!recordsBuffer.isEmpty()) {
            flushRecords("default-topic");
        }
    }

    @Override
    public String version() {
        return "1.0";
    }
}
