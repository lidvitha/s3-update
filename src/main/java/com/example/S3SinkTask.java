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
        String accessKeyId = props.get(S3SinkConfig.AWS_ACCESS_KEY_ID);
        String secretAccessKey = props.get(S3SinkConfig.AWS_SECRET_ACCESS_KEY);
        bucketName = props.get(S3SinkConfig.S3_BUCKET_NAME);

        BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKeyId, secretAccessKey);
        s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .build();

        recordsBuffer = new ArrayList<>();
        lastFlushTime = System.currentTimeMillis();
        batchSize = Integer.parseInt(props.get("s3.batch.size"));
        batchTimeMs = Long.parseLong(props.get("s3.batch.time.ms"));
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
                //File existing
                if (s3Client.doesObjectExist(bucketName, currentFileKey)) {
                    S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, currentFileKey));
                    BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        fileContent.append(line).append("\n");
                    }
                }

                // Appending content
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