package com.example;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.*;

public class S3SinkTask extends SinkTask {
    private AmazonS3 s3Client;
    private String bucketName;
    private Map<String, List<String>> topicBuffers;
    private Map<String, Long> topicLastFlushTimes;
    private Map<String, String> topicFileKeys;
    private int batchSize;
    private long batchTimeMs;
    private int eventCounter = 0;
    private SchemaRegistryClient schemaRegistryClient;
    private Map<String, Schema> schemas = new HashMap<>();

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

        topicBuffers = new HashMap<>();
        topicLastFlushTimes = new HashMap<>();
        topicFileKeys = new HashMap<>();

        batchSize = Integer.parseInt(props.get(S3SinkConfig.S3_BATCH_SIZE));
        batchTimeMs = Long.parseLong(props.get(S3SinkConfig.S3_BATCH_TIME_MS));

        String schemaRegistryUrl = props.get(S3SinkConfig.SCHEMA_REGISTRY_URL);
        schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        for (SinkRecord record : records) {
            String topic = record.topic();
            try {
                Schema schema = getSchemaForTopic(topic);
                if (schema != null) {
                    validateAvroPayload(record.value(), schema);
                    topicBuffers.computeIfAbsent(topic, k -> new ArrayList<>()).add(record.value().toString());

                    if (topicBuffers.get(topic).size() >= batchSize || (System.currentTimeMillis() - topicLastFlushTimes.getOrDefault(topic, 0L)) >= batchTimeMs) {
                        flushRecords(topic);
                        // Generate a new file key for the next batch after flushing
                        topicFileKeys.put(topic, generateFileKey());
                    }
                } else {
                    // Handle invalid schema
                    throw new RuntimeException("Schema not found for topic: " + topic);
                }
            } catch (Exception e) {
                // Handle exceptions
                throw new RuntimeException("Error processing record", e);
            }
        }
    }

    private Schema getSchemaForTopic(String topic) throws Exception {
        String subject = topic + "-value";
        SchemaString schemaString = schemaRegistryClient.getLatestSchemaMetadata(subject).getSchema();
        return new Schema.Parser().parse(schemaString.getSchemaString());
    }

    private void validateAvroPayload(Object value, Schema schema) throws Exception {
        byte[] payload = (byte[]) value;
        DatumReader<GenericRecord> reader = new SpecificDatumReader<>(schema);
        DecoderFactory.get().binaryDecoder(payload, null);
        reader.read(null, DecoderFactory.get().binaryDecoder(payload, null));
    }

    private void flushRecords(String topic) {
        if (!topicBuffers.get(topic).isEmpty()) {
            try {
                String key = String.format("%s/%s", topic, topicFileKeys.getOrDefault(topic, generateFileKey()));

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
                for (String record : topicBuffers.get(topic)) {
                    fileContent.append(record).append("\n");
                }

                s3Client.putObject(bucketName, key, fileContent.toString());
                topicBuffers.get(topic).clear();
                topicLastFlushTimes.put(topic, System.currentTimeMillis());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private String generateFileKey() {
        eventCounter++;
        String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        return String.format("event%d-%s.txt", eventCounter, timestamp);
    }

    @Override
    public void stop() {
        // Ensure any remaining records are flushed before stopping
        for (String topic : topicBuffers.keySet()) {
            if (!topicBuffers.get(topic).isEmpty()) {
                flushRecords(topic);
            }
        }
    }

    @Override
    public String version() {
        return "1.0";
    }
}
