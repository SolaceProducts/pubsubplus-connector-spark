package com.solace.connector.spark.streaming;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.solace.connector.spark.SolaceRecord;
import com.solace.connector.spark.streaming.solace.AppSingleton;
import com.solace.connector.spark.streaming.solace.EventListener;
import com.solace.connector.spark.streaming.solace.InitBroker;
import com.solace.connector.spark.streaming.solace.SolaceMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.apache.spark.sql.connector.read.streaming.SupportsAdmissionControl;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.App;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SolaceMicroBatch implements MicroBatchStream, SupportsAdmissionControl {

    private static Logger log = LoggerFactory.getLogger(SolaceMicroBatch.class);
    int latestOffsetValue = 0;
    int batchSize = 1;
//    int numOfPartitions = 0;

    EventListener eventListener;

    JsonObject offsetJson = new JsonObject();

    InitBroker initBroker;

    SolaceInputPartition[] inputPartitions;

    AppSingleton appSingleton;

    boolean isCommitTriggered = false;

    boolean ackLastProcessedMessages = false;

    List<SolaceMessage> recordsSentForProcessing = new ArrayList<>();

    public SolaceMicroBatch(StructType schema, Map<String, String> properties, CaseInsensitiveStringMap options) {
        appSingleton = AppSingleton.getInstance();
        eventListener = new EventListener();
        appSingleton.setCallback(eventListener);
        eventListener.setAppSingleton(appSingleton);
        if(!properties.containsKey("host") || properties.get("host") == null || properties.get("host").isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace Host name in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace Host name in configuration options");
        }
        if(!properties.containsKey("vpn") || properties.get("vpn") == null || properties.get("vpn").isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace VPN name in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace VPN name in configuration options");
        }

        if(!properties.containsKey("username") || properties.get("username") == null || properties.get("username").isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace Username in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace Username in configuration options");
        }

        if(!properties.containsKey("password") || properties.get("password") == null || properties.get("password").isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace Password in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace Password in configuration options");
        }

        if(!properties.containsKey("queue") || properties.get("queue") == null || properties.get("queue").isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace Queue name in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace Queue in configuration options");
        }

        if(!properties.containsKey("batchSize") || properties.get("batchSize") == null || properties.get("batchSize").isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Batch size in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Batch Size in configuration options");
        }

        if(Integer.parseInt(properties.get("batchSize").toString()) <= 0) {
            log.error("SolaceSparkConnector - Please set Batch size to minimum of 1");
            throw new RuntimeException("SolaceSparkConnector - Please set Batch size to minimum of 1");
        }

        ackLastProcessedMessages = properties.containsKey("ackLastProcessedMessages") ? Boolean.valueOf(properties.get("ackLastProcessedMessages").toString()) : false;
        log.info("SolaceSparkConnector - Ack Last processed messages is set to " + ackLastProcessedMessages);
        log.info("SolaceSparkConnector - Solace Connection Details Host : " + properties.get("host") + ", VPN : " + properties.get("vpn") + ", Username : " + properties.get("username"));
        initBroker = new InitBroker(properties.get("host"), properties.get("vpn"), properties.get("username"), properties.get("password"), properties.get("queue"));
        initBroker.setReceiver(eventListener);
        log.info("SolaceSparkConnector - Acquired connection to Solace broker");

        batchSize = Integer.parseInt(properties.get("batchSize").toString());
        log.info("SolaceSparkConnector - Batch Size is set to " + batchSize);

//        numOfPartitions = Integer.parseInt(properties.get("partitions").toString());
//        log.info("SolaceSparkConnector - Number of partitions is set to " + numOfPartitions);

        log.info("SolaceSparkConnector - Initialization Completed");
    }

    @Override
    public Offset latestOffset() {
        latestOffsetValue+=batchSize;
        log.info("SolaceSparkConnector - latestOffset :: (key,value) - (" + latestOffsetValue + "," + String.join(",", this.appSingleton.processedMessageIDs) + ")");
        return new BasicOffset(latestOffsetValue, String.join(",", this.appSingleton.processedMessageIDs));
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        int size = 1;
        SolaceInputPartition[] partitions = new SolaceInputPartition[size];
        List<SolaceRecord> recordList = new ArrayList<>();

        if(isCommitTriggered || inputPartitions == null) {
            log.info("SolaceSparkConnector - Creating new records list");
            recordList = new ArrayList<>();
            for (int j = 0; j < batchSize && j < this.appSingleton.messageMap.keySet().size(); j++) {
                Object key = this.appSingleton.messageMap.keySet().stream().toArray()[j];
                BytesXMLMessage bytesXMLMessage = this.appSingleton.messageMap.get(key).bytesXMLMessage;
                SolaceRecord solaceRecord = null;
                try {
                    solaceRecord = SolaceRecord.getMapper().map(bytesXMLMessage);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if (solaceRecord != null) {
                    if(ackLastProcessedMessages) {
                        if (offsetJson != null && offsetJson.has("messageIDs")) {
                            List<String> messageIDsInLastOffset = Arrays.asList(offsetJson.get("messageIDs").getAsString().split(","));
                            if (messageIDsInLastOffset.contains(solaceRecord.getMessageId())) {
                                if (this.appSingleton.messageMap.containsKey(solaceRecord.getMessageId())) {
                                    log.info("SolaceSparkConnector - Previous offset " + offsetJson.toString());
                                    log.info("SolaceSparkConnector - Acknowledging previously processed message with ID :: " + solaceRecord.getMessageId());
                                    this.appSingleton.messageMap.get(solaceRecord.getMessageId()).bytesXMLMessage.ackMessage();
                                }

                                if (this.appSingleton.processedMessageIDs.contains(solaceRecord.getMessageId())) {
                                    this.appSingleton.processedMessageIDs.remove(solaceRecord.getMessageId());
                                }
                            } else {
                                this.appSingleton.processedMessageIDs.add(solaceRecord.getMessageId());
                                recordList.add(solaceRecord);
                            }
                        } else {
                            log.info("SolaceSparkConnector - Trying to check if messages are already processed but offset is not available. Hence reprocessing it.");
                            this.appSingleton.processedMessageIDs.add(solaceRecord.getMessageId());
                            recordList.add(solaceRecord);
                        }
                    } else {
                        this.appSingleton.processedMessageIDs.add(solaceRecord.getMessageId());
                        recordList.add(solaceRecord);
                    }

                }
            }

            isCommitTriggered = false;
        } else if(inputPartitions != null && inputPartitions.length > 0 && inputPartitions[0] != null) {
            recordList = inputPartitions[0].getValues();
        }

//        if(numOfPartitions > 0) {
//            size = numOfPartitions;
//            partitions = new SolaceInputPartition[size];
//            for(int i=0; i< numOfPartitions; i++) {
//                partitions[i] = new SolaceInputPartition(i, "", recordList);
//            }
//        } else {
//            log.info("SolaceSparkConnector - Plan input partitions with records :: " + recordList.size());
//            partitions[0] = new SolaceInputPartition(0, "", recordList);
//        }

        log.info("SolaceSparkConnector - Plan input partitions with records :: " + recordList.size());
        partitions[0] = new SolaceInputPartition(0, "", recordList);

        inputPartitions = partitions;
        return partitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
//        this.appSingleton.processedMessageIDs = new ArrayList<>();
        log.info("SolaceSparkConnector - Create reader factory with batchSize :: " + batchSize + " and offsets :: " + offsetJson.toString());
        return new SolaceDataSourceReaderFactory(batchSize, offsetJson.toString());
    }

    @Override
    public Offset latestOffset(Offset startOffset, ReadLimit limit) {
        latestOffsetValue+=batchSize;
        log.info("SolaceSparkConnector - latestOffset with params :: (key,value) - (" + latestOffsetValue + "," + String.join(",", this.appSingleton.processedMessageIDs) + ")");
        return new BasicOffset(latestOffsetValue, String.join(",", this.appSingleton.processedMessageIDs));
    }

    @Override
    public Offset initialOffset() {
        return new BasicOffset(latestOffsetValue, String.join(",", this.appSingleton.processedMessageIDs));
    }

    @Override
    public Offset deserializeOffset(String json) {
        JsonObject gson = new Gson().fromJson(json, JsonObject.class);
        if(gson != null && gson.has("offset")) {
            latestOffsetValue = gson.get("offset").getAsInt();
            offsetJson = gson;
        }
        return new BasicOffset(latestOffsetValue, String.join(",", this.appSingleton.processedMessageIDs));
    }

    @Override
    public void commit(Offset end) {
        log.info("SolaceSparkConnector - Commit triggered");
        BasicOffset basicOffset = (BasicOffset) end;

        log.info("SolaceSparkConnector - Processed message ID's by Spark " + basicOffset.json());

        if(basicOffset != null && basicOffset.messageIDs != null && basicOffset.messageIDs.length() > 0) {
            String[] messageIDs = basicOffset.messageIDs.split(",");
            for(String messageID: messageIDs) {
                if(this.appSingleton.messageMap.containsKey(messageID)) {
                    this.appSingleton.messageMap.get(messageID).bytesXMLMessage.ackMessage();
                    this.appSingleton.messageMap.remove(messageID);
                    if(this.appSingleton.processedMessageIDs.contains(messageID)) {
                        this.appSingleton.processedMessageIDs.remove(messageID);
                    }
                }
            }
        }
//        this.appSingleton.processedMessageIDs = new ArrayList<>();
        isCommitTriggered = true;
    }

    @Override
    public void stop() {
        log.info("SolaceSparkConnector - Closing connection to Solace");
        if(initBroker != null) {
            initBroker.close();
        }
    }

}
