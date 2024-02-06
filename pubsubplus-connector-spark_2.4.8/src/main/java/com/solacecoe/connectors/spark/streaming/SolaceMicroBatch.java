package com.solacecoe.connectors.spark.streaming;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.solacecoe.connectors.spark.SolaceRecord;
import com.solacecoe.connectors.spark.streaming.solace.AppSingleton;
import com.solacecoe.connectors.spark.streaming.solace.EventListener;
import com.solacecoe.connectors.spark.streaming.solace.InitBroker;
import com.solacesystems.jcsmp.BytesXMLMessage;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class SolaceMicroBatch implements MicroBatchReader, Serializable {
    private static Logger log = LoggerFactory.getLogger(SolaceMicroBatch.class);
    int latestOffsetValue = 0;
    int batchSize = 1;
    boolean isCommitTriggered = false;
    boolean ackLastProcessedMessages = false;
    boolean skipMessageReprocessingIfTasksAreRunningLate = false;
    boolean includeHeaders = false;
    EventListener eventListener;
    JsonObject offsetJson = new JsonObject();
    InitBroker initBroker;
    List<SolaceDataSourceReaderFactory> inputPartitions = new ArrayList<>();
    AppSingleton appSingleton;

    public SolaceMicroBatch(DataSourceOptions options) {
        log.info("SolaceSparkConnector - Initializing Solace Spark Connector");
        // Initialize classes required for Solace connectivity
        appSingleton = AppSingleton.getInstance();
        appSingleton.solaceOffsetIndicator = options.get("offsetIndicator").orElse("MESSAGE_ID");

        eventListener = new EventListener();
        appSingleton.setCallback(eventListener);
        eventListener.setAppSingleton(appSingleton);

        // User configuration validation
        if(options.get("host") == null || options.get("host").get().isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace Host name in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace Host name in configuration options");
        }
        if(options.get("vpn") == null || options.get("vpn").get().isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace VPN name in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace VPN name in configuration options");
        }

        if(options.get("username") == null || options.get("username").get().isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace Username in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace Username in configuration options");
        }

        if(options.get("password") == null || options.get("password").get().isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace Password in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace Password in configuration options");
        }

        if(options.get("queue") == null || options.get("queue").get().isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace Queue name in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace Queue in configuration options");
        }

        if(options.getInt("batchSize", 1) <= 0) {
            log.error("SolaceSparkConnector - Please set Batch size to minimum of 1");
            throw new RuntimeException("SolaceSparkConnector - Please set Batch size to minimum of 1");
        }

        ackLastProcessedMessages = options.getBoolean("ackLastProcessedMessages", false);
        skipMessageReprocessingIfTasksAreRunningLate = options.getBoolean("skipDuplicates", false);
        log.info("SolaceSparkConnector - Ack Last processed messages is set to " + ackLastProcessedMessages);
        includeHeaders = options.getBoolean("includeHeaders", false);
        log.info("SolaceSparkConnector - includeHeaders is set to " + includeHeaders);
        batchSize = options.getInt("batchSize", 1);
        log.info("SolaceSparkConnector - Batch Size is set to " + batchSize);

        // Initialize connection to Solace Broker
        log.info("SolaceSparkConnector - Solace Connection Details Host : " + options.get("host").get() + ", VPN : " + options.get("vpn").get() + ", Username : " + options.get("username").get());
        initBroker = new InitBroker(options.get("host").get(), options.get("vpn").get(), options.get("username").get(), options.get("password").get(), options.get("queue").get());
        initBroker.setReceiver(eventListener);
        log.info("SolaceSparkConnector - Acquired connection to Solace broker");

        log.info("SolaceSparkConnector - Initialization Completed");
    }

//    @Override
//    public Offset latestOffset() {
//        latestOffsetValue+=batchSize;
//        log.info("SolaceSparkConnector - latestOffset :: (key,value) - (" + latestOffsetValue + "," + String.join(",", this.appSingleton.processedMessageIDs) + ")");
//        return new BasicOffset(latestOffsetValue, String.join(",", this.appSingleton.processedMessageIDs));
//    }

    private boolean shouldAddMessage(String messageID) {
        if(skipMessageReprocessingIfTasksAreRunningLate && this.appSingleton.processedMessageIDs.contains(messageID)) {
            return false;
        }

        return true;
    }

    @Override
    public StructType readSchema() {
        if(includeHeaders) {
            StructField[] structFields = new StructField[]{
                    new StructField("Id", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("Payload", DataTypes.BinaryType, true, Metadata.empty()),
                    new StructField("Topic", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("TimeStamp", DataTypes.TimestampType, true, Metadata.empty()),
                    new StructField("Headers", new MapType(DataTypes.StringType, DataTypes.BinaryType, false), true, Metadata.empty())
            };
            return new StructType(structFields);
        }

        StructField[] structFields = new StructField[]{
                new StructField("Id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Payload", DataTypes.BinaryType, true, Metadata.empty()),
                new StructField("Topic", DataTypes.StringType, true, Metadata.empty()),
                new StructField("TimeStamp", DataTypes.TimestampType, true, Metadata.empty())
        };
        return new StructType(structFields);
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
        int size = 1;
        List<InputPartition<InternalRow>> partitions = new ArrayList<>();
        List<SolaceRecord> recordList = new ArrayList<>();

        if(isCommitTriggered || inputPartitions == null) {
            log.info("SolaceSparkConnector - Creating new records list");
            recordList = new ArrayList<>();
            for (int j = 0; j < batchSize && j < this.appSingleton.messageMap.keySet().size(); j++) {
                Object key = this.appSingleton.messageMap.keySet().stream().toArray()[j];
                BytesXMLMessage bytesXMLMessage = this.appSingleton.messageMap.get(key).bytesXMLMessage;
                SolaceRecord solaceRecord = null;
                try {
                    solaceRecord = SolaceRecord.getMapper(this.appSingleton.solaceOffsetIndicator).map(bytesXMLMessage);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if (solaceRecord != null && shouldAddMessage(solaceRecord.getMessageId())) {
                    if(ackLastProcessedMessages) {
                        // based on last successful offset, extract the message ID and see if same message is received, if so ack the message
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
        } else if(inputPartitions != null && inputPartitions.size() > 0 && inputPartitions.get(0) != null) {
            recordList = inputPartitions.get(0).getValues();
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
        partitions.add(new SolaceDataSourceReaderFactory(includeHeaders, recordList));

        inputPartitions = Collections.singletonList(new SolaceDataSourceReaderFactory(includeHeaders, recordList));
        return partitions;
    }

//    @Override
//    public PartitionReaderFactory createReaderFactory() {
//        log.info("SolaceSparkConnector - Create reader factory with batchSize :: " + batchSize + " and offsets :: " + offsetJson.toString());
//        return new SolaceDataSourceReaderFactory(includeHeaders);
//    }

    @Override
    public Offset getEndOffset() {
        latestOffsetValue+=batchSize;
        log.info("SolaceSparkConnector - latestOffset with params :: (key,value) - (" + latestOffsetValue + "," + String.join(",", this.appSingleton.processedMessageIDs) + ")");
        return new BasicOffset(latestOffsetValue, String.join(",", this.appSingleton.processedMessageIDs));
    }

    @Override
    public void setOffsetRange(Optional<Offset> optional, Optional<Offset> optional1) {

    }

    @Override
    public Offset getStartOffset() {
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
                    log.info("SolaceSparkConnector - Acknowledged message with ID :: " + messageID);
                    this.appSingleton.messageMap.remove(messageID);
                    if(this.appSingleton.processedMessageIDs.contains(messageID)) {
                        this.appSingleton.processedMessageIDs.remove(messageID);
                    }
                }
            }
        }

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
