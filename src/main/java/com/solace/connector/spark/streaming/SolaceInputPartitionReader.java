package com.solace.connector.spark.streaming;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.solace.connector.spark.SolaceRecord;
import com.solace.connector.spark.streaming.solace.AppSingleton;
import com.solace.connector.spark.streaming.solace.SolaceMessage;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.JavaConversions;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolaceInputPartitionReader implements PartitionReader<InternalRow>, Serializable {

    private static Logger log = LoggerFactory.getLogger(SolaceInputPartitionReader.class);

    SolaceInputPartition solaceInputPartition;

    int batchSize = 0;
//    private List<String> previousMessageIDs = new ArrayList<>();
    int index = 0;

    SolaceRecord solaceRecord = null;

    public SolaceInputPartitionReader(SolaceInputPartition inputPartition, int batchSize, String offsetJson) {
        log.info("SolaceSparkConnector - Initializing Solace Input Partition reader");
        this.solaceInputPartition = inputPartition;
        this.batchSize = batchSize;
//        JsonObject offset = new Gson().fromJson(offsetJson, JsonObject.class);
//        if(offset.has("messageIDs")) {
//            previousMessageIDs = Arrays.asList(offset.get("messageIDs").getAsString().split(","));
//        }
//        AppSingleton.processedMessageIDs = new ArrayList<>();
    }

    @Override
    public boolean next() {
        if(!this.solaceInputPartition.getValues().isEmpty() && index < this.solaceInputPartition.getValues().size()) {
            solaceRecord = this.solaceInputPartition.getValues().get(index);
//            if(solaceTextRecord != null && previousMessageIDs.contains(solaceTextRecord.getMessageId()) && this.appSingleton.messageMap.containsKey(solaceTextRecord.getMessageId())) {
//                this.appSingleton.messageMap.get(solaceTextRecord.getMessageId()).bytesXMLMessage.ackMessage();
//                this.appSingleton.messageMap.remove(solaceTextRecord.getMessageId());
//                log.info("SolaceSparkConnector - Received previously processed message. Acknowledging message with ID " + solaceTextRecord.getMessageId());
//                return false;
//            } else {
                log.info("SolaceSparkConnector - Next message is available");
                log.info("SolaceSparkConnector - Current processing index " + index);
                log.info("SolaceSparkConnector - Current received messages from Solace " + this.solaceInputPartition.getValues().size());
                return true;
//            }
        }

        log.info("SolaceSparkConnector - Next message is not available");
        log.info("SolaceSparkConnector - Current received messages from Solace " + this.solaceInputPartition.getValues().size());
        log.info("SolaceSparkConnector - Current processing index " + index);
        return false;
    }

    @Override
    public InternalRow get() {
        Long timestamp = solaceRecord.getSenderTimestamp();
        if (solaceRecord.getSenderTimestamp() == 0) {
            timestamp = System.currentTimeMillis();
        }
        InternalRow row = InternalRow.apply(JavaConversions.asScalaBuffer(Arrays.asList(
                new Object[]{UTF8String.fromString(solaceRecord.getMessageId().toString()),
                        solaceRecord.getPayload(), UTF8String.fromString(solaceRecord.getDestination()),
                        DateTimeUtils.fromJavaTimestamp(new Timestamp(timestamp))
                })).seq());
//        this.appSingleton.processedMessageIDs.add(solaceTextRecord.getMessageId());
//        log.info("SolaceSparkConnector - Count of processed messages :: " + this.appSingleton.processedMessageIDs.size());
        index++;

        log.info("SolaceSparkConnector - Created Spark row for message with ID " + solaceRecord.getMessageId());
        return row;
    }

    @Override
    public void close() throws IOException {

    }
}
