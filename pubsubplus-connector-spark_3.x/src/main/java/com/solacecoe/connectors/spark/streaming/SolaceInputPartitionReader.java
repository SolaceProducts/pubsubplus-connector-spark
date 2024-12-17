package com.solacecoe.connectors.spark.streaming;

import com.solacecoe.connectors.spark.SolaceRecord;
import com.solacecoe.connectors.spark.streaming.properties.SolaceHeaders;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class SolaceInputPartitionReader implements PartitionReader<InternalRow>, Serializable {
    private static Logger log = LoggerFactory.getLogger(SolaceInputPartitionReader.class);
    private int index = 0;
    private SolaceRecord solaceRecord = null;
    private final boolean includeHeaders;
    private final SolaceInputPartition solaceInputPartition;

    public SolaceInputPartitionReader(SolaceInputPartition inputPartition, boolean includeHeaders) {
        log.info("SolaceSparkConnector - Initializing Solace Input Partition reader");
        this.includeHeaders = includeHeaders;
        this.solaceInputPartition = inputPartition;
    }

    private boolean checkForDataInInputPartition() {
        if(!this.solaceInputPartition.getValues().isEmpty() && index < this.solaceInputPartition.getValues().size()) {
            solaceRecord = this.solaceInputPartition.getValues().get(index);
            log.info("SolaceSparkConnector - Next message is available");
            log.info("SolaceSparkConnector - Current processing index " + index);
            log.info("SolaceSparkConnector - Total messages in InputPartition " + this.solaceInputPartition.getValues().size());
            return true;
        }

        log.info("SolaceSparkConnector - Next message is not available");
        log.info("SolaceSparkConnector - Current processing index " + index);
        log.info("SolaceSparkConnector - Total messages in InputPartition " + this.solaceInputPartition.getValues().size());
        return false;
    }

    @Override
    public boolean next() {
        return checkForDataInInputPartition();
    }

    @Override
    public InternalRow get() {
        Long timestamp = solaceRecord.getSenderTimestamp();
        if (solaceRecord.getSenderTimestamp() == 0) {
            timestamp = System.currentTimeMillis();
        }
        InternalRow row;
        if(this.includeHeaders) {
            log.info("SolaceSparkConnector - Adding event headers to Spark row");
            Map<String, Object> headers = new HashMap<>();
            Map<String, Object> userProperties = (solaceRecord.getProperties() != null) ? solaceRecord.getProperties() : new HashMap<>();
            if(!userProperties.isEmpty()) {
                headers.putAll(userProperties);
            }
            if(solaceRecord.getSequenceNumber() != null) {
                headers.put(SolaceHeaders.SEQUENCE_NUMBER, solaceRecord.getSequenceNumber());
            }
            headers.put(SolaceHeaders.EXPIRATION, solaceRecord.getExpiration());
            headers.put(SolaceHeaders.TIME_TO_LIVE, solaceRecord.getTimeToLive());
            headers.put(SolaceHeaders.PRIORITY, solaceRecord.getPriority());
            headers.put(SolaceHeaders.REDELIVERED, solaceRecord.isRedelivered());
            MapData mapData = new ArrayBasedMapData(new GenericArrayData(headers.keySet().stream().map(key -> UTF8String.fromString(key)).toArray()), new GenericArrayData(headers.values().stream().map(value -> value.toString().getBytes(StandardCharsets.UTF_8)).toArray()));
            row = new GenericInternalRow(new Object[]{UTF8String.fromString(solaceRecord.getMessageId()),
                    solaceRecord.getPayload(), UTF8String.fromString(solaceRecord.getPartitionKey()), UTF8String.fromString(solaceRecord.getDestination()),
                    DateTimeUtils.fromJavaTimestamp(new Timestamp(timestamp)),mapData
            });
//            row = InternalRow.apply(CollectionConverters.asScala(Arrays.asList(
//                    new Object[]{UTF8String.fromString(solaceRecord.getMessageId()),
//                            solaceRecord.getPayload(), UTF8String.fromString(solaceRecord.getPartitionKey()), UTF8String.fromString(solaceRecord.getDestination()),
//                            DateTimeUtils.fromJavaTimestamp(new Timestamp(timestamp)),mapData
//                    })).coll().toSeq());
        } else {
            row = new GenericInternalRow(new Object[]{UTF8String.fromString(solaceRecord.getMessageId()),
                    solaceRecord.getPayload(), UTF8String.fromString(solaceRecord.getPartitionKey()), UTF8String.fromString(solaceRecord.getDestination()),
                    DateTimeUtils.fromJavaTimestamp(new Timestamp(timestamp))
            });
//            row = InternalRow.apply(CollectionConverters.asScala(Arrays.asList(
//                    new Object[]{UTF8String.fromString(solaceRecord.getMessageId()),
//                            solaceRecord.getPayload(), UTF8String.fromString(solaceRecord.getPartitionKey()), UTF8String.fromString(solaceRecord.getDestination()),
//                            DateTimeUtils.fromJavaTimestamp(new Timestamp(timestamp))
//                    })).coll().toSeq());
        }

        index++;
        log.info("SolaceSparkConnector - Created Spark row for message with ID " + solaceRecord.getMessageId());
        return row;
    }

    @Override
    public void close() {
        log.info("Input partition with ID " + this.solaceInputPartition.getId() + " is closed");
    }
}
