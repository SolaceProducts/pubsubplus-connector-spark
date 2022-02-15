package com.solace.connector.spark;

//import connector.csv.ValueConverters;
import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.connector.read.PartitionReader;
        import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.JavaConversions;

        import java.io.IOException;
        import java.sql.Timestamp;
import java.util.Arrays;
        import java.util.List;

public class SolacePartitionReader implements PartitionReader<InternalRow> {

    private static final Logger log = Logger.getLogger(SolacePartitionReader.class);
    private List<SolaceRecord> payload;
    private int records = 0;
    SolacePartitionReader(List<SolaceRecord> input) {
        log.info("SolaceSparkConnector - Initializing partition reader for records of size " + input.size());
        this.payload = input;
    }

    @Override
    public boolean next() {
        log.info("SolaceSparkConnector - Checking for next available record. Is record available: " + (records < payload.size()));
        return records < payload.size();
    }

    @Override
    public InternalRow get() {
        SolaceRecord solaceTextRecord = payload.get(records++);
//        SolaceTextRecord solaceTextRecord = null;
////        try {
//            solaceTextRecord = SolaceTextRecord.getMapper().map(msg.message);
//        } catch (Exception e) {
//            //log.error("SolaceSparkConnector - Error converting message to Solace Text Record " + e.getMessage());
//            System.exit(0);
//        }
        log.info("SolaceSparkConnector - Creating internal row for solace record " + solaceTextRecord.getMessageId());
        Long timestamp = solaceTextRecord.getSenderTimestamp();
        if(solaceTextRecord.getSenderTimestamp() == 0) {
            timestamp = System.currentTimeMillis();
        }
        InternalRow row = InternalRow.apply(JavaConversions.asScalaBuffer(Arrays.asList(
                new Object[]{UTF8String.fromString(Long.toString(solaceTextRecord.getMessageId())),
                        solaceTextRecord.getPayload(), UTF8String.fromString(solaceTextRecord.getDestination()),
                        DateTimeUtils.fromJavaTimestamp(new Timestamp(timestamp))})).seq());
        log.info("SolaceSparkConnector - Internal Row Created: " + row.getString(0));
        return row;
    }

    @Override
    public void close() throws IOException {
    }
}
