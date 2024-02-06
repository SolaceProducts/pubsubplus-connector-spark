package com.solacecoe.connectors.spark.streaming;

import com.solacecoe.connectors.spark.SolaceRecord;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

public class SolaceDataSourceReaderFactory implements InputPartition<InternalRow>, Serializable {

    private static final Logger log = LoggerFactory.getLogger(SolaceDataSourceReaderFactory.class);
    private boolean includeHeaders;
    private List<SolaceRecord> values;

    SolaceDataSourceReaderFactory(boolean includeHeaders, List<SolaceRecord> values) {
        this.includeHeaders = includeHeaders;
        this.values = values;
        log.info("SolaceSparkConnector - Initializing Partition reader factory");
    }

//    @Override
//    public PartitionReader<InternalRow> createReader(InputPartition partition) {
//        SolaceInputPartition solaceInputPartition = (SolaceInputPartition) partition;
//        log.info("SolaceSparkConnector - Creating reader for input partition reader factory");
//        return new SolaceInputPartitionReader(solaceInputPartition, includeHeaders);
//    }

    public List<SolaceRecord> getValues() {
        return this.values;
    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {
        return new SolaceInputPartitionReader(this.values, this.includeHeaders);
    }
}
