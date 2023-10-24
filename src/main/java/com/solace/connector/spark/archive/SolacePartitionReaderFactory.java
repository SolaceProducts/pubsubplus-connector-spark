package com.solace.connector.spark.archive;

import com.solace.connector.spark.SolaceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

import java.io.Serializable;
import java.util.List;

public class SolacePartitionReaderFactory implements PartitionReaderFactory, Serializable {

    private static final Logger log = LoggerFactory.getLogger(SolacePartitionReaderFactory.class);
    private boolean isRestarted = false;
    public SolacePartitionReaderFactory(boolean isRestarted) {
        log.info("SolaceSparkConnector - Initializing Partition reader factory");
        this.isRestarted = isRestarted;
    }
//    public SolacePartitionReaderFactory(StructType schema, String fileName) {
//        this.schema = schema;
//        this.filePath = fileName;
//    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        List<SolaceRecord> inputPartition = ((SolaceInputPartition) partition).getValues();
        log.info("SolaceSparkConnector - Creating reader for input partition of size " + inputPartition.size());
        return new SolacePartitionReader(((SolaceInputPartition) partition).getValues(), isRestarted);
    }

//    @Override
//    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
//        return PartitionReaderFactory.super.createColumnarReader(partition);
//    }
//
//    @Override
//    public boolean supportColumnarReads(InputPartition partition) {
//        return PartitionReaderFactory.super.supportColumnarReads(partition);
//    }
}
