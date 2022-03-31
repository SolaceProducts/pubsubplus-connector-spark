package com.solace.connector.spark;

import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

import java.io.Serializable;
import java.util.List;

public class SolacePartitionReaderFactory implements PartitionReaderFactory, Serializable {

    private static final Logger log = Logger.getLogger(SolacePartitionReaderFactory.class);
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
