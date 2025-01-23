package com.solacecoe.connectors.spark.streaming.offset;

import org.apache.spark.sql.execution.streaming.MetadataLog;
import scala.Option;
import scala.Tuple2;

public class SolaceSourceOffsetWriter implements MetadataLog<SolaceSourceOffset> {

    @Override
    public boolean add(long batchId, SolaceSourceOffset metadata) {
        return false;
    }

    @Override
    public Option<SolaceSourceOffset> get(long batchId) {
        return null;
    }

    @Override
    public Tuple2<Object, SolaceSourceOffset>[] get(Option<Object> startId, Option<Object> endId) {
        return new Tuple2[0];
    }

    @Override
    public Option<Tuple2<Object, SolaceSourceOffset>> getLatest() {
        return null;
    }

    @Override
    public void purge(long thresholdBatchId) {

    }
}
