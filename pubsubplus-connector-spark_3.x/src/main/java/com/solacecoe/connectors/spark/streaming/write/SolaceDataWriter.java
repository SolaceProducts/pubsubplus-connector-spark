package com.solacecoe.connectors.spark.streaming.write;

import com.solacecoe.connectors.spark.streaming.solace.SolaceBroker;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Map;

public class SolaceDataWriter implements DataWriter {
    private final StructType schema;
    private final Map<String, String> options;
    public SolaceDataWriter(StructType schema, Map<String, String> options) {
        this.schema = schema;
        this.options = options;

        SolaceBroker solaceBroker = new SolaceBroker(options.get("host"), options.get("vpn"), options.get("username"), options.get("password"), options.get("topic"), options);
    }

    @Override
    public void write(Object o) throws IOException {

    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        return null;
    }

    @Override
    public void abort() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
}
