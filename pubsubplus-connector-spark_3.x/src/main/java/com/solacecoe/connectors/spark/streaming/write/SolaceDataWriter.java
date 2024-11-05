package com.solacecoe.connectors.spark.streaming.write;

import com.google.gson.Gson;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkSchemaProperties;
import com.solacecoe.connectors.spark.streaming.solace.SolaceBroker;
import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceAbortMessage;
import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceClassLoader;
import com.solacecoe.connectors.spark.streaming.solace.utils.SolacePublishStatus;
import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceWriterCommitMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.types.DataTypeUtils;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class SolaceDataWriter implements DataWriter<InternalRow>, Serializable {
    private static final Logger log = LoggerFactory.getLogger(SolaceDataWriter.class);
    private final StructType schema;
    private final Map<String, String> properties;
    private final SolaceBroker solaceBroker;
    private final UnsafeProjection projection;
    private final Map<String, SolaceWriterCommitMessage> commitMessages;
    private final Map<String, SolaceAbortMessage> abortedMessages;
    private Exception exception;
    private Class acknowledgementCallback;
    public SolaceDataWriter(StructType schema, Map<String, String> properties) {
        this.schema = schema;
        this.properties = properties;

        this.solaceBroker = new SolaceBroker(properties.get("host"), properties.get("vpn"), properties.get("username"), properties.get("password"), properties.get("topic"), properties);
        this.solaceBroker.initProducer(getJCSMPStreamingPublishCorrelatingEventHandler());

        this.projection = createProjection();
        this.commitMessages = new HashMap<>();
        this.abortedMessages = new HashMap<>();
        try {
            SolaceClassLoader solaceClassLoader = new SolaceClassLoader(SolaceBroker.class.getClassLoader());
            String className = properties.getOrDefault("acknowledgementCallback", null);
            if(className != null) {
                acknowledgementCallback = solaceClassLoader.loadClass(className);
            } else {
                log.warn("SolaceSparkConnector - No acknowledgement callback specified. Producer acknowledgement will not be notified");
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(InternalRow row) throws IOException {
        try {
            UnsafeRow projectedRow = this.projection.apply(row);
            checkForException();
            this.solaceBroker.publishMessage(projectedRow.getString(3), projectedRow.getBinary(1), projectedRow.getMap(5));
            checkForException();
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            String sStackTrace = sw.toString();
            SolaceAbortMessage abortMessage = new SolaceAbortMessage(SolacePublishStatus.FAILED, sStackTrace);
            abortedMessages.put(row.getString(0), abortMessage);
            exception = e;
            throw new RuntimeException(abortedMessages.toString());
        }
    }

    @Override
    public WriterCommitMessage commit() {
        checkForException();
        if(this.commitMessages.size() < Integer.parseInt(this.properties.getOrDefault("batchSize", "1"))) {
            try {
                log.info("SolaceSparkConnector - Expected acknowledgements {}, Actual acknowledgements {}", this.properties.getOrDefault("batchSize", "1"), this.commitMessages.size());
                log.info("SolaceSparkConnector - Sleeping for 3000ms to check for pending acknowledgments");
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        checkForException();
        invokeCallback(commitMessages);
        return null;
    }

    @Override
    public void abort() {
        log.error("SolaceSparkConnector - Publishing to Solace aborted", exception);
        invokeCallback(abortedMessages);
        throw new RuntimeException(abortedMessages.toString());
    }

    @Override
    public void close() {
        this.solaceBroker.close();
    }

    private UnsafeProjection createProjection() {
        List<Attribute> attributeList = new ArrayList<>();
        this.schema.foreach(field -> attributeList.add(DataTypeUtils.toAttribute(field)));
        Seq<Attribute> attributes = JavaConverters.asScalaIteratorConverter(attributeList.iterator()).asScala().toSeq();

        return UnsafeProjection.create(JavaConverters.asScalaIteratorConverter(Arrays.stream(getExpressions(attributes)).iterator()).asScala().toSeq(),
                JavaConverters.asScalaIteratorConverter(attributeList.iterator()).asScala().toSeq()
        );
    }

    private Expression[] getExpressions(Seq<Attribute> attributes) {

        return new Expression[] {
                // DataTypeUtils.toAttribute(new StructField("Id", DataTypes.StringType, true, Metadata.empty()))
                new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.id().name(), SolaceSparkSchemaProperties.id().dataType(), null).getExpression(),
                new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.payload().name(), SolaceSparkSchemaProperties.payload().dataType(), null).getExpression(),
                new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.partitionKey().name(), SolaceSparkSchemaProperties.partitionKey().dataType(), null).getExpression(),
                new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.topic().name(), SolaceSparkSchemaProperties.topic().dataType(), null).getExpression(),
                new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.timestamp().name(), SolaceSparkSchemaProperties.timestamp().dataType(), null).getExpression(),
                new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.headers().name(), SolaceSparkSchemaProperties.headers().dataType(), null).getExpression()
        };
    }

    private JCSMPStreamingPublishCorrelatingEventHandler getJCSMPStreamingPublishCorrelatingEventHandler() {
        return new JCSMPStreamingPublishCorrelatingEventHandler() {
            @Override
            public void responseReceivedEx(Object o) {
                log.info("SolaceSparkConnector - Message published successfully to Solace");
                SolaceWriterCommitMessage solaceWriterCommitMessage = new SolaceWriterCommitMessage(SolacePublishStatus.SUCCESS, "");
                commitMessages.put(o.toString(), solaceWriterCommitMessage);
            }

            @Override
            public void handleErrorEx(Object o, JCSMPException e, long l) {
                log.error("SolaceSparkConnector - Exception when publishing message to Solace", e);
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);
                String sStackTrace = sw.toString();
                SolaceAbortMessage abortMessage = new SolaceAbortMessage(SolacePublishStatus.FAILED, sStackTrace);
                abortedMessages.put(o.toString(), abortMessage);
                exception = e;
            }
        };
    }

    private void checkForException() {
        if(exception != null) {
            throw new RuntimeException(abortedMessages.toString());
        }
    }

    private void invokeCallback(Map<String, ?> messages) {
        if(acknowledgementCallback != null) {
            for (Constructor<?> constructor : acknowledgementCallback.getConstructors()) {
                try {
                    Gson gson = new Gson();
                    constructor.newInstance(gson.toJson(messages, Map.class));
                } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
