package com.solacecoe.connectors.spark;

import com.solacecoe.connectors.spark.base.SempV2Api;
import com.solacecoe.connectors.spark.containers.SolaceTestContainer;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkSchemaProperties;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.write.SolaceDataWriter;
import com.solacecoe.connectors.spark.streaming.write.SolaceStreamingDataWriterFactory;
import com.solace.semp.v2.monitor.client.model.MsgVpnClientsResponse;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.solace.Service;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * DATAGO-149324 Finding 3: SolaceDataWriter previously constructed a brand-new SolaceBroker (a full
 * JCSMP session - DNS resolution, TLS handshake) in its constructor, and Spark calls
 * DataWriterFactory.createWriter() once per partition per micro-batch. A 10s-trigger streaming job
 * opened/closed thousands of sessions per day, and a single transient DNS blip failed the write task.
 *
 * The fix caches the producer session per partition (via SolaceConnectionManager, the same mechanism
 * already used for consumer connections) and reuses it across micro-batches instead of rebuilding it
 * every trigger.
 *
 * This test runs against a real Solace broker and drives the actual SolaceStreamingDataWriterFactory /
 * SolaceDataWriter classes exactly as Spark would across several simulated micro-batches for the same
 * partition, then asserts via the SEMP monitor API that only ONE underlying client connection was ever
 * established - not one per micro-batch.
 */
@Testcontainers
class SolaceDataWriterSessionReuseIT {
    private static SolaceTestContainer solaceTestContainer;
    private static SempV2Api sempV2Api;
    private static Map<String, String> properties;
    private static final int PARTITION_ID = 0;
    private static final int MICRO_BATCHES = 5;

    @BeforeAll
    static void setup() throws Exception {
        solaceTestContainer = new SolaceTestContainer("solace/solace-pubsub-standard:latest", Collections.emptyMap());
        solaceTestContainer.start();
        sempV2Api = new SempV2Api(String.format("http://%s:%d", solaceTestContainer.getHost(), solaceTestContainer.getMappedPort(8080)), "admin", "admin");

        properties = new HashMap<>();
        properties.put(SolaceSparkStreamingProperties.HOST, solaceTestContainer.getOrigin(Service.SMF));
        properties.put(SolaceSparkStreamingProperties.VPN, solaceTestContainer.getVpn());
        properties.put(SolaceSparkStreamingProperties.USERNAME, solaceTestContainer.getUsername());
        properties.put(SolaceSparkStreamingProperties.PASSWORD, solaceTestContainer.getPassword());
        properties.put(SolaceSparkStreamingProperties.TOPIC, "solace/spark/streaming/writer-reuse-test");
        properties.put(SolaceSparkStreamingProperties.PUBLISH_ACK_TIMEOUT, "5000");
    }

    @AfterAll
    static void teardown() {
        if (solaceTestContainer != null) {
            solaceTestContainer.stop();
        }
    }

    private InternalRow row(String id) {
        return new GenericInternalRow(new Object[]{
                UTF8String.fromString(id),
                "hello".getBytes(StandardCharsets.UTF_8),
                UTF8String.fromString(""),
                null, // topic column null - hasDefaultTopic covers it via properties.TOPIC
                System.currentTimeMillis() * 1000L
        });
    }

    @Test
    void producerSessionIsReusedAcrossMicroBatchesForTheSamePartition() throws Exception {
        StructType schema = new StructType(SolaceSparkSchemaProperties.structFields(false));
        SolaceStreamingDataWriterFactory factory = new SolaceStreamingDataWriterFactory(schema, properties, new CaseInsensitiveStringMap(Collections.emptyMap()));

        for (int batch = 0; batch < MICRO_BATCHES; batch++) {
            // Exactly what Spark does once per partition, per micro-batch, per epoch.
            DataWriter<InternalRow> writer = factory.createWriter(PARTITION_ID, batch, batch);
            writer.write(row("msg-" + batch));
            writer.commit();
            writer.close();
        }

        MsgVpnClientsResponse response = sempV2Api.monitor().getMsgVpnClients(
                solaceTestContainer.getVpn(), 100, null,
                List.of("clientUsername==" + solaceTestContainer.getUsername(), "clientName==*producer*"),
                null);

        int connectedProducerClients = response.getData() == null ? 0 : response.getData().size();
        assertEquals(1, connectedProducerClients,
                "Expected exactly 1 underlying producer connection after " + MICRO_BATCHES +
                        " simulated micro-batches for the same partition (session reuse), but found " +
                        connectedProducerClients + " - see DATAGO-149324 Finding 3");
    }
}
