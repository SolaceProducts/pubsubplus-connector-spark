package com.solacecoe.connectors.spark;

import com.solacecoe.connectors.spark.base.SempV2Api;
import com.solacecoe.connectors.spark.containers.SolaceTestContainer;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.solace.SolaceBroker;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolaceSessionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.solace.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DATAGO-149324 Finding 2: SolaceBroker.publishMessage(topic, msg) - the LVQ checkpoint publish used
 * by SolaceMicroBatch.commit() - had no retry. A single transient transport exception (e.g. "Channel
 * is closed by peer", a routine, recoverable event over a multi-day session) went straight to
 * handleException() and killed the streaming query fatally.
 *
 * These tests run against a real Solace broker (Testcontainers) and use the SEMP action API to
 * forcibly disconnect the client mid-test, reproducing the actual "Channel is closed by peer" failure
 * class rather than a mocked exception.
 */
@Testcontainers
class SolaceBrokerLvqPublishRetryIT {
    private static SolaceTestContainer solaceTestContainer;
    private static SempV2Api sempV2Api;
    private static Map<String, String> baseProperties;

    @BeforeAll
    static void setup() throws Exception {
        solaceTestContainer = new SolaceTestContainer("solace/solace-pubsub-standard:latest", Collections.emptyMap());
        solaceTestContainer.start();
        sempV2Api = new SempV2Api(String.format("http://%s:%d", solaceTestContainer.getHost(), solaceTestContainer.getMappedPort(8080)), "admin", "admin");

        baseProperties = new HashMap<>();
        baseProperties.put(SolaceSparkStreamingProperties.HOST, solaceTestContainer.getOrigin(Service.SMF));
        baseProperties.put(SolaceSparkStreamingProperties.VPN, solaceTestContainer.getVpn());
        baseProperties.put(SolaceSparkStreamingProperties.USERNAME, solaceTestContainer.getUsername());
        baseProperties.put(SolaceSparkStreamingProperties.PASSWORD, solaceTestContainer.getPassword());
        // SolaceBroker's constructor reads this but browseLVQ()/isQueueFull() are never exercised here.
        baseProperties.put(SolaceSparkStreamingProperties.QUEUE, "lvq-retry-test-queue-not-used");
    }

    @AfterAll
    static void teardown() {
        if (solaceTestContainer != null) {
            solaceTestContainer.stop();
        }
    }

    private Map<String, String> propertiesWith(String... kv) {
        Map<String, String> props = new HashMap<>(baseProperties);
        for (int i = 0; i < kv.length; i += 2) {
            props.put(kv[i], kv[i + 1]);
        }
        return props;
    }

    @Test
    void publishSurvivesTransientDisconnectWithinRetryBudget() throws Exception {
        Map<String, String> properties = propertiesWith(
                SolaceSparkStreamingProperties.LVQ_PUBLISH_RETRIES, "6",
                SolaceSparkStreamingProperties.LVQ_PUBLISH_RETRY_INTERVAL, "500",
                // JCSMP's own session-level auto-reconnect - this is what actually heals the transport;
                // our retry loop's job is only to give it time to do so instead of failing immediately.
                SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES, "10",
                SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES_WAIT_TIME, "300"
        );
        SolaceBroker broker = new SolaceBroker(properties, "producer");
        try {
            broker.createLVQIfNotExist();
            broker.initProducer();

            // Forcibly sever this client's transport at the broker - the same failure class as the
            // customer's "Channel is closed by peer", not a simulated/mocked exception.
            sempV2Api.action().doMsgVpnClientDisconnect(solaceTestContainer.getVpn(), broker.getUniqueName(), new Object());

            assertDoesNotThrow(() -> broker.publishMessage(
                            SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_DEFAULT_TOPIC, "{\"test\":\"payload\"}"),
                    "A transient disconnect within the configured retry budget must not fail the LVQ " +
                            "publish - see DATAGO-149324 Finding 2");
        } finally {
            broker.close();
        }
    }

    @Test
    void publishRetriesWithBackoffThenFailsWhenBudgetExhausted() throws Exception {
        Map<String, String> properties = propertiesWith(
                SolaceSparkStreamingProperties.LVQ_PUBLISH_RETRIES, "3",
                SolaceSparkStreamingProperties.LVQ_PUBLISH_RETRY_INTERVAL, "300",
                // Deliberately prevent JCSMP's own session from self-healing, so every one of our
                // retry attempts genuinely fails - isolating our own retry+backoff loop's behavior.
                SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES, "0"
        );
        SolaceBroker broker = new SolaceBroker(properties, "producer");
        try {
            broker.createLVQIfNotExist();
            broker.initProducer();

            sempV2Api.action().doMsgVpnClientDisconnect(solaceTestContainer.getVpn(), broker.getUniqueName(), new Object());

            long start = System.currentTimeMillis();
            assertThrows(SolaceSessionException.class, () -> broker.publishMessage(
                    SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_DEFAULT_TOPIC, "{\"test\":\"payload\"}"));
            long elapsed = System.currentTimeMillis() - start;

            // 3 retries at 300/600/1200ms backoff = >=2100ms before giving up on the 4th attempt.
            // This is the core regression guard: one failure must NOT fail the query immediately.
            assertTrue(elapsed >= 2000,
                    "Expected publishMessage to retry with exponential backoff (>=2000ms elapsed for " +
                            "3 retries at 300/600/1200ms) before giving up, but only " + elapsed + "ms elapsed " +
                            "- see DATAGO-149324 Finding 2");
        } finally {
            broker.close();
        }
    }
}
