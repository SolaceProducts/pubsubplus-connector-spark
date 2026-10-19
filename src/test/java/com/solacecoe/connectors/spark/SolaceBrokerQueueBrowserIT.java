package com.solacecoe.connectors.spark;

import com.solace.semp.v2.config.client.model.MsgVpnQueue;
import com.solace.semp.v2.monitor.client.model.MsgVpnQueueTxFlowsResponse;
import com.solacecoe.connectors.spark.base.SempV2Api;
import com.solacecoe.connectors.spark.base.SolaceSession;
import com.solacecoe.connectors.spark.containers.SolaceTestContainer;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.solace.SolaceBroker;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.solace.Service;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DATAGO-149324 Finding 4: SolaceBroker.isQueueFull()/browseLVQ() created and destroyed a Browser
 * (a broker-side flow) on every micro-batch, and used the no-arg Browser.getNext() which - per
 * Solace Support's independent reproduction - could block the driver's offset thread for 80+ minutes
 * instead of honouring the configured wait timeout.
 *
 * The fix: getNext(timeout) is called explicitly, the wait timeout / retry count are configurable
 * (queueFullCheckWaitTimeoutInMillis / queueFullCheckRetries), and the Browser is reused across
 * micro-batches by default (reuseBrowserConnections, opt-out available for flow-constrained brokers).
 *
 * Runs against a real Solace broker. Flow reuse is verified via the SEMP monitor API - the same
 * getMsgVpnQueueTxFlows call already used elsewhere in this test suite - rather than by inference.
 */
@Testcontainers
class SolaceBrokerQueueBrowserIT {
    private static SolaceTestContainer solaceTestContainer;
    private static SempV2Api sempV2Api;
    private static Map<String, String> baseProperties;
    private static final String QUEUE_NAME = "queue-browser-reuse-test-queue";

    @BeforeAll
    static void setup() throws Exception {
        solaceTestContainer = new SolaceTestContainer("solace/solace-pubsub-standard:latest", Collections.emptyMap());
        solaceTestContainer.start();
        sempV2Api = new SempV2Api(String.format("http://%s:%d", solaceTestContainer.getHost(), solaceTestContainer.getMappedPort(8080)), "admin", "admin");

        MsgVpnQueue queue = new MsgVpnQueue();
        queue.queueName(QUEUE_NAME);
        queue.accessType(MsgVpnQueue.AccessTypeEnum.NON_EXCLUSIVE);
        queue.permission(MsgVpnQueue.PermissionEnum.DELETE);
        queue.ingressEnabled(true);
        queue.egressEnabled(true);
        sempV2Api.config().createMsgVpnQueue("default", queue, null, null);

        baseProperties = new HashMap<>();
        baseProperties.put(SolaceSparkStreamingProperties.HOST, solaceTestContainer.getOrigin(Service.SMF));
        baseProperties.put(SolaceSparkStreamingProperties.VPN, solaceTestContainer.getVpn());
        baseProperties.put(SolaceSparkStreamingProperties.USERNAME, solaceTestContainer.getUsername());
        baseProperties.put(SolaceSparkStreamingProperties.PASSWORD, solaceTestContainer.getPassword());
        baseProperties.put(SolaceSparkStreamingProperties.QUEUE, QUEUE_NAME);
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

    private void publishToQueue(String body) throws Exception {
        SolaceSession session = new SolaceSession(solaceTestContainer.getOrigin(Service.SMF), solaceTestContainer.getVpn(), solaceTestContainer.getUsername(), solaceTestContainer.getPassword());
        XMLMessageProducer producer = session.getSession().getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
            @Override public void responseReceivedEx(Object o) { }
            @Override public void handleErrorEx(Object o, com.solacesystems.jcsmp.JCSMPException e, long l) { }
        });
        com.solacesystems.jcsmp.BytesXMLMessage msg = JCSMPFactory.onlyInstance().createMessage(com.solacesystems.jcsmp.BytesXMLMessage.class);
        msg.writeBytes(body.getBytes(StandardCharsets.UTF_8));
        msg.setDeliveryMode(DeliveryMode.PERSISTENT);
        Queue queue = JCSMPFactory.onlyInstance().createQueue(QUEUE_NAME);
        producer.send(msg, queue);
        Thread.sleep(500); // let it land before browsing
        producer.close();
        session.getSession().closeSession();
    }

    @Test
    void reusedBrowserKeepsExactlyOneStandingFlowAcrossManyPolls() throws Exception {
        Map<String, String> properties = propertiesWith(
                SolaceSparkStreamingProperties.QUEUE_FULL_CHECK_WAIT_TIMEOUT, "300",
                SolaceSparkStreamingProperties.QUEUE_FULL_CHECK_RETRIES, "1",
                SolaceSparkStreamingProperties.REUSE_BROWSER_CONNECTIONS, "true"
        );
        SolaceBroker broker = new SolaceBroker(properties, "monitoring-consumer");
        try {
            for (int i = 0; i < 5; i++) {
                broker.isQueueFull(); // queue is empty - each call is bounded by the configured timeout
            }

            MsgVpnQueueTxFlowsResponse flows = sempV2Api.monitor().getMsgVpnQueueTxFlows("default", QUEUE_NAME, 10, null, null, null);
            int flowCount = flows.getData() == null ? 0 : flows.getData().size();
            assertEquals(1, flowCount,
                    "Expected exactly 1 standing egress flow on the queue after 5 polls with " +
                            "reuseBrowserConnections=true (one Browser reused across calls), but found " +
                            flowCount + " - see DATAGO-149324 Finding 4");
        } finally {
            broker.close();
        }
    }

    @Test
    void nonReusedBrowserLeavesNoStandingFlowBetweenPolls() throws Exception {
        Map<String, String> properties = propertiesWith(
                SolaceSparkStreamingProperties.QUEUE_FULL_CHECK_WAIT_TIMEOUT, "300",
                SolaceSparkStreamingProperties.QUEUE_FULL_CHECK_RETRIES, "1",
                SolaceSparkStreamingProperties.REUSE_BROWSER_CONNECTIONS, "false"
        );
        SolaceBroker broker = new SolaceBroker(properties, "monitoring-consumer");
        try {
            broker.isQueueFull();

            MsgVpnQueueTxFlowsResponse flows = sempV2Api.monitor().getMsgVpnQueueTxFlows("default", QUEUE_NAME, 10, null, null, null);
            int flowCount = flows.getData() == null ? 0 : flows.getData().size();
            assertEquals(0, flowCount,
                    "Expected no standing flow between polls when reuseBrowserConnections=false " +
                            "(each poll should provision and tear down its own Browser), but found " +
                            flowCount + " - see DATAGO-149324 Finding 4");
        } finally {
            broker.close();
        }
    }

    @Test
    void isQueueFullReturnsBoundedRegardlessOfConfiguredRetries() throws Exception {
        // This is the direct regression guard for the confirmed root cause of the 80+ minute hang:
        // getNext(timeout) must actually bound the wait, honouring the configured retry count, rather
        // than blocking indefinitely on the no-arg getNext().
        Map<String, String> properties = propertiesWith(
                SolaceSparkStreamingProperties.QUEUE_FULL_CHECK_WAIT_TIMEOUT, "500",
                SolaceSparkStreamingProperties.QUEUE_FULL_CHECK_RETRIES, "3"
        );
        SolaceBroker broker = new SolaceBroker(properties, "monitoring-consumer");
        try {
            long start = System.currentTimeMillis();
            boolean full = broker.isQueueFull();
            long elapsed = System.currentTimeMillis() - start;

            assertFalse(full, "Queue is empty at this point in the test - isQueueFull() should return false");
            // 3 attempts x 500ms = 1500ms upper bound, with generous headroom for scheduling jitter.
            assertTrue(elapsed < 5000,
                    "isQueueFull() took " + elapsed + "ms on an empty queue with " +
                            "queueFullCheckRetries=3/queueFullCheckWaitTimeoutInMillis=500 (expected <5000ms) " +
                            "- indicates the bounded getNext(timeout) fix regressed - see DATAGO-149324 Finding 4");
        } finally {
            broker.close();
        }

        // Now prove it correctly detects a genuinely full queue too, not just "always returns false fast".
        publishToQueue("hello");
        SolaceBroker broker2 = new SolaceBroker(properties, "monitoring-consumer");
        try {
            assertTrue(broker2.isQueueFull(), "isQueueFull() should return true once a message is available");
        } finally {
            broker2.close();
        }
    }
}
