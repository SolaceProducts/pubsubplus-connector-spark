package com.solacecoe.connectors.spark;

import com.solacecoe.connectors.spark.containers.SolaceTestContainer;
import com.solacesystems.jcsmp.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Standalone empirical validation: does a single, long-lived JCSMP Browser instance continue to
 * see NEW messages published to a queue after it has already been exhausted (getNext() returned
 * null), or does it behave as a one-time snapshot that never sees anything published afterward?
 *
 * This directly validates the Browser-reuse fix applied to SolaceBroker.browseLVQ()/isQueueFull()
 * for DATAGO-149324 Finding 4 — reusing one Browser across micro-batches instead of recreating it
 * every call is only safe if the SAME Browser instance keeps delivering newly-arrived messages.
 */
@Testcontainers
public class BrowserLivenessValidationIT {
    private static SolaceTestContainer solaceTestContainer;
    private static JCSMPSession session;
    private static final String QUEUE_NAME = "browser-liveness-test-queue";

    @BeforeAll
    static void setup() throws Exception {
        solaceTestContainer = new SolaceTestContainer("solace/solace-pubsub-standard:latest", Collections.emptyMap());
        solaceTestContainer.start();

        JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, solaceTestContainer.getOrigin(org.testcontainers.solace.Service.SMF));
        properties.setProperty(JCSMPProperties.VPN_NAME, solaceTestContainer.getVpn());
        properties.setProperty(JCSMPProperties.USERNAME, solaceTestContainer.getUsername());
        properties.setProperty(JCSMPProperties.PASSWORD, solaceTestContainer.getPassword());
        session = JCSMPFactory.onlyInstance().createSession(properties);
        session.connect();

        Queue queue = JCSMPFactory.onlyInstance().createQueue(QUEUE_NAME);
        EndpointProperties endpointProperties = new EndpointProperties();
        endpointProperties.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);
        session.provision(queue, endpointProperties, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS | JCSMPSession.WAIT_FOR_CONFIRM);
    }

    @AfterAll
    static void teardown() {
        if (session != null && !session.isClosed()) {
            session.closeSession();
        }
        if (solaceTestContainer != null) {
            solaceTestContainer.stop();
        }
    }

    private void publish(String body) throws Exception {
        CountDownLatch acked = new CountDownLatch(1);
        AtomicReference<JCSMPException> failure = new AtomicReference<>();
        XMLMessageProducer producer = session.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
            @Override
            public void responseReceivedEx(Object key) {
                acked.countDown();
            }

            @Override
            public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
                failure.set(cause);
                acked.countDown();
            }
        });
        BytesXMLMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);
        msg.writeBytes(body.getBytes(StandardCharsets.UTF_8));
        msg.setDeliveryMode(DeliveryMode.PERSISTENT);
        Queue queue = JCSMPFactory.onlyInstance().createQueue(QUEUE_NAME);
        producer.send(msg, queue);
        if (!acked.await(10, TimeUnit.SECONDS)) {
            fail("Publish of '" + body + "' was not acknowledged within 10 seconds");
        }
        if (failure.get() != null) {
            throw failure.get();
        }
        producer.close();
    }

    private String bodyOf(BytesXMLMessage msg) {
        byte[] data = msg.getContentLength() != 0 ? msg.getBytes() : msg.getAttachmentByteBuffer().array();
        return new String(data, StandardCharsets.UTF_8);
    }

    @Test
    void reusedBrowserInstanceSeesMessagesPublishedAfterInitialExhaustion() throws Exception {
        Queue queue = JCSMPFactory.onlyInstance().createQueue(QUEUE_NAME);
        BrowserProperties browserProperties = new BrowserProperties();
        browserProperties.setEndpoint(queue);
        browserProperties.setTransportWindowSize(1);
        browserProperties.setWaitTimeout(2000);

        // Create the Browser ONCE — this is the exact reuse pattern applied in SolaceBroker.
        Browser browser = session.createBrowser(browserProperties);

        // 1. Publish msg1 BEFORE the browser has ever seen anything, browse it.
        publish("msg1");
        BytesXMLMessage first = browser.getNext(3000);
        assertEquals("msg1", first != null ? bodyOf(first) : null,
                "Expected the browser to see the message published before it started browsing");

        // 2. Exhaust the browser — getNext() should now time out and return null,
        //    exactly like isQueueFull()'s "queue appears empty" case.
        BytesXMLMessage exhausted = browser.getNext(2000);
        assertNull(exhausted, "Expected browser to report no messages once caught up");

        // 3. Publish msg2 AFTER the browser reported empty — WITHOUT recreating the browser.
        publish("msg2");

        // 4. Call getNext() again on the SAME, already-"exhausted" Browser instance.
        //    If Browser is a one-time snapshot, this will return null (or hang) forever — a real
        //    feature failure for the reuse fix. If it's a live, ongoing cursor, this returns msg2.
        BytesXMLMessage second = browser.getNext(5000);
        assertEquals("msg2", second != null ? bodyOf(second) : null,
                "REGRESSION: the reused Browser instance did NOT see a message published after " +
                        "it was previously exhausted. This would mean the Browser-reuse fix in " +
                        "SolaceBroker (Finding 4) is unsafe and must be reverted.");

        // 5. Do it again for good measure — rule out a one-off fluke.
        assertNull(browser.getNext(1000));
        publish("msg3");
        BytesXMLMessage third = browser.getNext(5000);
        assertEquals("msg3", third != null ? bodyOf(third) : null,
                "REGRESSION on second round-trip: reused Browser did not see msg3 either.");

        browser.close();
    }
}
