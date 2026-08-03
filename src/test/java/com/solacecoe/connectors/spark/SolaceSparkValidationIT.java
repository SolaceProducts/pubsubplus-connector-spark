package com.solacecoe.connectors.spark;

import com.solace.semp.v2.config.ApiException;
import com.solace.semp.v2.config.client.model.MsgVpnQueue;
import com.solace.semp.v2.config.client.model.MsgVpnQueueSubscription;
import com.solacecoe.connectors.spark.base.SempV2Api;
import com.solacecoe.connectors.spark.base.SolaceSession;
import com.solacecoe.connectors.spark.containers.SolaceTestContainer;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.solace.SolaceConnectionManager;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.solace.Service;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * All connector configuration-validation tests, run against a single <strong>in-JVM local[*]
 * SparkSession</strong> and a single Solace broker container.
 *
 * <p>These were originally spread across {@code SolaceSparkStreamingSourceIT},
 * {@code SolaceSparkStreamingSinkIT}, {@code SolaceSparkStreamingOAuthIT} and the TLS suites, where
 * each test launched a {@code spark-submit} inside a Spark master container and then polled
 * {@code /tmp/spark.log} for one literal string. None of them need a Spark cluster, a worker, or
 * (mostly) any data: the connector <em>throws</em> these messages, so asserting on the exception
 * chain is both faster and stricter than a log grep.
 *
 * <p>Everything lives in one class deliberately — a broker boot and a SparkSession start cost far
 * more than the tests themselves, so they are paid once here rather than once per class.
 *
 * <p>Sections below, and why they differ:
 * <ul>
 *   <li><b>Source</b> — {@code SolaceMicroBatch} validation and broker-side errors.</li>
 *   <li><b>Sink (batch)</b> — {@code SolaceBatchWrite}, which validates username/password itself.
 *       These are the only tests here that need data, since a non-empty micro-batch must exist
 *       before the batch writer is constructed; they call {@link #publishTestMessages()} explicitly.</li>
 *   <li><b>Sink (streaming)</b> — {@code SolaceStreamingWrite}, which defers username/password to
 *       JCSMP, so the same broken option yields a different message than the batch path.</li>
 *   <li><b>Auth</b> — OAuth2 and basic-auth option checks in
 *       {@code SolaceUtils.validateCommonProperties}, which runs before any connection.</li>
 * </ul>
 *
 * <p>Data-flow tests, the checkpoint-restart tests, and the OAuth/TLS cases whose errors come from a
 * live broker or {@code OAuthClient} remain on the container path in their original classes.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SolaceSparkValidationIT {
    private static final Logger LOG = LoggerFactory.getLogger(SolaceSparkValidationIT.class);

    /** Upper bound only — these tests return as soon as the connector throws. */
    private static final long FAILURE_TIMEOUT_MS = 60_000L;

    private SolaceTestContainer solaceTestContainer;
    private SempV2Api sempV2Api;
    private SparkSession sparkSession;

    /** Set when a test published messages, so only those tests pay for draining the queue. */
    private boolean messagesPublished = false;

    @BeforeAll
    public void beforeAll() throws ApiException {
        Map<String, Service> topics = new HashMap<String, Service>() {
            {
                put("solace/spark/streaming", Service.SMF);
                put("solace/spark/connector/offset", Service.SMF);
                put("solace/spark/streaming/offset", Service.SMF);
                put("random/topic", Service.SMF);
            }
        };
        solaceTestContainer = new SolaceTestContainer("solace/solace-pubsub-standard:10.11.1.147", topics);
        solaceTestContainer.start();
        if (!solaceTestContainer.isRunning()) {
            throw new RuntimeException("Solace Container is not started yet");
        }

        sempV2Api = new SempV2Api(
                String.format("http://%s:%d", solaceTestContainer.getHost(), solaceTestContainer.getMappedPort(8080)),
                "admin", "admin");

        MsgVpnQueue queue = new MsgVpnQueue();
        queue.queueName("Solace/Queue/0");
        queue.accessType(MsgVpnQueue.AccessTypeEnum.NON_EXCLUSIVE);
        queue.permission(MsgVpnQueue.PermissionEnum.DELETE);
        queue.ingressEnabled(true);
        queue.egressEnabled(true);
        queue.setMaxDeliveredUnackedMsgsPerFlow(50L);

        MsgVpnQueueSubscription subscription = new MsgVpnQueueSubscription();
        subscription.setSubscriptionTopic("solace/spark/streaming");

        sempV2Api.config().createMsgVpnQueue("default", queue, null, null);
        sempV2Api.config().createMsgVpnQueueSubscription("default", "Solace/Queue/0", subscription, null, null);

        // Deliberately restricted LVQ (EXCLUSIVE + NO_ACCESS) — this is what makes the three LVQ
        // permission tests get their expected 403s from the broker.
        MsgVpnQueue lvq = new MsgVpnQueue();
        lvq.queueName("Solace/Queue/lvq/0");
        lvq.accessType(MsgVpnQueue.AccessTypeEnum.EXCLUSIVE);
        lvq.permission(MsgVpnQueue.PermissionEnum.NO_ACCESS);
        lvq.setOwner("default");
        lvq.ingressEnabled(true);
        lvq.egressEnabled(true);
        lvq.setMaxMsgSpoolUsage(0L);

        MsgVpnQueueSubscription lvqSubscription = new MsgVpnQueueSubscription();
        lvqSubscription.setSubscriptionTopic("solace/spark/streaming/offset");

        sempV2Api.config().createMsgVpnQueue("default", lvq, null, null);
        sempV2Api.config().createMsgVpnQueueSubscription("default", "Solace/Queue/lvq/0", lvqSubscription, null, null);

        sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("SolaceSparkValidationIT")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.driver.host", "localhost")
                .getOrCreate();
        LOG.info("Local SparkSession started for validation tests (no Spark containers required)");
    }

    @AfterAll
    public void afterAll() {
        if (sparkSession != null) {
            sparkSession.stop();
        }
        if (solaceTestContainer != null) {
            solaceTestContainer.stop();
        }
    }

    @AfterEach
    public void afterEach() throws com.solace.semp.v2.action.ApiException {
        if (messagesPublished) {
            sempV2Api.action().doMsgVpnQueueDeleteMsgs("default", "Solace/Queue/0", new Object());
            messagesPublished = false;
        }
        // The connector keeps JVM-static registries (SolaceConnectionManager / SolaceMessageTracker).
        // In local[*] the driver and executors share this JVM, so state must be cleared between tests
        // or leftover broker sessions and unacked messages leak into the next test.
        SolaceConnectionManager.closeAllConnections();
    }

    /**
     * Publishes 100 messages to the source queue. Called only by the batch-sink tests: those need a
     * non-empty micro-batch before {@code SolaceBatchWrite} is constructed. Every other test here
     * fails during validation or connect, so publishing for them was pure overhead.
     */
    private void publishTestMessages() throws JCSMPException {
        SolaceSession session = new SolaceSession(solaceTestContainer.getOrigin(Service.SMF),
                solaceTestContainer.getVpn(), solaceTestContainer.getUsername(), solaceTestContainer.getPassword());
        XMLMessageProducer messageProducer = session.getSession()
                .getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
                    @Override
                    public void responseReceivedEx(Object o) {
                        // not required in test
                    }

                    @Override
                    public void handleErrorEx(Object o, JCSMPException e, long l) {
                        // not required in test
                    }
                });

        Topic topic = JCSMPFactory.onlyInstance().createTopic("solace/spark/streaming");
        for (int i = 0; i < 100; i++) {
            TextMessage textMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
            textMessage.setText("Hello Spark!");
            textMessage.setCorrelationId("test-id");
            messageProducer.send(textMessage, topic);
        }

        messageProducer.close();
        session.getSession().closeSession();
        messagesPublished = true;
    }

    private void assertSourceFailureContains(Map<String, String> options, Path checkpointDir, String expected) {
        String failure = SparkTestUtils.captureStreamFailure(sparkSession, options, checkpointDir, FAILURE_TIMEOUT_MS);
        assertTrue(failure.contains(expected),
                () -> "Expected the failure chain to contain:\n  " + expected + "\nbut got:\n" + failure);
    }

    private void assertBatchSinkFailureContains(Map<String, String> writeOptions, Path checkpointDir, String expected)
            throws JCSMPException {
        publishTestMessages();
        String failure = SparkTestUtils.captureBatchSinkFailure(sparkSession,
                SparkTestUtils.baseSourceOptions(solaceTestContainer), writeOptions, checkpointDir, FAILURE_TIMEOUT_MS);
        assertTrue(failure.contains(expected),
                () -> "Expected the failure chain to contain:\n  " + expected + "\nbut got:\n" + failure);
    }

    private void assertStreamingSinkFailureContains(Map<String, String> writeOptions, Path checkpointDir, String expected) {
        String failure = SparkTestUtils.captureStreamingSinkFailure(sparkSession,
                SparkTestUtils.baseSourceOptions(solaceTestContainer), writeOptions, checkpointDir, FAILURE_TIMEOUT_MS);
        assertTrue(failure.contains(expected),
                () -> "Expected the failure chain to contain:\n  " + expected + "\nbut got:\n" + failure);
    }

    // ==================================================================
    // Source validation
    // ==================================================================

    @Test
    void Should_Fail_IfQueueIsUnknown(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseSourceOptions(solaceTestContainer);
        options.put(SolaceSparkStreamingProperties.QUEUE, "unknown.q");

        assertSourceFailureContains(options, checkpointDir,
                "com.solacesystems.jcsmp.JCSMPErrorResponseException: 503: Unknown Queue");
    }

    @Test
    void Should_Fail_IfSolaceHostIsInvalid(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseSourceOptions(solaceTestContainer);
        options.put(SolaceSparkStreamingProperties.HOST, "tcp://invalid-host:55555");

        assertSourceFailureContains(options, checkpointDir,
                "com.solacesystems.jcsmp.InvalidPropertiesException: All hosts in the host list: 'tcp://invalid-host:55555' are not resolvable");
    }

    @Test
    void Should_Fail_IfMandatoryHostIsMissing(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseSourceOptions(solaceTestContainer);
        options.remove(SolaceSparkStreamingProperties.HOST);

        assertSourceFailureContains(options, checkpointDir,
                "SolaceSparkConnector - Please provide Solace Host name in configuration options");
    }

    @Test
    void Should_Fail_IfMandatoryHostIsEmpty(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseSourceOptions(solaceTestContainer);
        options.put(SolaceSparkStreamingProperties.HOST, "");

        assertSourceFailureContains(options, checkpointDir,
                "SolaceSparkConnector - Please provide Solace Host name in configuration options");
    }

    @Test
    void Should_Fail_IfMandatoryVpnIsMissing(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseSourceOptions(solaceTestContainer);
        options.remove(SolaceSparkStreamingProperties.VPN);

        assertSourceFailureContains(options, checkpointDir,
                "SolaceSparkConnector - Please provide Solace VPN name in configuration options");
    }

    @Test
    void Should_Fail_IfMandatoryVpnIsEmpty(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseSourceOptions(solaceTestContainer);
        options.put(SolaceSparkStreamingProperties.VPN, "");

        assertSourceFailureContains(options, checkpointDir,
                "SolaceSparkConnector - Please provide Solace VPN name in configuration options");
    }

    @Test
    void Should_Fail_IfMandatoryUsernameIsMissing(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseSourceOptions(solaceTestContainer);
        options.remove(SolaceSparkStreamingProperties.USERNAME);

        assertSourceFailureContains(options, checkpointDir,
                "InvalidPropertiesException: Property (username) is not provided.");
    }

    @Test
    void Should_Fail_IfMandatoryUsernameIsEmpty(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseSourceOptions(solaceTestContainer);
        options.put(SolaceSparkStreamingProperties.USERNAME, "");

        assertSourceFailureContains(options, checkpointDir,
                "InvalidPropertiesException: Property (username) is not provided.");
    }

    @Test
    void Should_Fail_IfMandatoryPasswordIsMissing(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseSourceOptions(solaceTestContainer);
        options.remove(SolaceSparkStreamingProperties.PASSWORD);

        assertSourceFailureContains(options, checkpointDir,
                "com.solacesystems.jcsmp.JCSMPErrorResponseException: 401: Unauthorized");
    }

    @Test
    void Should_Fail_IfMandatoryPasswordIsEmpty(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseSourceOptions(solaceTestContainer);
        options.put(SolaceSparkStreamingProperties.PASSWORD, "");

        assertSourceFailureContains(options, checkpointDir,
                "com.solacesystems.jcsmp.JCSMPErrorResponseException: 401: Unauthorized");
    }

    @Test
    void Should_Fail_IfMandatoryQueueIsMissing(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseSourceOptions(solaceTestContainer);
        options.remove(SolaceSparkStreamingProperties.QUEUE);

        assertSourceFailureContains(options, checkpointDir,
                "SolaceSparkConnector - Please provide Solace Queue in configuration options");
    }

    @Test
    void Should_Fail_IfMandatoryQueueIsNull(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseSourceOptions(solaceTestContainer);
        // Was the "NULL" env-var sentinel, which PySpark passed through as a real null.
        options.put(SolaceSparkStreamingProperties.QUEUE, null);

        assertSourceFailureContains(options, checkpointDir,
                "SolaceSparkConnector - Please provide Solace Queue in configuration options");
    }

    @Test
    void Should_Fail_IfMandatoryQueueIsEmpty(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseSourceOptions(solaceTestContainer);
        options.put(SolaceSparkStreamingProperties.QUEUE, "");

        assertSourceFailureContains(options, checkpointDir,
                "SolaceSparkConnector - Please provide Solace Queue in configuration options");
    }

    @Test
    void Should_Fail_IfBatchSizeLessThan0(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseSourceOptions(solaceTestContainer);
        options.put(SolaceSparkStreamingProperties.BATCH_SIZE, "-1");

        assertSourceFailureContains(options, checkpointDir,
                "SolaceSparkConnector - Please set batch size greater than zero");
    }

    @Test
    void Should_Fail_IfLVQTopic_Has_No_Permission_To_Publish(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseSourceOptions(solaceTestContainer);
        options.put(SolaceSparkStreamingProperties.BATCH_SIZE, "1");
        options.put(SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_TOPIC, "invalid/topic");

        assertSourceFailureContains(options, checkpointDir,
                "com.solacesystems.jcsmp.JCSMPErrorResponseException: 403: Subscription ACL Denied - Queue 'solace.spark.connector.state' - Topic 'invalid/topic'");
    }

    @Test
    void Should_Fail_IfLVQ_Has_No_Permission_To_Access(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseSourceOptions(solaceTestContainer);
        options.put(SolaceSparkStreamingProperties.BATCH_SIZE, "1");
        options.put(SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_NAME, "Solace/Queue/lvq/0");
        options.put(SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_TOPIC, "solace/spark/streaming/offset");

        assertSourceFailureContains(options, checkpointDir,
                "com.solacesystems.jcsmp.JCSMPErrorResponseException: 403: Permission Not Allowed - Queue 'Solace/Queue/lvq/0' - Topic 'solace/spark/streaming/offset'");
    }

    @Test
    void Should_Fail_IfLVQ_Has_No_Permission_To_Add_Subscription(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseSourceOptions(solaceTestContainer);
        options.put(SolaceSparkStreamingProperties.BATCH_SIZE, "1");
        options.put(SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_NAME, "Solace/Queue/lvq/0");
        options.put(SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_TOPIC, "invalid/topic");

        assertSourceFailureContains(options, checkpointDir,
                "com.solacesystems.jcsmp.JCSMPErrorResponseException: 403: Subscription ACL Denied - Queue 'Solace/Queue/lvq/0' - Topic 'invalid/topic'");
    }

    // ==================================================================
    // Sink validation — batch write path (SolaceBatchWrite); needs data
    // ==================================================================

    @Test
    void Should_Fail_Publish_IfSolaceHostIsInvalid(@TempDir Path checkpointDir) throws JCSMPException {
        Map<String, String> writeOptions = SparkTestUtils.baseSinkOptions(solaceTestContainer);
        writeOptions.put(SolaceSparkStreamingProperties.HOST, "tcp://invalid-host:55555");

        assertBatchSinkFailureContains(writeOptions, checkpointDir,
                "com.solacesystems.jcsmp.InvalidPropertiesException: All hosts in the host list: 'tcp://invalid-host:55555' are not resolvable");
    }

    @Test
    void Should_Fail_Publish_IfMandatoryHostIsMissing(@TempDir Path checkpointDir) throws JCSMPException {
        Map<String, String> writeOptions = SparkTestUtils.baseSinkOptions(solaceTestContainer);
        writeOptions.remove(SolaceSparkStreamingProperties.HOST);

        assertBatchSinkFailureContains(writeOptions, checkpointDir,
                "SolaceSparkConnector - Please provide Solace Host name in configuration options");
    }

    @Test
    void Should_Fail_Publish_IfMandatoryHostIsEmpty(@TempDir Path checkpointDir) throws JCSMPException {
        Map<String, String> writeOptions = SparkTestUtils.baseSinkOptions(solaceTestContainer);
        writeOptions.put(SolaceSparkStreamingProperties.HOST, "");

        assertBatchSinkFailureContains(writeOptions, checkpointDir,
                "SolaceSparkConnector - Please provide Solace Host name in configuration options");
    }

    @Test
    void Should_Fail_Publish_IfMandatoryVpnIsMissing(@TempDir Path checkpointDir) throws JCSMPException {
        Map<String, String> writeOptions = SparkTestUtils.baseSinkOptions(solaceTestContainer);
        writeOptions.remove(SolaceSparkStreamingProperties.VPN);

        assertBatchSinkFailureContains(writeOptions, checkpointDir,
                "SolaceSparkConnector - Please provide Solace VPN name in configuration options");
    }

    @Test
    void Should_Fail_Publish_IfMandatoryVpnIsEmpty(@TempDir Path checkpointDir) throws JCSMPException {
        Map<String, String> writeOptions = SparkTestUtils.baseSinkOptions(solaceTestContainer);
        writeOptions.put(SolaceSparkStreamingProperties.VPN, "");

        assertBatchSinkFailureContains(writeOptions, checkpointDir,
                "SolaceSparkConnector - Please provide Solace VPN name in configuration options");
    }

    @Test
    void Should_Fail_Publish_IfMandatoryUsernameIsMissing(@TempDir Path checkpointDir) throws JCSMPException {
        Map<String, String> writeOptions = SparkTestUtils.baseSinkOptions(solaceTestContainer);
        writeOptions.remove(SolaceSparkStreamingProperties.USERNAME);

        assertBatchSinkFailureContains(writeOptions, checkpointDir,
                "Please provide Solace Username in configuration options");
    }

    @Test
    void Should_Fail_Publish_IfMandatoryUsernameIsEmpty(@TempDir Path checkpointDir) throws JCSMPException {
        Map<String, String> writeOptions = SparkTestUtils.baseSinkOptions(solaceTestContainer);
        writeOptions.put(SolaceSparkStreamingProperties.USERNAME, "");

        assertBatchSinkFailureContains(writeOptions, checkpointDir,
                "Please provide Solace Username in configuration options");
    }

    @Test
    void Should_Fail_Publish_IfMandatoryPasswordIsMissing(@TempDir Path checkpointDir) throws JCSMPException {
        Map<String, String> writeOptions = SparkTestUtils.baseSinkOptions(solaceTestContainer);
        writeOptions.remove(SolaceSparkStreamingProperties.PASSWORD);

        assertBatchSinkFailureContains(writeOptions, checkpointDir,
                "Please provide Solace Password in configuration options");
    }

    @Test
    void Should_Fail_Publish_IfMandatoryPasswordIsEmpty(@TempDir Path checkpointDir) throws JCSMPException {
        Map<String, String> writeOptions = SparkTestUtils.baseSinkOptions(solaceTestContainer);
        writeOptions.put(SolaceSparkStreamingProperties.PASSWORD, "");

        assertBatchSinkFailureContains(writeOptions, checkpointDir,
                "Please provide Solace Password in configuration options");
    }

    // ==================================================================
    // Sink validation — streaming write path (SolaceStreamingWrite); no data needed
    // ==================================================================

    @Test
    void Should_Fail_Publish_Stream_IfSolaceHostIsInvalid(@TempDir Path checkpointDir) {
        Map<String, String> writeOptions = SparkTestUtils.baseSinkOptions(solaceTestContainer);
        writeOptions.put(SolaceSparkStreamingProperties.HOST, "tcp://invalid-host:55555");

        assertStreamingSinkFailureContains(writeOptions, checkpointDir,
                "com.solacesystems.jcsmp.InvalidPropertiesException: All hosts in the host list: 'tcp://invalid-host:55555' are not resolvable");
    }

    @Test
    void Should_Fail_Publish_Stream_IfMandatoryHostIsMissing(@TempDir Path checkpointDir) {
        Map<String, String> writeOptions = SparkTestUtils.baseSinkOptions(solaceTestContainer);
        writeOptions.remove(SolaceSparkStreamingProperties.HOST);

        assertStreamingSinkFailureContains(writeOptions, checkpointDir,
                "SolaceSparkConnector - Please provide Solace Host name in configuration options");
    }

    @Test
    void Should_Fail_Publish_Stream_IfMandatoryHostIsEmpty(@TempDir Path checkpointDir) {
        Map<String, String> writeOptions = SparkTestUtils.baseSinkOptions(solaceTestContainer);
        writeOptions.put(SolaceSparkStreamingProperties.HOST, "");

        assertStreamingSinkFailureContains(writeOptions, checkpointDir,
                "SolaceSparkConnector - Please provide Solace Host name in configuration options");
    }

    @Test
    void Should_Fail_Publish_Stream_IfMandatoryVpnIsMissing(@TempDir Path checkpointDir) {
        Map<String, String> writeOptions = SparkTestUtils.baseSinkOptions(solaceTestContainer);
        writeOptions.remove(SolaceSparkStreamingProperties.VPN);

        assertStreamingSinkFailureContains(writeOptions, checkpointDir,
                "SolaceSparkConnector - Please provide Solace VPN name in configuration options");
    }

    @Test
    void Should_Fail_Publish_Stream_IfMandatoryVpnIsEmpty(@TempDir Path checkpointDir) {
        Map<String, String> writeOptions = SparkTestUtils.baseSinkOptions(solaceTestContainer);
        writeOptions.put(SolaceSparkStreamingProperties.VPN, "");

        assertStreamingSinkFailureContains(writeOptions, checkpointDir,
                "SolaceSparkConnector - Please provide Solace VPN name in configuration options");
    }

    @Test
    void Should_Fail_Publish_Stream_IfMandatoryUsernameIsMissing(@TempDir Path checkpointDir) {
        Map<String, String> writeOptions = SparkTestUtils.baseSinkOptions(solaceTestContainer);
        writeOptions.remove(SolaceSparkStreamingProperties.USERNAME);

        assertStreamingSinkFailureContains(writeOptions, checkpointDir,
                "InvalidPropertiesException: Property (username) is not provided.");
    }

    @Test
    void Should_Fail_Publish_Stream_IfMandatoryUsernameIsEmpty(@TempDir Path checkpointDir) {
        Map<String, String> writeOptions = SparkTestUtils.baseSinkOptions(solaceTestContainer);
        writeOptions.put(SolaceSparkStreamingProperties.USERNAME, "");

        assertStreamingSinkFailureContains(writeOptions, checkpointDir,
                "InvalidPropertiesException: Property (username) is not provided.");
    }

    @Test
    void Should_Fail_Publish_Stream_IfMandatoryPasswordIsMissing(@TempDir Path checkpointDir) {
        Map<String, String> writeOptions = SparkTestUtils.baseSinkOptions(solaceTestContainer);
        writeOptions.remove(SolaceSparkStreamingProperties.PASSWORD);

        assertStreamingSinkFailureContains(writeOptions, checkpointDir,
                "com.solacesystems.jcsmp.JCSMPErrorResponseException: 401: Unauthorized");
    }

    @Test
    void Should_Fail_Publish_Stream_IfMandatoryPasswordIsEmpty(@TempDir Path checkpointDir) {
        Map<String, String> writeOptions = SparkTestUtils.baseSinkOptions(solaceTestContainer);
        writeOptions.put(SolaceSparkStreamingProperties.PASSWORD, "");

        assertStreamingSinkFailureContains(writeOptions, checkpointDir,
                "com.solacesystems.jcsmp.JCSMPErrorResponseException: 401: Unauthorized");
    }

    // ==================================================================
    // OAuth2 option validation (SolaceUtils.validateCommonProperties, OAuth branch)
    // ==================================================================

    @Test
    void Should_Fail_IfMandatoryOAuthURLIsMissing(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseOAuthOptions();
        options.remove(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL);

        assertSourceFailureContains(options, checkpointDir,
                "SolaceSparkConnector - Please provide OAuth Client Authentication Server URL");
    }

    @Test
    void Should_Fail_IfMandatoryOAuthURLIsEmpty(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseOAuthOptions();
        options.put(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL, "");

        assertSourceFailureContains(options, checkpointDir,
                "SolaceSparkConnector - Please provide OAuth Client Authentication Server URL");
    }

    @Test
    void Should_Fail_IfMandatoryOAuthClientIdIsMissing(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseOAuthOptions();
        options.remove(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID);

        assertSourceFailureContains(options, checkpointDir, "SolaceSparkConnector - Please provide OAuth Client ID");
    }

    @Test
    void Should_Fail_IfMandatoryOAuthClientIdIsEmpty(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseOAuthOptions();
        options.put(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID, "");

        assertSourceFailureContains(options, checkpointDir, "SolaceSparkConnector - Please provide OAuth Client ID");
    }

    @Test
    void Should_Fail_IfMandatoryOAuthClientSecretIsMissing(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseOAuthOptions();
        options.remove(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET);

        assertSourceFailureContains(options, checkpointDir,
                "SolaceSparkConnector - Please provide OAuth Client Credentials Secret");
    }

    @Test
    void Should_Fail_IfMandatoryOAuthClientSecretIsEmpty(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseOAuthOptions();
        options.put(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET, "");

        assertSourceFailureContains(options, checkpointDir,
                "SolaceSparkConnector - Please provide OAuth Client Credentials Secret");
    }

    @Test
    void Should_Fail_IfAccessTokenFileIsEmpty(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseOAuthOptions();
        // Presence of the access-token option selects the token branch; empty value is then rejected.
        options.put(SolaceSparkStreamingProperties.OAUTH_CLIENT_ACCESSTOKEN, "");

        assertSourceFailureContains(options, checkpointDir,
                "SolaceSparkConnector - Please provide valid access token input");
    }

    @Test
    void Should_Fail_When_TrustStorePasswordIsNull(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseOAuthOptions();
        // Equivalent to add_client_cert + add_client_cert_to_custom_truststore +
        // set_truststore_password_null: a client certificate is configured, so a truststore password
        // becomes mandatory. The paths are only null-checked, never opened.
        options.put(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_CLIENT_CERTIFICATE,
                "/opt/spark/work-dir/keycloak.crt");
        options.put(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_TRUSTSTORE_FILE,
                "/opt/spark/work-dir/custom_truststore.jks");
        options.put(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_SSL_VALIDATE_CERTIFICATE, "true");
        options.remove(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_TRUSTSTORE_PASSWORD);

        assertSourceFailureContains(options, checkpointDir,
                "SolaceSparkConnector - Please provide OAuth Client TrustStore Password. If TrustStore file path is not configured, please provide password for default java truststore");
    }

    // ==================================================================
    // Basic auth option validation (converted from the TLS username/password tests)
    // ==================================================================

    @Test
    void Should_Fail_BasicAuth_IfMandatoryUsernameIsMissing(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseBasicAuthOptions();
        options.remove(SolaceSparkStreamingProperties.USERNAME);

        assertSourceFailureContains(options, checkpointDir,
                "SolaceSparkConnector - Please provide Solace Username in configuration options");
    }

    @Test
    void Should_Fail_BasicAuth_IfMandatoryUsernameIsEmpty(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseBasicAuthOptions();
        options.put(SolaceSparkStreamingProperties.USERNAME, "");

        assertSourceFailureContains(options, checkpointDir,
                "SolaceSparkConnector - Please provide Solace Username in configuration options");
    }

    @Test
    void Should_Fail_BasicAuth_IfMandatoryPasswordIsMissing(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseBasicAuthOptions();
        options.remove(SolaceSparkStreamingProperties.PASSWORD);

        assertSourceFailureContains(options, checkpointDir,
                "SolaceSparkConnector - Please provide Solace Password in configuration options");
    }

    @Test
    void Should_Fail_BasicAuth_IfMandatoryPasswordIsEmpty(@TempDir Path checkpointDir) {
        Map<String, String> options = SparkTestUtils.baseBasicAuthOptions();
        options.put(SolaceSparkStreamingProperties.PASSWORD, "");

        assertSourceFailureContains(options, checkpointDir,
                "SolaceSparkConnector - Please provide Solace Password in configuration options");
    }
}
