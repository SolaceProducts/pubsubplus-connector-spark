package com.solacecoe.connectors.spark;

import com.solace.semp.v2.config.ApiException;
import com.solace.semp.v2.config.client.model.MsgVpnQueue;
import com.solace.semp.v2.config.client.model.MsgVpnQueueSubscription;
import com.solace.semp.v2.config.client.model.MsgVpnReplayLog;
import com.solacecoe.connectors.spark.base.SempV2Api;
import com.solacecoe.connectors.spark.base.SolaceSession;
import com.solacecoe.connectors.spark.containers.SolaceTestContainer;
import com.solacecoe.connectors.spark.containers.SparkContainer;
import com.solacecoe.connectors.spark.containers.SparkWorkerContainer;
import com.solacesystems.jcsmp.*;
import com.solacesystems.jcsmp.Queue;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.solace.Service;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SolaceSparkStreamingMessageReplayIT {
    private SempV2Api sempV2Api = null;
    private SparkContainer sparkContainer;
    private SparkWorkerContainer sparkWorkerContainer;
    private SolaceTestContainer solaceTestContainer;
    private String replicationGroupMessageId = "";
    private String messageTimestamp = "";
    private int testIndex = 0;
    @BeforeAll
    public void beforeAll() throws ApiException, IOException {
        sparkContainer = new SparkContainer();
        sparkContainer.start();

        sparkWorkerContainer = new SparkWorkerContainer();
        sparkWorkerContainer.dependsOn(sparkContainer);
        sparkWorkerContainer.start();

        Map<String, Service> topics = new HashMap<String, Service>(){
            {
                put("solace/spark/streaming", Service.SMF);
                put("solace/spark/connector/offset", Service.SMF);
            }
        };
        solaceTestContainer = new SolaceTestContainer("solace/solace-pubsub-standard:latest", topics);
        solaceTestContainer.start();

        if(solaceTestContainer.isRunning()) {
            sempV2Api = new SempV2Api(String.format("http://%s:%d", solaceTestContainer.getHost(), solaceTestContainer.getMappedPort(8080)), "admin", "admin");
            MsgVpnQueue queue = new MsgVpnQueue();
            queue.queueName("Solace/Queue/0");
            queue.accessType(MsgVpnQueue.AccessTypeEnum.EXCLUSIVE);
            queue.permission(MsgVpnQueue.PermissionEnum.DELETE);
            queue.ingressEnabled(true);
            queue.egressEnabled(true);

            MsgVpnQueueSubscription subscription = new MsgVpnQueueSubscription();
            subscription.setSubscriptionTopic("solace/spark/streaming");

            sempV2Api.config().createMsgVpnQueue("default", queue, null, null);
            sempV2Api.config().createMsgVpnQueueSubscription("default", "Solace/Queue/0", subscription, null, null);

            MsgVpnReplayLog body = new MsgVpnReplayLog();
            body.setMaxSpoolUsage(10L);
            body.setReplayLogName("integration-test-replay-log");
            body.setIngressEnabled(true);
            body.setEgressEnabled(true);
            sempV2Api.config().createMsgVpnReplayLog("default", body, null, null);

            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            Date date = new Date(timestamp.getTime());
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            format.setTimeZone(TimeZone.getTimeZone(          // Capture the current moment in the wall-clock time used by the people of a certain region (a time zone).
                    ZoneId.systemDefault()   // Get the JVM’s current default time zone. Can change at any moment during runtime. If important, confirm with the user.
            ));
            messageTimestamp = format.format(date);
        } else {
            throw new RuntimeException("Solace Container is not started yet");
        }
    }

    @AfterAll
    public void afterAll() {
        sparkContainer.stop();
        sparkWorkerContainer.stop();
        solaceTestContainer.stop();
    }

    @BeforeEach
    public void beforeEach() throws ApiException {
        if(solaceTestContainer.isRunning() && (testIndex != 0 && testIndex <= 4)) {
            SempV2Api sempV2Api = new SempV2Api(String.format("http://%s:%d", solaceTestContainer.getHost(), solaceTestContainer.getMappedPort(8080)), "admin", "admin");

            MsgVpnQueue queue = new MsgVpnQueue();
            queue.queueName("Solace/Queue/" + testIndex);
            queue.accessType(MsgVpnQueue.AccessTypeEnum.EXCLUSIVE);
            queue.permission(MsgVpnQueue.PermissionEnum.DELETE);
            queue.ingressEnabled(true);
            queue.egressEnabled(true);

            MsgVpnQueueSubscription subscription = new MsgVpnQueueSubscription();
            subscription.setSubscriptionTopic("solace/spark/streaming");

            sempV2Api.config().createMsgVpnQueue("default", queue, null, null);
            sempV2Api.config().createMsgVpnQueueSubscription("default", "Solace/Queue/" + testIndex, subscription, null, null);
        }
    }

    @AfterEach
    public void afterEach() throws com.solace.semp.v2.action.ApiException {
        testIndex++;
        sempV2Api.action().doMsgVpnQueueDeleteMsgs("default", "Solace/Queue/0", new Object());
        sparkContainer.stop();
        sparkContainer.start();
        sparkWorkerContainer.stop();
        sparkWorkerContainer.start();
    }

    private void executeScript(String envVars) throws IOException, InterruptedException {
        sparkContainer.execInContainer(
                "sh", "-c",
                envVars + "/opt/spark/bin/spark-submit " +
                        "--master spark://spark-master:7077 " +
                        "--jars /opt/spark/jars/pubsubplus-connector-spark.jar " +
                        "/opt/spark/work-dir/SolaceSparkSource.py > /tmp/spark.log 2>&1 &"
        );
    }

    private void assertResult(boolean assertResult, int count, String text) throws InterruptedException, IOException {
        int expectedTotal = count;
        int timeoutSeconds = 60;
        boolean customMatcherResult = false;
        Pattern batchPattern = Pattern.compile("\"batchId\"\\s*:\\s*(\\d+)");
        Pattern rowsPattern = Pattern.compile("\"numInputRows\"\\s*:\\s*(\\d+)");
        Pattern customPattern = null;
        if(text != null) {
            customPattern = Pattern.compile(Pattern.quote(text));
        }
        Set<Integer> seenBatches = new HashSet<>();
        int total = 0;

        long start = System.currentTimeMillis();

        while ((System.currentTimeMillis() - start) < timeoutSeconds * 1000) {

            // 3️⃣ Read log file from container
            Container.ExecResult logResult = sparkContainer.execInContainer(
                    "bash", "-c", "cat /tmp/spark.log || true"
            );

            String logs = logResult.getStdout();

            // 4️⃣ Extract batchIds and numInputRows
            Matcher batchMatcher = batchPattern.matcher(logs);
            Matcher rowsMatcher = rowsPattern.matcher(logs);
            Matcher customMatcher = null;
            if(customPattern != null) {
                customMatcher = customPattern.matcher(logs);
            }

            List<Integer> batches = new ArrayList<>();
            List<Integer> rows = new ArrayList<>();

            while (batchMatcher.find()) {
                batches.add(Integer.parseInt(batchMatcher.group(1)));
            }

            while (rowsMatcher.find()) {
                rows.add(Integer.parseInt(rowsMatcher.group(1)));
            }

            if(customMatcher != null) {
                while (customMatcher.find()) {
                    customMatcherResult = true;
                }
            }

            // 5️⃣ Sum only new batches (avoid duplicates)
            for (int i = 0; i < Math.min(batches.size(), rows.size()); i++) {
                int batchId = batches.get(i);
                int numRows = rows.get(i);

                if (seenBatches.add(batchId)) {
                    total += numRows;
                }
            }

            if (assertResult && total >= expectedTotal) {
                System.out.println("Total records consumed " + total);
                if(text != null) {
                    System.out.println("Text '" + text + "' found in logs :: " + customMatcherResult);
                }
                break;
            } else if(!assertResult && customMatcherResult){
                System.out.println("Text '" + text + "' found in logs :: " + customMatcherResult);
                break;
            }

            Thread.sleep(1000);
        }

        // 6️⃣ Assertion
        if(assertResult) {
            assertEquals(expectedTotal, total);
        }
        if(text != null) {
            assertTrue(customMatcherResult);
        }
    }

    @Test
    @Order(1)
    public void Should_ProcessData() throws TimeoutException, InterruptedException, JCSMPException, ParseException, IOException {
        SolaceSession session = new SolaceSession(solaceTestContainer.getOrigin(Service.SMF), solaceTestContainer.getVpn(), solaceTestContainer.getUsername(), solaceTestContainer.getPassword());

        Queue tempQueue = session.getSession().createTemporaryQueue("temp.q");

        ConsumerFlowProperties flowProps = new ConsumerFlowProperties();
        flowProps.setEndpoint(tempQueue);

        FlowReceiver flowReceiver = session.getSession().createFlow(new XMLMessageListener() {
            @Override
            public void onReceive(BytesXMLMessage bytesXMLMessage) {
                if(replicationGroupMessageId == null || replicationGroupMessageId.isEmpty()) {
                    replicationGroupMessageId = bytesXMLMessage.getReplicationGroupMessageId().toString();
                    System.out.println("Rep group id " + replicationGroupMessageId);
                }
            }

            @Override
            public void onException(JCSMPException e) {

            }
        }, flowProps);

        Topic tempQueueSubscription = JCSMPFactory.onlyInstance().createTopic("solace/spark/streaming");
        session.getSession().addSubscription(tempQueue, tempQueueSubscription, JCSMPSession.WAIT_FOR_CONFIRM);

        flowReceiver.start();

        XMLMessageProducer messageProducer = session.getSession().getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
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
            textMessage.setDeliveryMode(DeliveryMode.PERSISTENT);
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            Date date = new Date(timestamp.getTime());
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            format.setTimeZone(TimeZone.getTimeZone(          // Capture the current moment in the wall-clock time used by the people of a certain region (a time zone).
                    ZoneId.systemDefault()   // Get the JVM’s current default time zone. Can change at any moment during runtime. If important, confirm with the user.
            ));
            textMessage.setSenderTimestamp(format.parse(format.format(date)).getTime());
            messageProducer.send(textMessage, topic);
        }

        messageProducer.close();

        executeScript("");
        assertResult(true, 100,null);
    }

    @Test
    @Order(2)
    public void Should_InitiateReplay_ALL_STRATEGY_And_ProcessData() throws TimeoutException, InterruptedException, IOException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_queue","Solace/Queue/1");
                put("solace_replayStrategy","BEGINNING");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(true, 100,null);
    }

    @Test
    @Order(3)
    public void Should_InitiateReplay_ALL_STRATEGY_And_Ack_Duplicate_Messages() throws TimeoutException, InterruptedException, IOException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_queue","Solace/Queue/2");
                put("solace_replayStrategy","BEGINNING");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(true, 0,null);
    }

    @Test
    @Order(4)
    public void Should_InitiateReplay_TIMEBASED_STRATEGY_And_ProcessData() throws TimeoutException, InterruptedException, IOException {
        String timezone = ZoneId.systemDefault().toString();

        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_queue","Solace/Queue/3");
                put("solace_replayStrategy","TIMEBASED");
                put("solace_replayStartTime", messageTimestamp);
                put("solace_replayStartTimeTimezone", timezone);
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(true, 100,null);
    }

    @Test
    @Order(5)
    public void Should_InitiateReplay_REPLICATIONGROUPMESSAGEID_STRATEGY_And_ProcessData() throws TimeoutException, InterruptedException, IOException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_queue","Solace/Queue/4");
                put("solace_replayStrategy","REPLICATION-GROUP-MESSAGE-ID");
                put("solace_replayReplicationGroupMessageId", replicationGroupMessageId);
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(true, 100,null);
    }

    @Test
    @Order(6)
    public void Should_Fail_IfReplayStrategyIsInvalid() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_queue","Solace/Queue/0");
                put("solace_replayStrategy","invalid-replay-strategy");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false, 0,"SolaceSparkConnector - Unsupported replay strategy: invalid-replay-strategy");
    }

    @Test
    @Order(7)
    public void Should_Fail_IfReplicationGroupMessageIdIsInvalid() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_queue","Solace/Queue/0");
                put("solace_replayStrategy","REPLICATION-GROUP-MESSAGE-ID");
                put("solace_replayReplicationGroupMessageId", "invalid-id");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false, 0,"Invalid Replication Group Message ID format: invalid-id");
    }

    @Test
    @Order(8)
    public void Should_Fail_IfReplicationGroupMessageIdIsNull() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_queue","Solace/Queue/0");
                put("solace_replayStrategy","REPLICATION-GROUP-MESSAGE-ID");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false, 0,"SolaceSparkConnector - Invalid replication group message id");
    }

    @Test
    @Order(8)
    public void Should_Fail_IfReplicationGroupMessageIdIsEmpty() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_queue","Solace/Queue/0");
                put("solace_replayStrategy","REPLICATION-GROUP-MESSAGE-ID");
                put("solace_replayReplicationGroupMessageId", "");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false, 0,"SolaceSparkConnector - Invalid replication group message id");
    }
}

