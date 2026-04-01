package com.solacecoe.connectors.spark;

import com.solace.semp.v2.config.ApiException;
import com.solace.semp.v2.config.client.model.MsgVpnQueue;
import com.solace.semp.v2.config.client.model.MsgVpnQueueSubscription;
import com.solace.semp.v2.monitor.client.model.MsgVpnQueueResponse;
import com.solace.semp.v2.monitor.client.model.MsgVpnQueueTxFlowsResponse;
import com.solacecoe.connectors.spark.base.SempV2Api;
import com.solacecoe.connectors.spark.base.SolaceSession;
import com.solacecoe.connectors.spark.containers.SolaceTestContainer;
import com.solacecoe.connectors.spark.containers.SparkContainer;
import com.solacecoe.connectors.spark.containers.SparkWorkerContainer;
import com.solacesystems.jcsmp.*;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.solace.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SolaceSparkStreamingSinkIT {
    private SempV2Api sempV2Api = null;
    private SparkContainer sparkContainer;
    private SparkWorkerContainer sparkWorkerContainer;
    private SolaceTestContainer solaceTestContainer;
    private SolaceSession session;
    private Topic topic;
    @BeforeAll
    public void beforeAll() throws ApiException, IOException, JCSMPException {
        sparkContainer = new SparkContainer(false, false);
        sparkContainer.start();

        sparkWorkerContainer = new SparkWorkerContainer(false, false);
        sparkWorkerContainer.dependsOn(sparkContainer);
        sparkWorkerContainer.start();

        Map<String, Service> topics = new HashMap<String, Service>(){
            {
                put("solace/spark/streaming", Service.SMF);
                put("solace/spark/connector/offset", Service.SMF);
                put("random/topic", Service.SMF);
                put("Spark/Topic/0", Service.SMF);
            }
        };
        solaceTestContainer = new SolaceTestContainer("solace/solace-pubsub-standard:latest", topics);
        solaceTestContainer.start();
        if(solaceTestContainer.isRunning()) {
            sempV2Api = new SempV2Api(String.format("http://%s:%d", solaceTestContainer.getHost(), solaceTestContainer.getMappedPort(8080)), "admin", "admin");
            MsgVpnQueue queue = new MsgVpnQueue();
            queue.queueName("Solace/Queue/0");
            queue.accessType(MsgVpnQueue.AccessTypeEnum.NON_EXCLUSIVE);
            queue.permission(MsgVpnQueue.PermissionEnum.DELETE);
            queue.ingressEnabled(true);
            queue.egressEnabled(true);

            MsgVpnQueueSubscription subscription = new MsgVpnQueueSubscription();
            subscription.setSubscriptionTopic("solace/spark/streaming");

            sempV2Api.config().createMsgVpnQueue("default", queue, null, null);
            sempV2Api.config().createMsgVpnQueueSubscription("default", "Solace/Queue/0", subscription, null, null);

            MsgVpnQueue emptyQueue = new MsgVpnQueue();
            emptyQueue.queueName("Solace/Queue/Empty");
            emptyQueue.accessType(MsgVpnQueue.AccessTypeEnum.EXCLUSIVE);
            emptyQueue.permission(MsgVpnQueue.PermissionEnum.DELETE);
            emptyQueue.ingressEnabled(true);
            emptyQueue.egressEnabled(true);

            sempV2Api.config().createMsgVpnQueue("default", emptyQueue, null, null);

            session = new SolaceSession(solaceTestContainer.getOrigin(Service.SMF), solaceTestContainer.getVpn(), solaceTestContainer.getUsername(), solaceTestContainer.getPassword());
            topic = JCSMPFactory.onlyInstance().createTopic("random/topic");
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
    public void beforeEach() throws JCSMPException {
        if(solaceTestContainer.isRunning()) {
            session.getSession().addSubscription(topic);
            SolaceSession session = new SolaceSession(solaceTestContainer.getOrigin(Service.SMF), solaceTestContainer.getVpn(), solaceTestContainer.getUsername(), solaceTestContainer.getPassword());

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

            for (int i = 0; i < 100; i++) {
                TextMessage textMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
                textMessage.setText("Hello Spark!");
                textMessage.setCorrelationId("test-id");
                textMessage.setPriority(1);
                textMessage.setDMQEligible(true);
                SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();
                sdtMap.putString("custom-string", "custom-value");
                sdtMap.putBoolean("custom-boolean", true);
                sdtMap.putString("null-custom-property", null);
                sdtMap.putString("empty-custom-property", "");
                sdtMap.putInteger("custom-sequence", i);
                textMessage.setProperties(sdtMap);
                Topic topic = JCSMPFactory.onlyInstance().createTopic("solace/spark/streaming");
                messageProducer.send(textMessage, topic);
            }

            messageProducer.close();
            session.getSession().closeSession();
        } else {
            throw new RuntimeException("Solace Container is not started yet");
        }
    }

    @AfterEach
    public void afterEach() throws com.solace.semp.v2.action.ApiException, JCSMPException {
        sempV2Api.action().doMsgVpnQueueDeleteMsgs("default", "Solace/Queue/0", new Object());
        session.getSession().removeSubscription(topic);
        sparkContainer.stop();
        sparkContainer.start();
        sparkWorkerContainer.stop();
        sparkWorkerContainer.start();
    }

    private void executeScript(String envVars, boolean isBatch) throws IOException, InterruptedException {
        if(!isBatch) {
            sparkContainer.execInContainer(
                    "sh", "-c",
                    envVars + "/opt/spark/bin/spark-submit " +
                            "--master spark://spark-master:7077 " +
                            "--jars /opt/spark/jars/pubsubplus-connector-spark.jar " +
                            "/opt/spark/work-dir/SolaceSparkSink.py > /tmp/spark.log 2>&1 &"
            );
        } else {
            sparkContainer.execInContainer(
                    "sh", "-c",
                    envVars + "/opt/spark/bin/spark-submit " +
                            "--master spark://spark-master:7077 " +
                            "--jars /opt/spark/jars/pubsubplus-connector-spark.jar " +
                            "/opt/spark/work-dir/SolaceSparkSink_ForEachBatch.py > /tmp/spark.log 2>&1 &"
            );
        }
    }

    private void assertResult(boolean assertResult, String text, int times) throws InterruptedException, IOException {
        int expectedTotal = 100;
        int timeoutSeconds = 60;
        boolean customMatcherResult = false;
        int customMatcherCount = 0;
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
                    customMatcherCount++;
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
            }  else if(times > 1 && customMatcherCount < times) {
                // ignore and continue
            } else if(!assertResult && customMatcherResult){
                if(times > 1) {
                    // check again to make sure the matching is accurate
                }
                System.out.println("Text '" + text + "' found " +customMatcherCount+ " times in logs :: " + customMatcherResult);
                break;
            }

            Thread.sleep(1000);
        }

        // 6️⃣ Assertion
        if(assertResult) {
            assertEquals(expectedTotal, total);
        }
        if(text != null) {
            if(times > 0) {
                assertTrue(customMatcherResult);
            } else {
                assertFalse(customMatcherResult);
            }
        }
    }

    @Test
    @Order(1)
    void Should_ProcessData_And_Publish_As_Stream_To_Solace() throws TimeoutException, InterruptedException, IOException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_id","my-default-id");
                put("solace_topic","random/topic");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), false);

        final long[] count = {0};
        final String[] messageId = {""};

        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                    if(count[0] == 100) {
                        messageId[0] = bytesXMLMessage.getApplicationMessageId();
                        System.out.println("Total records consumed " + count[0]);
                        System.out.println("Received Application Message Id " + messageId[0]);
                    }
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test
                }
            });

            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
        Assertions.assertEquals("my-default-id", messageId[0], "MessageId mismatch");
    }

    @Test
    @Order(2)
    void Should_ProcessData_And_Publish_With_CustomId_To_Solace() throws TimeoutException, InterruptedException, IOException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_id","my-default-id");
                put("solace_topic","random/topic");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);

        final long[] count = {0};
        final String[] messageId = {""};


        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                    if(count[0] == 100) {
                        messageId[0] = bytesXMLMessage.getApplicationMessageId();
                        System.out.println("Total records consumed " + count[0]);
                        System.out.println("Received Application Message Id " + messageId[0]);
                    }
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test
                }
            });

            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
        Assertions.assertEquals("my-default-id", messageId[0], "MessageId mismatch");
    }

    @Test
    @Order(3)
    void Should_ProcessData_And_Publish_With_DataFrameId_To_Solace() throws TimeoutException, InterruptedException, IOException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_id","__DELETE__");
                put("solace_topic","random/topic");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);

        final long[] count = {0};


        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                    if(count[0] == 100) {
                        System.out.println("Total records consumed " + count[0]);
                    }
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });

            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
    }

    @Test
    @Order(17)
    void Should_ProcessData_And_Publish_To_CustomTopic_Solace() throws TimeoutException, InterruptedException, IOException, JCSMPException {
        session.getSession().removeSubscription(topic);
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_id","__DELETE__");
                put("solace_topic","Spark/Topic/0");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);

        final long[] count = {0};

        Topic topic = JCSMPFactory.onlyInstance().createTopic("Spark/Topic/0");
        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                    if(count[0] == 100) {
                        System.out.println("Total records consumed " + count[0]);
                    }
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
        session.getSession().removeSubscription(topic);
    }

    @Test
    @Order(5)
    void Should_ProcessData_And_Publish_With_Headers_To_Solace() throws TimeoutException, InterruptedException, IOException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_id","__DELETE__");
                put("solace_topic","random/topic");
                put("solace_includeHeaders","true");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);

        final long[] count = {0};
        final int[] messageHeader = {0};

        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                    if(count[0] == 100) {
                        if(bytesXMLMessage.getProperties().containsKey("custom-string")) {
                            try {
                                System.out.println(new String(bytesXMLMessage.getProperties().getByteArray("custom-string").asBytes(), StandardCharsets.UTF_8));
                            } catch (SDTException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        messageHeader[0] = bytesXMLMessage.getPriority();

                        System.out.println("Total records consumed " + count[0]);
                        System.out.println("Received Application Priority " + bytesXMLMessage.getPriority());
                    }
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> count[0] == 100);
        Assertions.assertEquals(1, messageHeader[0], "Message Priority mismatch");
    }

    @Test
    @Order(6)
    void Should_ProcessData_And_Publish_Without_Headers_To_Solace() throws TimeoutException, InterruptedException, IOException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_topic","random/topic");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);

        final long[] count = {0};
        final int[] messageHeader = {4};

        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                    if(count[0] == 100) {
                        messageHeader[0] = bytesXMLMessage.getPriority();
                        System.out.println("Total records consumed " + count[0]);
                        System.out.println("Received Default Application Priority " + messageHeader[0]);
                    }
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });

            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> count[0] == 100);
        Assertions.assertEquals(4, messageHeader[0], "Message Priority mismatch");
    }

    @Test
    @Order(7)
    void Should_ProcessData_And_Publish_With_Only_PayloadColumn_To_Solace() throws TimeoutException, InterruptedException, IOException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_topic","random/topic");
                put("drop_columns","TimeStamp,PartitionKey,Headers");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);

        final long[] count = {0};

        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                    if(count[0] == 100) {
                        System.out.println("Total records consumed " + count[0]);
                    }
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test
                }
            });

            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
    }

    @Test
    @Order(8)
    void Should_ProcessData_And_GetDataFrameCount_And_Publish_To_Solace() throws TimeoutException, InterruptedException, IOException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_topic","random/topic");
                put("drop_columns","TimeStamp,PartitionKey,Headers");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);

        final long[] count = {0};

        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                    if(count[0] == 100) {
                        System.out.println("Total records consumed from Solace " + count[0]);
                    }
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });

            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        assertResult(true, null, 0);
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
    }

    @Test
    @Order(9)
    void Should_ProcessData_And_Publish_To_Solace_And_NewBatch_ShouldNotBeTriggered() throws TimeoutException, InterruptedException, IOException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_topic","random/topic");
                put("drop_columns","TimeStamp,PartitionKey,Headers");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);

        final long[] count = {0};

        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                    if(count[0] == 100) {
                        System.out.println("Total records consumed from Solace " + count[0]);
                    }
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });

            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }
        assertResult(false, "Write Batch", 2);
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
    }

    @Test
    @Order(10)
    void Should_ProcessData_WithSingleConsumer_And_Publish_To_Solace_With_MultipleOperations_On_Dataframe() throws TimeoutException, InterruptedException, IOException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_topic","random/topic");
                put("drop_columns","TimeStamp,PartitionKey,Headers");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);

        final long[] count = {0};

        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });

            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        assertResult(true, null, 0);
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
    }

    @Test
    @Order(12)
    void Should_ProcessData_WithMultipleConsumer_And_Publish_To_Solace_With_MultipleOperations_On_Dataframe() throws TimeoutException, InterruptedException, IOException, com.solace.semp.v2.monitor.ApiException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_partitions","3");
                put("solace_topic","random/topic");
                put("drop_columns","TimeStamp,PartitionKey,Headers");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);

        final long[] count = {0};


        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });

            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

//        assertResult(true, null, 0);
        MsgVpnQueueTxFlowsResponse msgVpnQueueTxFlowResponse = sempV2Api.monitor().getMsgVpnQueueTxFlows("default", "Solace/Queue/0", 10, null, null, null);
        if (msgVpnQueueTxFlowResponse.getData() != null && !msgVpnQueueTxFlowResponse.getData().isEmpty()) {
            assertEquals(3, msgVpnQueueTxFlowResponse.getData().size(), "Number of consumer flows should be 3");
        }
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertTrue(count[0] >= 100));
    }

    @Test
    @Order(16)
    void Should_Not_ProcessData_When_QueueIsEmpty() throws TimeoutException, InterruptedException, IOException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_queue", "Solace/Queue/Empty");
                put("solace_topic","random/topic");
                put("drop_columns","TimeStamp,PartitionKey,Headers");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);

        final long[] count = {0};


        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });

            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        assertResult(false, "Write Batch", 1);
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(0, count[0]));
    }

    @Test
    @Order(13)
    @Disabled
    void Should_ProcessData_Publish_MicrosAs_SenderTimeStamp_To_Solace() throws TimeoutException, InterruptedException, IOException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_topic","random/topic");
                put("solace_includeHeaders", "false");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);

        final long[] count = {0};


        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                    if(count[0] == 100) {
                        System.out.println("Total records consumed from Solace " + count[0]);
                        System.out.println("Sender timestamp :: " + bytesXMLMessage.getSenderTimestamp());
                    }
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });

            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
    }

    @Test
    @Order(14)
    void Should_ProcessData_And_Publish_MillisAs_SenderTimeStamp_To_Solace() throws TimeoutException, InterruptedException, IOException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_topic","random/topic");
                put("solace_includeHeaders", "false");
                put("add_columns", "timestamp_ms");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);

        final long[] count = {0};
        final long[] timestamp = {0};

        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                    if(count[0] == 100) {
                        timestamp[0] = bytesXMLMessage.getSenderTimestamp();
                        System.out.println("Total records consumed from Solace " + count[0]);
                        System.out.println("Sender timestamp :: " + bytesXMLMessage.getSenderTimestamp());
                    }
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });

            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
        LocalDate dateFromTimestamp = Instant.ofEpochSecond(timestamp[0])
                .atZone(ZoneId.systemDefault())
                .toLocalDate();

        LocalDate today = LocalDate.now(ZoneId.systemDefault());

        assertEquals(today, dateFromTimestamp, "Timestamp is not current day");
    }

    @Test
    @Order(15)
    @Disabled
    void Should_ProcessData_And_Publish_SecondsAs_SenderTimeStamp_To_Solace() throws TimeoutException, InterruptedException, IOException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_topic","random/topic");
                put("solace_includeHeaders", "false");
                put("add_columns", "timestamp_sec");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);

        final long[] count = {0};
        final long[] timestamp = {0};

        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                    if(count[0] == 100) {
                        timestamp[0] = bytesXMLMessage.getSenderTimestamp();
                        System.out.println("Total records consumed from Solace " + count[0]);
                        System.out.println("Sender timestamp :: " + bytesXMLMessage.getSenderTimestamp());
                    }
                }

                @Override
                public void onException(JCSMPException e) {
                    // Not required for test

                }
            });

            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
        LocalDate dateFromTimestamp = Instant.ofEpochSecond(timestamp[0])
                .atZone(ZoneId.systemDefault())
                .toLocalDate();

        LocalDate today = LocalDate.now(ZoneId.systemDefault());

        assertEquals(today, dateFromTimestamp, "Timestamp is not from today");
    }

    @Test
    @Order(18)
    void Should_Not_ProcessData_And_Should_Not_Throw_ConcurrentModificationException() throws InterruptedException, IOException, com.solace.semp.v2.monitor.ApiException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_topic","random/topic/1");
                put("solace_includeHeaders", "false");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);
        assertResult(false, "SolacePublishException", 1);
        assertResult(false, "java.util.ConcurrentModificationException", 0);
        MsgVpnQueueResponse msgVpnQueueTxFlowResponse = sempV2Api.monitor().getMsgVpnQueue("default", "Solace/Queue/0", null);
        if (msgVpnQueueTxFlowResponse.getCollections() != null && msgVpnQueueTxFlowResponse.getCollections().getMsgs() != null) {
            System.out.println("Total message in queue " + msgVpnQueueTxFlowResponse.getCollections().getMsgs().getCount());
            assertEquals(100, msgVpnQueueTxFlowResponse.getCollections().getMsgs().getCount(), "Number of messages should be 100");
        }
    }

    @Test
    void Should_Fail_With_Publish_Exception_To_Solace() throws IOException, InterruptedException, com.solace.semp.v2.monitor.ApiException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_topic","publish/deny");
                put("solace_includeHeaders", "false");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);
        assertResult(false, "com.solacesystems.jcsmp.JCSMPErrorResponseException: 403: Publish ACL Denied", 1);
        MsgVpnQueueResponse msgVpnQueueTxFlowResponse = sempV2Api.monitor().getMsgVpnQueue("default", "Solace/Queue/0", null);
        if (msgVpnQueueTxFlowResponse.getCollections() != null && msgVpnQueueTxFlowResponse.getCollections().getMsgs() != null) {
            System.out.println("Total message in queue " + msgVpnQueueTxFlowResponse.getCollections().getMsgs().getCount());
            assertEquals(100, msgVpnQueueTxFlowResponse.getCollections().getMsgs().getCount(), "Number of messages should be 100");
        }
    }

    @Test
    void Should_Fail_Publish_IfMessageIdIsMissing() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_topic","random/topic");
                put("solace_includeHeaders", "false");
                put("solace_id", "__DELETE__");
                put("drop_columns", "Id");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);
        assertResult(false, "Could not find attribute Id either in data frame column or options. Please use id option for setting a id", 1);
    }

    @Test
    void Should_Fail_Publish_IfMessageTopicIsMissing() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_includeHeaders", "false");
                put("solace_topic", "__DELETE__");
                put("drop_columns", "topic");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);
        assertResult(false, "Could not find attribute Topic either in data frame column or options. Please use topic option for setting a topic", 1);
    }

    @Test
    @Order(11)
    void Should_Fail_Publish_IfMessagePayloadIsMissing() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_includeHeaders", "false");
                put("drop_columns", "payload");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);
        assertResult(false, "Payload Column is not present in data frame", 1);
    }

    @Test
    void Should_Fail_Publish_IfSolaceHostIsInvalid() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("only_write", "true");
                put("solace_host", "tcp://invalid-host:55555");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);
        assertResult(false, "com.solacesystems.jcsmp.InvalidPropertiesException: All hosts in the host list: 'tcp://invalid-host:55555' are not resolvable", 1);
    }

    @Test
    void Should_Fail_Publish_IfMandatoryHostIsMissing() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("only_write", "true");
                put("solace_host", "__DELETE__");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);
        assertResult(false, "SolaceSparkConnector - Please provide Solace Host name in configuration options", 1);
    }

    @Test
    void Should_Fail_Publish_IfMandatoryHostIsEmpty() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("only_write", "true");
                put("solace_host", "");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);
        assertResult(false, "SolaceSparkConnector - Please provide Solace Host name in configuration options", 1);
    }

    @Test
    void Should_Fail_Publish_IfMandatoryVpnIsMissing() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("only_write", "true");
                put("solace_vpn", "__DELETE__");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);
        assertResult(false, "SolaceSparkConnector - Please provide Solace VPN name in configuration options", 1);
    }

    @Test
    void Should_Fail_Publish_IfMandatoryVpnIsEmpty() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("only_write", "true");
                put("solace_vpn", "");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);
        assertResult(false, "SolaceSparkConnector - Please provide Solace VPN name in configuration options", 1);
    }

    @Test
    void Should_Fail_Publish_IfMandatoryUsernameIsMissing() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("only_write", "true");
                put("solace_username", "__DELETE__");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);
        assertResult(false, "Please provide Solace Username in configuration options", 1);
    }

    @Test
    void Should_Fail_Publish_IfMandatoryUsernameIsEmpty() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("only_write", "true");
                put("solace_username", "");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);
        assertResult(false, "Please provide Solace Username in configuration options", 1);
    }

    @Test
    void Should_Fail_Publish_IfMandatoryPasswordIsMissing() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("only_write", "true");
                put("solace_password", "__DELETE__");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);
        assertResult(false, "Please provide Solace Password in configuration options", 1);
    }

    @Test
    void Should_Fail_Publish_IfMandatoryPasswordIsEmpty() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("only_write", "true");
                put("solace_password", "");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), true);
        assertResult(false, "Please provide Solace Password in configuration options", 1);
    }

    @Test
    void Should_Fail_Publish_Stream_IfSolaceHostIsInvalid() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("only_write", "true");
                put("solace_host", "tcp://invalid-host:55555");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), false);
        assertResult(false, "com.solacesystems.jcsmp.InvalidPropertiesException: All hosts in the host list: 'tcp://invalid-host:55555' are not resolvable", 1);
    }

    @Test
    void Should_Fail_Publish_Stream_IfMandatoryHostIsMissing() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("only_write", "true");
                put("solace_host", "__DELETE__");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), false);
        assertResult(false, "SolaceSparkConnector - Please provide Solace Host name in configuration options", 1);
    }

    @Test
    void Should_Fail_Publish_Stream_IfMandatoryHostIsEmpty() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("only_write", "true");
                put("solace_host", "");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), false);
        assertResult(false, "SolaceSparkConnector - Please provide Solace Host name in configuration options", 1);
    }

    @Test
    void Should_Fail_Publish_Stream_IfMandatoryVpnIsMissing() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("only_write", "true");
                put("solace_vpn", "__DELETE__");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), false);
        assertResult(false, "SolaceSparkConnector - Please provide Solace VPN name in configuration options", 1);
    }

    @Test
    void Should_Fail_Publish_Stream_IfMandatoryVpnIsEmpty() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("only_write", "true");
                put("solace_vpn", "");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), false);
        assertResult(false, "SolaceSparkConnector - Please provide Solace VPN name in configuration options", 1);
    }

    @Test
    void Should_Fail_Publish_Stream_IfMandatoryUsernameIsMissing() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("only_write", "true");
                put("solace_username", "__DELETE__");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), false);
        assertResult(false, "InvalidPropertiesException: Property (username) is not provided.", 1);
    }

    @Test
    void Should_Fail_Publish_Stream_IfMandatoryUsernameIsEmpty() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("only_write", "true");
                put("solace_username", "");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), false);
        assertResult(false, "InvalidPropertiesException: Property (username) is not provided.", 1);
    }

    @Test
    void Should_Fail_Publish_Stream_IfMandatoryPasswordIsMissing() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("only_write", "true");
                put("solace_password", "__DELETE__");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), false);
        assertResult(false, "com.solacesystems.jcsmp.JCSMPErrorResponseException: 401: Unauthorized", 1);
    }

    @Test
    void Should_Fail_Publish_Stream_IfMandatoryPasswordIsEmpty() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("only_write", "true");
                put("solace_password", "");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString(), false);
        assertResult(false, "com.solacesystems.jcsmp.JCSMPErrorResponseException: 401: Unauthorized", 1);
    }
}
