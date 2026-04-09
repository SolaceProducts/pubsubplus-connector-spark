package com.solacecoe.connectors.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.solace.semp.v2.config.ApiException;
import com.solace.semp.v2.config.client.model.MsgVpnQueue;
import com.solace.semp.v2.config.client.model.MsgVpnQueueSubscription;
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
import org.testcontainers.solace.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SolaceSparkStreamingSourceIT {
    private SempV2Api sempV2Api = null;
    private SparkContainer sparkContainer;
    private SparkWorkerContainer sparkWorkerContainer;
    private SolaceTestContainer solaceTestContainer;
    @BeforeAll
    public void beforeAll() throws ApiException, IOException {
        sparkContainer = new SparkContainer(false, false);
        sparkContainer.start();

        sparkWorkerContainer = new SparkWorkerContainer(false, false);
        sparkWorkerContainer.dependsOn(sparkContainer);
        sparkWorkerContainer.start();

        Map<String, Service> topics = new HashMap<String, Service>(){
            {
                put("solace/spark/streaming", Service.SMF);
                put("solace/spark/connector/offset", Service.SMF);
                put("solace/spark/streaming/offset", Service.SMF);
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
            queue.setMaxDeliveredUnackedMsgsPerFlow(50L);

            MsgVpnQueueSubscription subscription = new MsgVpnQueueSubscription();
            subscription.setSubscriptionTopic("solace/spark/streaming");

            sempV2Api.config().createMsgVpnQueue("default", queue, null, null);
            sempV2Api.config().createMsgVpnQueueSubscription("default", "Solace/Queue/0", subscription, null, null);

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
                if(i <= 90) {
                    textMessage.setText("Hello Spark!");
                } else if(i <= 95) {
                    textMessage.writeAttachment("Hello Spark!".getBytes(StandardCharsets.UTF_8));
                } else {
                    textMessage.writeBytes("Hello Spark!".getBytes(StandardCharsets.UTF_8));
                }
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
    public void afterEach() throws IOException, InterruptedException {
        sparkContainer.execInContainer(
                "bash",
                "-c",
                "pkill -f spark-submit || true"
        );

        sparkContainer.execInContainer(
                "bash",
                "-c",
                "rm -rf /opt/spark/checkpoint/solace-spark-connector-integration-test-checkpoint"
        );

        sparkContainer.execInContainer(
                "bash",
                "-c",
                "rm -f /tmp/spark.log"
        );
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

    private void assertResult(boolean assertResult, String text) throws InterruptedException, IOException {
        int expectedTotal = 100;
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
    void Should_ProcessData() throws InterruptedException, IOException {
        executeScript("");
        assertResult(true,null);
    }

    @Test
    @Order(2)
    void Should_ProcessSolaceTextMessage() throws InterruptedException, IOException {
        executeScript("");
        assertResult(true,"Write Payload is: Hello Spark!");
    }



    @Test
    @Order(3)
    void Should_CreateMultipleConsumersOnDifferentSessions_And_ProcessData() throws InterruptedException, com.solace.semp.v2.monitor.ApiException, IOException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_partitions","2");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(true,null);
        MsgVpnQueueTxFlowsResponse msgVpnQueueTxFlowResponse = sempV2Api.monitor().getMsgVpnQueueTxFlows("default", "Solace/Queue/0", 10, null, null, null);
        if (msgVpnQueueTxFlowResponse.getData() != null && !msgVpnQueueTxFlowResponse.getData().isEmpty()) {
            assertEquals(2, msgVpnQueueTxFlowResponse.getData().size(), "Number of consumer flows should be 2");
            Assertions.assertNotEquals(msgVpnQueueTxFlowResponse.getData().get(0).getClientName(), msgVpnQueueTxFlowResponse.getData().get(1).getClientName(), "Client Name of two solace sessions should not be the same");
            System.out.println("Total " + msgVpnQueueTxFlowResponse.getData().size() + " consumers with different client names");
        }
//        streamingQuery.stop();
    }

    @Test
    @Order(4)
    void Should_Validate_SolaceSourceMetrics() throws Exception {
        executeScript("");
        assertResult(true,"solaceMetrics");
        String url = String.format("http://%s:%d/metrics/json", sparkContainer.getHost(), sparkContainer.getMappedPort(4040));
        URL obj = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) obj.openConnection();

        connection.setRequestMethod("GET");
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);

        int statusCode = connection.getResponseCode();
        assertEquals(200, statusCode);

        BufferedReader reader = new BufferedReader(
                new InputStreamReader(connection.getInputStream())
        );

        StringBuilder response = new StringBuilder();
        String line;

        while ((line = reader.readLine()) != null) {
            response.append(line);
        }
        reader.close();

        // Parse JSON
        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(response.toString());

        json.get("gauges").fields().forEachRemaining(item -> {
            if(item.getKey().contains("solace")) {
                assertTrue(item.getValue().get("value").has("batchId"));
                assertTrue(item.getValue().get("value").has("acknowledgements"));
                assertTrue(item.getValue().get("value").has("sessionName"));
                assertTrue(item.getValue().get("value").has("pendingAcknowledgements"));
                assertTrue(item.getValue().get("value").has("messagesConsumed"));
            }
        });
    }

    @Test
    void Should_Fail_IfQueueIsUnknown() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_queue","unknown.q");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false, "com.solacesystems.jcsmp.JCSMPErrorResponseException: 503: Unknown Queue");
    }

    @Test
    void Should_Fail_IfSolaceHostIsInvalid() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_host","tcp://invalid-host:55555");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false, "com.solacesystems.jcsmp.InvalidPropertiesException: All hosts in the host list: 'tcp://invalid-host:55555' are not resolvable");
    }

    @Test
    void Should_Fail_IfMandatoryHostIsMissing() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_host", "__DELETE__");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false, "SolaceSparkConnector - Please provide Solace Host name in configuration options");
    }


    @Test
    void Should_Fail_IfMandatoryHostIsEmpty() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_host", "");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false, "SolaceSparkConnector - Please provide Solace Host name in configuration options");
    }

    @Test
    void Should_Fail_IfMandatoryVpnIsMissing() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_vpn", "__DELETE__");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false, "SolaceSparkConnector - Please provide Solace VPN name in configuration options");
    }

    @Test
    void Should_Fail_IfMandatoryVpnIsEmpty() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_vpn", "");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false, "SolaceSparkConnector - Please provide Solace VPN name in configuration options");
    }

    @Test
    void Should_Fail_IfMandatoryUsernameIsMissing() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_username", "__DELETE__");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false, "InvalidPropertiesException: Property (username) is not provided.");
    }

    @Test
    void Should_Fail_IfMandatoryUsernameIsEmpty() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_username", "");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false, "InvalidPropertiesException: Property (username) is not provided.");
    }

    @Test
    void Should_Fail_IfMandatoryPasswordIsMissing() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_password", "__DELETE__");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false, "com.solacesystems.jcsmp.JCSMPErrorResponseException: 401: Unauthorized");
    }

    @Test
    void Should_Fail_IfMandatoryPasswordIsEmpty() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_password", "");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false, "com.solacesystems.jcsmp.JCSMPErrorResponseException: 401: Unauthorized");
    }

    @Test
    void Should_Fail_IfMandatoryQueueIsMissing() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_queue", "__DELETE__");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false, "SolaceSparkConnector - Please provide Solace Queue in configuration options");
    }

    @Test
    void Should_Fail_IfMandatoryQueueIsNull() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_queue", "NULL");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false, "SolaceSparkConnector - Please provide Solace Queue in configuration options");
    }

    @Test
    void Should_Fail_IfMandatoryQueueIsEmpty() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_queue", "");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false, "SolaceSparkConnector - Please provide Solace Queue in configuration options");
    }

    @Test
    void Should_Fail_IfBatchSizeLessThan0() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_batchSize", "-1");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false, "SolaceSparkConnector - Please set batch size greater than zero");
    }

    @Test
    void Should_Fail_IfLVQTopic_Has_No_Permission_To_Publish() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_batchSize", "1");
                put("solace_lvq_topic", "invalid/topic");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false, "com.solacesystems.jcsmp.JCSMPErrorResponseException: 403: Subscription ACL Denied - Queue 'solace.spark.connector.state' - Topic 'invalid/topic'");
    }

    @Test
    void Should_Fail_IfLVQ_Has_No_Permission_To_Access() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_batchSize", "1");
                put("solace_lvq_name", "Solace/Queue/lvq/0");
                put("solace_lvq_topic", "solace/spark/streaming/offset");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false, "com.solacesystems.jcsmp.JCSMPErrorResponseException: 403: Permission Not Allowed - Queue 'Solace/Queue/lvq/0' - Topic 'solace/spark/streaming/offset'");
    }

    @Test
    void Should_Fail_IfLVQ_Has_No_Permission_To_Add_Subscription() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_batchSize", "1");
                put("solace_lvq_name", "Solace/Queue/lvq/0");
                put("solace_lvq_topic", "invalid/topic");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false, "com.solacesystems.jcsmp.JCSMPErrorResponseException: 403: Subscription ACL Denied - Queue 'Solace/Queue/lvq/0' - Topic 'invalid/topic'");
    }
}