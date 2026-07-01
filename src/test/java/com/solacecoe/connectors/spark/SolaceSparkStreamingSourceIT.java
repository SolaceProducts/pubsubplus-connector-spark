package com.solacecoe.connectors.spark;

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
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.solace.Service;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
        Path tempCheckpoint = Paths.get(System.getProperty("java.io.tmpdir"), "checkpoint");
        Files.createDirectories(tempCheckpoint);

        tempCheckpoint.toFile().setWritable(true, false);
        tempCheckpoint.toFile().setReadable(true, false);
        tempCheckpoint.toFile().setExecutable(true, false);

        sparkContainer.withFileSystemBind(System.getProperty("java.io.tmpdir") + "/checkpoint", "/opt/spark/checkpoint/solace-spark-connector-integration-test-checkpoint", BindMode.READ_WRITE);
        sparkContainer.start();

        // Fix mount point permissions inside container immediately after start
        try {
            sparkContainer.execInContainer(
                    "bash", "-c",
                    "chmod -R 777 /opt/spark/checkpoint/solace-spark-connector-integration-test-checkpoint"
            );
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        sparkWorkerContainer = new SparkWorkerContainer(false, false);
        sparkWorkerContainer.withFileSystemBind(System.getProperty("java.io.tmpdir") + "/checkpoint", "/opt/spark/checkpoint/solace-spark-connector-integration-test-checkpoint", BindMode.READ_WRITE);
        sparkWorkerContainer.dependsOn(sparkContainer);
        sparkWorkerContainer.start();

        // Fix permissions on worker container too
        try {
            sparkWorkerContainer.execInContainer(
                    "bash", "-c",
                    "chmod -R 777 /opt/spark/checkpoint/solace-spark-connector-integration-test-checkpoint"
            );
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

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
            publishMessages();
        } else {
            throw new RuntimeException("Solace Container is not started yet");
        }
    }

    private void publishMessages() throws JCSMPException {
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
    }

    @AfterEach
    public void afterEach() throws com.solace.semp.v2.action.ApiException {
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
    void Should_Not_Fail_If_CheckpointMessageId_Comparison_Fails() throws InterruptedException, com.solace.semp.v2.monitor.ApiException, IOException, JCSMPException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_partitions","1");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(true,null);

        sparkContainer.stop();
        sparkWorkerContainer.stop();

        publishMessages();

        // Step 3 — Simulate replication group change by directly modifying
        // the checkpoint with an ID from a different replication group
        try {
            injectDifferentReplicationGroupIdIntoCheckpoint();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        sparkContainer.start();
        sparkWorkerContainer.start();

        StringBuilder envVars1 = new StringBuilder();
        env.put("solace_ackLastProcessedMessages", "true");
        env.put("solace_ignoreCheckpointMessageIdComparisonError", "true");
        env.forEach((k,v) -> envVars1.append(k).append("=").append(v).append(" "));

        executeScript(envVars1.toString());

        assertResult(true, null);

    }

    @Test
    @Order(5)
    void Should_Fail_If_CheckpointMessageId_Comparison_Fails() throws InterruptedException, com.solace.semp.v2.monitor.ApiException, IOException, JCSMPException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_partitions","1");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(true,null);

        sparkContainer.stop();
        sparkWorkerContainer.stop();

        publishMessages();

        // Step 3 — Simulate replication group change by directly modifying
        // the checkpoint with an ID from a different replication group
        try {
            injectDifferentReplicationGroupIdIntoCheckpoint();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        sparkContainer.start();
        sparkWorkerContainer.start();

        StringBuilder envVars1 = new StringBuilder();
        env.put("solace_ackLastProcessedMessages", "true");
        env.forEach((k,v) -> envVars1.append(k).append("=").append(v).append(" "));

        executeScript(envVars1.toString());

        assertResult(false, "Replication Group Message Id are not comparable. Messages must be published to the same broker or HA pair for their Replication Group Message Id to be comparable");

    }

    private void injectDifferentReplicationGroupIdIntoCheckpoint() throws Exception {
        String checkpointDir = System.getProperty("java.io.tmpdir") + "/checkpoint";

        File offsetsDir = new File(checkpointDir + "/offsets");

        if (!offsetsDir.exists() || offsetsDir.listFiles() == null) {
            throw new IllegalStateException(
                    "Checkpoint offsets directory not found: " + offsetsDir.getAbsolutePath());
        }

        File latestOffsetFile = Arrays.stream(offsetsDir.listFiles())
                .filter(File::isFile)
                .filter(f -> !f.getName().endsWith(".crc") && !f.getName().endsWith(".tmp"))
                .max(Comparator.comparingLong(f -> Long.parseLong(f.getName())))
                .orElseThrow(() -> new IllegalStateException(
                        "No offset files found in: " + offsetsDir.getAbsolutePath()));

        String checkpointContent = new String(
                Files.readAllBytes(latestOffsetFile.toPath()),
                StandardCharsets.UTF_8);

        if (checkpointContent.isEmpty()) {
            throw new IllegalStateException(
                    "Checkpoint file is empty: " + latestOffsetFile.getAbsolutePath());
        }

        // Replace only the replication group identifier portion (middle segment)
        // Format: rmid1:<replication-group-id>-<sequence>
        // Example: rmid1:485a1-e2fcad4695f-00000000-007b4fdb
        //                 ^^^^^^^^^^^^^^^^^  ← this is the replication group identifier
        //                                    changing this simulates a different group
        // Use a different but valid hex value to keep the ID parseable by Solace
        String modifiedContent = checkpointContent.replaceAll(
                "rmid1:[a-f0-9]+-[a-f0-9]+-",  // match rmid1:<group-id>-
                "rmid1:99999-aabbccddeef-"      // replace with different group identifier
        );

        if (modifiedContent.equals(checkpointContent)) {
            throw new IllegalStateException(
                    "Could not find replication group message ID pattern in checkpoint. " +
                            "Ensure first batch completed and checkpoint contains messageIDs.");
        }

        Process process = Runtime.getRuntime().exec(new String[]{"sudo", "chmod", "-R", "777", offsetsDir.getAbsolutePath()});
        process.waitFor();

        // Write modified content back
        Files.write(latestOffsetFile.toPath(),
                modifiedContent.getBytes(StandardCharsets.UTF_8));

        // Verify
        String verifiedContent = new String(
                Files.readAllBytes(latestOffsetFile.toPath()),
                StandardCharsets.UTF_8);

        assertTrue(verifiedContent.contains("rmid1:99999-aabbccddeef-"),
                "Checkpoint should contain modified replication group identifier");
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