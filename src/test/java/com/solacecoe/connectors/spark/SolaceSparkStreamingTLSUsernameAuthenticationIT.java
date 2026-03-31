package com.solacecoe.connectors.spark;

import com.solace.semp.v2.action.ApiException;
import com.solacecoe.connectors.spark.base.SempV2Api;
import com.solacecoe.connectors.spark.base.SolaceSession;
import com.solacecoe.connectors.spark.containers.SparkContainer;
import com.solacecoe.connectors.spark.containers.SparkWorkerContainer;
import com.solacecoe.connectors.spark.containers.oauth.CertificateContainerResource;
import com.solacecoe.connectors.spark.containers.oauth.SolaceOAuthContainer;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.solace.SolaceConnectionManager;
import com.solacesystems.jcsmp.*;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SolaceSparkStreamingTLSUsernameAuthenticationIT {
    private SempV2Api sempV2Api = null;
    private final CertificateContainerResource containerResource = new CertificateContainerResource(false);
    private SparkContainer sparkContainer;
    private SparkWorkerContainer sparkWorkerContainer;
    @BeforeAll
    public void beforeAll() throws IOException {
        containerResource.start();
        if(containerResource.isRunning()) {
            sparkContainer = new SparkContainer(false, true);
            sparkContainer.withFileSystemBind("/tmp/checkpoint", "/opt/spark/checkpoint/solace-spark-connector-integration-test-checkpoint", BindMode.READ_WRITE);
            sparkContainer.start();

            sparkWorkerContainer = new SparkWorkerContainer(false, true);
            sparkWorkerContainer.withFileSystemBind("/tmp/checkpoint", "/opt/spark/checkpoint/solace-spark-connector-integration-test-checkpoint", BindMode.READ_WRITE);
            sparkWorkerContainer.dependsOn(sparkContainer);
            sparkWorkerContainer.start();

            sempV2Api = new SempV2Api(String.format("http://%s:%d", containerResource.getSolaceOAuthContainer().getHost(), containerResource.getSolaceOAuthContainer().getMappedPort(8080)), "admin", "admin");
        } else {
            throw new RuntimeException("Solace Container is not started yet");
        }
    }

    @AfterAll
    public void afterAll() throws IOException {
        sparkContainer.stop();
        sparkWorkerContainer.stop();
        containerResource.stop();

        Path path1 = Paths.get("tmp", "solace.jks");
        Path path2 = Paths.get("tmp", "solace_keystore.jks");

        if(Files.exists(path1)) {
            FileUtils.delete(path1.toAbsolutePath().toFile());
        }

        if(Files.exists(path2)) {
            FileUtils.delete(path2.toAbsolutePath().toFile());
        }
    }

    @BeforeEach
    public void beforeEach() throws JCSMPException {
        if(containerResource.getSolaceOAuthContainer().isRunning()) {
            SolaceSession session = new SolaceSession(containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF), containerResource.getSolaceOAuthContainer().getVpn(), containerResource.getSolaceOAuthContainer().getUsername(), containerResource.getSolaceOAuthContainer().getPassword());
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
                Topic topic = JCSMPFactory.onlyInstance().createTopic(SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_SUBSCRIPTION);
                messageProducer.send(textMessage, topic);
            }

            messageProducer.close();
            session.getSession().closeSession();
        } else {
            throw new RuntimeException("Solace Container is not started yet");
        }
    }

    @AfterEach
    public void afterEach() throws IOException, ApiException {
        sempV2Api.action().doMsgVpnQueueDeleteMsgs("default", SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME, new Object());

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
                        "/opt/spark/work-dir/SolaceSparkSourceTLS.py > /tmp/spark.log 2>&1 &"
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
    void Should_ConnectUsingClientCertificateWithoutPassword() throws TimeoutException, InterruptedException, IOException {
        final long[] count = {0};

        SolaceSession session = new SolaceSession(containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF), containerResource.getSolaceOAuthContainer().getVpn(), containerResource.getSolaceOAuthContainer().getUsername(), containerResource.getSolaceOAuthContainer().getPassword());
        Topic topic = JCSMPFactory.onlyInstance().createTopic("random/topic");
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
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_username","certificate-user");
                put("solace_password","__DELETE__");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));


        executeScript(envVars.toString());
        assertResult(true, null);
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertTrue(count[0] > 0));
    }
}