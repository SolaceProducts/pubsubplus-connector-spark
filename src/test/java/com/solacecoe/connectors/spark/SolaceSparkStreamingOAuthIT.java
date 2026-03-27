package com.solacecoe.connectors.spark;

import com.solace.semp.v2.action.ApiException;
import com.solacecoe.connectors.spark.base.SempV2Api;
import com.solacecoe.connectors.spark.base.SolaceSession;
import com.solacecoe.connectors.spark.containers.SparkContainer;
import com.solacecoe.connectors.spark.containers.SparkWorkerContainer;
import com.solacecoe.connectors.spark.containers.oauth.ContainerResource;
import com.solacecoe.connectors.spark.containers.oauth.SolaceOAuthContainer;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.solace.OAuthClient;
import com.solacesystems.jcsmp.*;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
public class SolaceSparkStreamingOAuthIT {
    private SempV2Api sempV2Api = null;
    private final ContainerResource containerResource = new ContainerResource();
    private SparkContainer sparkContainer;
    private SparkWorkerContainer sparkWorkerContainer;
    @BeforeAll
    public void beforeAll() throws IOException {
        sparkContainer = new SparkContainer(true, false);
        sparkContainer.start();

        sparkWorkerContainer = new SparkWorkerContainer(true, false);
        sparkWorkerContainer.dependsOn(sparkContainer);
        sparkWorkerContainer.start();

        containerResource.start();
        if(containerResource.isRunning()) {
            sempV2Api = new SempV2Api(String.format("http://%s:%d", containerResource.getSolaceOAuthContainer().getHost(), containerResource.getSolaceOAuthContainer().getMappedPort(8080)), "admin", "admin");
        } else {
            throw new RuntimeException("Solace Container is not started yet");
        }
    }

    @AfterAll
    public void afterAll() {
        sparkContainer.stop();
        sparkWorkerContainer.stop();
        containerResource.stop();
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
                        "/opt/spark/work-dir/SolaceSparkSourceOAuth.py > /tmp/spark.log 2>&1 &"
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
    void Should_ConnectToOAuthServer_WithoutValidatingCertificates_And_ProcessData() throws TimeoutException, InterruptedException, IOException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_"+SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL.replace(".", "_"), "5");
                put("solace_queue",SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME);
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(true,null);
    }

    @Test
    @Order(2)
    void Should_ConnectToInSecureOAuthServer_And_ProcessData() throws TimeoutException, InterruptedException, IOException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_"+SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL.replace(".", "_"), "5");
                put("solace_queue",SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME);
                put("oauth_insecure", "true");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(true,null);
    }

    @Test
    @Order(3)
    void Should_ConnectToOAuthServer_AddClientCertificateToDefaultTrustStore_And_ProcessData() throws TimeoutException, InterruptedException, IOException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_"+SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL.replace(".", "_"), "5");
                put("solace_queue",SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME);
                put("add_client_cert", "true");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(true,null);
    }

    @Test
    @Order(4)
    void Should_ConnectToOAuthServer_AddClientCertificateToCustomTrustStore_And_ProcessData() throws TimeoutException, InterruptedException, IOException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_"+SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL.replace(".", "_"), "5");
                put("solace_queue",SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME);
                put("add_client_cert", "true");
                put("add_client_cert_to_custom_truststore", "true");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(true,null);
    }

    @Test
    @Order(5)
    void Should_ReadAccessTokenFromFile_And_ProcessData() throws TimeoutException, IOException, InterruptedException {
        Path resources = Paths.get("src", "test", "resources");

        OAuthClient oAuthClient = new OAuthClient("https://localhost:7778/realms/solace/protocol/openid-connect/token", "solace", "solace-secret");

        oAuthClient.buildRequest(10,
                resources.toAbsolutePath().toString() + "/keycloak.crt",
                null, null, "TLSv1.2",
                "JKS", false);

        String accessToken = oAuthClient.getAccessToken().getValue();
        Files.write(Paths.get(resources.toAbsolutePath().toString(), "accesstoken.txt"), accessToken.getBytes(StandardCharsets.UTF_8));
        sparkContainer.copyFileToContainer(MountableFile.forHostPath(Paths.get(resources.toAbsolutePath().toString(), "accesstoken.txt")), "/opt/spark/work-dir/");
        sparkWorkerContainer.copyFileToContainer(MountableFile.forHostPath(Paths.get(resources.toAbsolutePath().toString(), "accesstoken.txt")), "/opt/spark/work-dir/");

        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_"+SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL.replace(".", "_"), "5");
                put("solace_queue",SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME);
                put("add_access_token_file", "true");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(true,null);

        Files.delete(Paths.get(resources.toAbsolutePath().toString(), "accesstoken.txt"));
    }

    @Test
    @Order(6)
    void Should_ConnectToInSecureOAuthServer_And_ProcessData_And_PublishToSolace() throws TimeoutException, InterruptedException, IOException {
        final long[] count = {0};

        SolaceSession session = new SolaceSession(containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF), containerResource.getSolaceOAuthContainer().getVpn(), containerResource.getSolaceOAuthContainer().getUsername(), containerResource.getSolaceOAuthContainer().getPassword());
        Topic topic = JCSMPFactory.onlyInstance().createTopic("random/topic");
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
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_"+SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL.replace(".", "_"), "5");
                put("solace_queue",SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME);
                put("oauth_insecure", "true");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(true,null);

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> assertEquals(100, count[0]));
    }

    @Test
    void Should_Fail_When_InvalidOAuthUrlIsProvided() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_"+SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL.replace(".", "_"), "5");
                put("solace_queue",SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME);
                put("add_invalid_oauth_url", "true");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false,"IOException: Realm does not exist");

    }

    @Test
    void Should_Fail_When_InvalidTLSVersionProvided() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_"+SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL.replace(".", "_"), "5");
                put("solace_queue",SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME);
                put("add_invalid_tls", "true");
                put("add_client_cert", "true");
                put("add_client_cert_to_custom_truststore", "true");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false,"SolaceSparkConnector - Invalid TLS version invalid");
    }

    @Test
    void Should_Fail_When_TrustStorePasswordIsNull() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_"+SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL.replace(".", "_"), "5");
                put("solace_queue",SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME);
                put("add_client_cert", "true");
                put("add_client_cert_to_custom_truststore", "true");
                put("set_truststore_password_null", "true");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false,"SolaceSparkConnector - Please provide OAuth Client TrustStore Password. If TrustStore file path is not configured, please provide password for default java truststore");
    }

    @Test
    void Should_Fail_When_AccessTokenIsInvalid() throws IOException, InterruptedException {
        Path resources = Paths.get("src", "test", "resources");

        OAuthClient oAuthClient = new OAuthClient("https://localhost:7778/realms/solace/protocol/openid-connect/token", "solace", "solace-secret");

        oAuthClient.buildRequest(10,
                resources.toAbsolutePath().toString() + "/keycloak.crt",
                null, null, "TLSv1.2",
                "JKS", false);

        String accessToken = oAuthClient.getAccessToken().getValue();
        Files.write(Paths.get(resources.toAbsolutePath().toString(), "accesstoken.txt"), accessToken.getBytes(StandardCharsets.UTF_8));
        sparkContainer.copyFileToContainer(MountableFile.forHostPath(Paths.get(resources.toAbsolutePath().toString(), "accesstoken.txt")), "/opt/spark/work-dir/");
        sparkWorkerContainer.copyFileToContainer(MountableFile.forHostPath(Paths.get(resources.toAbsolutePath().toString(), "accesstoken.txt")), "/opt/spark/work-dir/");

        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_"+SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL.replace(".", "_"), "5");
                put("solace_queue",SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME);
                put("add_access_token_file", "true");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(true,null);
        Files.write(Paths.get(resources.toAbsolutePath().toString(), "accesstoken.txt"), "Invalid Token".getBytes(StandardCharsets.UTF_8));
        sparkContainer.copyFileToContainer(MountableFile.forHostPath(Paths.get(resources.toAbsolutePath().toString(), "accesstoken.txt")), "/opt/spark/work-dir/");
        sparkWorkerContainer.copyFileToContainer(MountableFile.forHostPath(Paths.get(resources.toAbsolutePath().toString(), "accesstoken.txt")), "/opt/spark/work-dir/");
        assertResult(false, "JCSMPErrorResponseException: 401: Unauthorized");

        Files.delete(Paths.get(resources.toAbsolutePath().toString(), "accesstoken.txt"));
    }

    @Test
    void Should_Fail_When_MultipleAccessTokensArePresentInFile() throws IOException, InterruptedException {
        Path resources = Paths.get("src", "test", "resources");

        OAuthClient oAuthClient = new OAuthClient("https://localhost:7778/realms/solace/protocol/openid-connect/token", "solace", "solace-secret");

        oAuthClient.buildRequest(10,
                resources.toAbsolutePath().toString() + "/keycloak.crt",
                null, null, "TLSv1.2",
                "JKS", false);

        String accessToken = oAuthClient.getAccessToken().getValue();
        List<String> lines = new ArrayList<>();
        lines.add(accessToken);
        lines.add(accessToken);
        Files.write(Paths.get(resources.toAbsolutePath().toString(), "accesstoken.txt"), lines);
        sparkContainer.copyFileToContainer(MountableFile.forHostPath(Paths.get(resources.toAbsolutePath().toString(), "accesstoken.txt")), "/opt/spark/work-dir/");
        sparkWorkerContainer.copyFileToContainer(MountableFile.forHostPath(Paths.get(resources.toAbsolutePath().toString(), "accesstoken.txt")), "/opt/spark/work-dir/");

        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_"+SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL.replace(".", "_"), "5");
                put("solace_queue",SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME);
                put("add_access_token_file", "true");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false,"File /opt/spark/work-dir/accesstoken.txt is empty or has more than one access token");

        Files.delete(Paths.get(resources.toAbsolutePath().toString(), "accesstoken.txt"));
    }

    @Test
    void Should_Fail_IfMandatoryOAuthURLIsMissing() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_"+SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL.replace(".", "_"), "5");
                put("solace_queue",SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME);
                put("unset_oauth_url", "true");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false,"SolaceSparkConnector - Please provide OAuth Client Authentication Server URL");
    }

    @Test
    void Should_Fail_IfMandatoryOAuthURLIsEmpty() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_"+SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL.replace(".", "_"), "5");
                put("solace_queue",SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME);
                put("set_oauth_url_empty", "true");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false,"SolaceSparkConnector - Please provide OAuth Client Authentication Server URL");
    }

    @Test
    void Should_Fail_IfMandatoryOAuthClientIdIsMissing() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_"+SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL.replace(".", "_"), "5");
                put("solace_queue",SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME);
                put("unset_oauth_client_id", "true");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false,"SolaceSparkConnector - Please provide OAuth Client ID");
    }

    @Test
    void Should_Fail_IfMandatoryOAuthClientIdIsEmpty() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_"+SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL.replace(".", "_"), "5");
                put("solace_queue",SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME);
                put("set_oauth_client_id_empty", "true");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false,"SolaceSparkConnector - Please provide OAuth Client ID");
    }

    @Test
    void Should_Fail_IfMandatoryOAuthClientSecretIsMissing() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_"+SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL.replace(".", "_"), "5");
                put("solace_queue",SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME);
                put("unset_oauth_client_secret", "true");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false,"SolaceSparkConnector - Please provide OAuth Client Credentials Secret");
    }

    @Test
    void Should_Fail_IfMandatoryOAuthClientSecretIsEmpty() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_"+SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL.replace(".", "_"), "5");
                put("solace_queue",SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME);
                put("set_oauth_client_secret_empty", "true");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false,"SolaceSparkConnector - Please provide OAuth Client Credentials Secret");
    }

    @Test
    void Should_Fail_IfAccessTokenFileIsEmpty() throws IOException, InterruptedException {
        Map<String,String> env = new HashMap<String, String>(){
            {
                put("solace_"+SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL.replace(".", "_"), "5");
                put("solace_queue",SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME);
                put("set_access_token_file_empty", "true");
            }
        };

        StringBuilder envVars = new StringBuilder();
        env.forEach((k,v) -> envVars.append(k).append("=").append(v).append(" "));

        executeScript(envVars.toString());
        assertResult(false,"SolaceSparkConnector - Please provide valid access token input");
    }
}
