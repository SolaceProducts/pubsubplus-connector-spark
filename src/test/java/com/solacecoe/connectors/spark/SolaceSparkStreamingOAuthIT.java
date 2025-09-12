package com.solacecoe.connectors.spark;

import com.solacecoe.connectors.spark.base.SolaceSession;
import com.solacecoe.connectors.spark.oauth.ContainerResource;
import com.solacecoe.connectors.spark.oauth.SolaceOAuthContainer;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.solace.OAuthClient;
import com.solacesystems.jcsmp.*;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SolaceSparkStreamingOAuthIT {
    private SparkSession sparkSession;
    private final ContainerResource containerResource = new ContainerResource();
    @BeforeAll
    public void beforeAll() {
        containerResource.start();
        if(containerResource.isRunning()) {
            sparkSession = SparkSession.builder()
                    .appName("data_source_test")
                    .master("local[*]")
                    .getOrCreate();
        } else {
            throw new RuntimeException("Solace Container is not started yet");
        }
    }

    @AfterAll
    public void afterAll() {
        sparkSession.stop();
        sparkSession.close();
        containerResource.stop();
    }

    @BeforeEach
    public void beforeEach() throws JCSMPException {
        sparkSession = SparkSession.builder()
                .appName("data_source_test")
                .master("local[*]")
                .getOrCreate();

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
    public void afterEach() throws IOException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path path1 = Paths.get("src", "test", "resources", "spark-checkpoint-2");
        Path path2 = Paths.get("src", "test", "resources", "spark-checkpoint-3");
        if(Files.exists(path)) {
            FileUtils.deleteDirectory(path.toAbsolutePath().toFile());
        }
        if(Files.exists(path1)) {
            FileUtils.deleteDirectory(path1.toAbsolutePath().toFile());
        }
        if(Files.exists(path2)) {
            FileUtils.deleteDirectory(path2.toAbsolutePath().toFile());
        }

        sparkSession.stop();
        sparkSession.close();
    }

    @Test
    @Order(1)
    void Should_ConnectToOAuthServer_WithoutValidatingCertificates_And_ProcessData() throws TimeoutException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF_SSL))
                .option(SolaceSparkStreamingProperties.VPN, containerResource.getSolaceOAuthContainer().getVpn())
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false)
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL, "https://localhost:7778/realms/solace/protocol/openid-connect/token")
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID, "solace")
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET, "solace-secret")
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL, "5")
                .option(SolaceSparkStreamingProperties.QUEUE, SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME)
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_SSL_VALIDATE_CERTIFICATE, false)
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        final Object lock = new Object();
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            synchronized (lock) {
                count[0] = count[0] + dataset1.count();
            }
        }).start();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    @Order(2)
    void Should_ConnectToInSecureOAuthServer_And_ProcessData() throws TimeoutException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF_SSL))
                .option(SolaceSparkStreamingProperties.VPN, containerResource.getSolaceOAuthContainer().getVpn())
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false)
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL, "http://localhost:7777/realms/solace/protocol/openid-connect/token")
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID, "solace")
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET, "solace-secret")
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL, "5")
                .option(SolaceSparkStreamingProperties.QUEUE, SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME)
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_SSL_VALIDATE_CERTIFICATE, false)
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        final Object lock = new Object();
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            synchronized (lock) {
                count[0] = count[0] + dataset1.count();
            }
        }).start();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    @Order(3)
    void Should_ConnectToOAuthServer_AddClientCertificateToDefaultTrustStore_And_ProcessData() throws TimeoutException, InterruptedException {
        Path resources = Paths.get("src", "test", "resources");
        Path path = Paths.get(resources.toAbsolutePath().toString(), "spark-checkpoint-1");
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF_SSL))
                .option(SolaceSparkStreamingProperties.VPN, containerResource.getSolaceOAuthContainer().getVpn())
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false)
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL, "https://localhost:7778/realms/solace/protocol/openid-connect/token")
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID, "solace")
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET, "solace-secret")
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL, "5")
                .option(SolaceSparkStreamingProperties.QUEUE, SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME)
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_CLIENT_CERTIFICATE, resources.toAbsolutePath().toString() + "/keycloak.crt")
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_TRUSTSTORE_PASSWORD, "changeit")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        final Object lock = new Object();
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            synchronized (lock) {
                count[0] = count[0] + dataset1.count();
            }
        }).start();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    @Order(4)
    void Should_ConnectToOAuthServer_AddClientCertificateToCustomTrustStore_And_ProcessData() throws TimeoutException, InterruptedException {
        Path resources = Paths.get("src", "test", "resources");
        Path path = Paths.get(resources.toAbsolutePath().toString(), "spark-checkpoint-1");
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF_SSL))
                .option(SolaceSparkStreamingProperties.VPN, containerResource.getSolaceOAuthContainer().getVpn())
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false)
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL, "https://localhost:7778/realms/solace/protocol/openid-connect/token")
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID, "solace")
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET, "solace-secret")
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL, "5")
                .option(SolaceSparkStreamingProperties.QUEUE, SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME)
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_CLIENT_CERTIFICATE, resources.toAbsolutePath().toString() + "/keycloak.crt")
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_TRUSTSTORE_FILE, resources.toAbsolutePath().toString() + "/custom_truststore.jks")
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_TRUSTSTORE_PASSWORD, resources.toAbsolutePath().toString() + "changeit")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        final Object lock = new Object();
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            synchronized (lock) {
                count[0] = count[0] + dataset1.count();
            }
        }).start();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    @Order(5)
    void Should_ReadAccessTokenFromFile_And_ProcessData() throws TimeoutException, IOException, InterruptedException {
        Path resources = Paths.get("src", "test", "resources");
        Path path = Paths.get(resources.toAbsolutePath().toString(), "spark-checkpoint-1");

        OAuthClient oAuthClient = new OAuthClient("https://localhost:7778/realms/solace/protocol/openid-connect/token", "solace", "solace-secret");

        oAuthClient.buildRequest(10,
                resources.toAbsolutePath().toString() + "/keycloak.crt",
                null, null, "TLSv1.2",
                "JKS", false);

        String accessToken = oAuthClient.getAccessToken().getValue();
        Files.write(Paths.get(resources.toAbsolutePath().toString(), "accesstoken.txt"), accessToken.getBytes(StandardCharsets.UTF_8));

        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF_SSL))
                .option(SolaceSparkStreamingProperties.VPN, containerResource.getSolaceOAuthContainer().getVpn())
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false)
                .option(SolaceSparkStreamingProperties.QUEUE, SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME)
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_ACCESSTOKEN, resources.toAbsolutePath() + "/accesstoken.txt")
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL, "50")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "100")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        final Object lock = new Object();
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            synchronized (lock) {
                count[0] = count[0] + dataset1.count();
            }
        }).start();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    @Order(6)
    void Should_ConnectToInSecureOAuthServer_And_ProcessData_And_PublishToSolace() throws TimeoutException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path writePath = Paths.get("src", "test", "resources", "spark-checkpoint-3");
//        sparkSession.sparkContext().setLogLevel("TRACE");
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF_SSL))
                .option(SolaceSparkStreamingProperties.VPN, containerResource.getSolaceOAuthContainer().getVpn())
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false)
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL, "http://localhost:7777/realms/solace/protocol/openid-connect/token")
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID, "solace")
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET, "solace-secret")
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL, "50")
                .option(SolaceSparkStreamingProperties.QUEUE, SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME)
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_SSL_VALIDATE_CERTIFICATE, false)
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "100")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        Dataset<Row> dataset = reader.load();

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

        StreamingQuery streamingQuery = dataset.writeStream().option(SolaceSparkStreamingProperties.HOST, containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF_SSL))
                .option(SolaceSparkStreamingProperties.VPN, containerResource.getSolaceOAuthContainer().getVpn())
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false)
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL, "http://localhost:7777/realms/solace/protocol/openid-connect/token")
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID, "solace")
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET, "solace-secret")
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL, "50")
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_SSL_VALIDATE_CERTIFICATE, false)
                .option(SolaceSparkStreamingProperties.MESSAGE_ID, "my-default-id")
                .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
                .option("checkpointLocation", writePath.toAbsolutePath().toString())
                .format("solace").start();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> assertTrue(count[0] > 0));
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
        sparkSession.stop();
    }

    @Test
    void Should_Fail_When_InvalidOAuthUrlIsProvided() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF_SSL))
                    .option(SolaceSparkStreamingProperties.VPN, containerResource.getSolaceOAuthContainer().getVpn())
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false)
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL, "http://localhost:7777/realms/fail/protocol/openid-connect/token")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID, "solace")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET, "solace-secret")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL, "5")
                    .option(SolaceSparkStreamingProperties.QUEUE, SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME)
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_SSL_VALIDATE_CERTIFICATE, false)
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();

            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {}).start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    void Should_Fail_When_InvalidTLSVersionProvided() {
        Path resources = Paths.get("src", "test", "resources");
        Path path = Paths.get(resources.toAbsolutePath().toString(), "spark-checkpoint-1");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF_SSL))
                    .option(SolaceSparkStreamingProperties.VPN, containerResource.getSolaceOAuthContainer().getVpn())
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false)
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL, "https://localhost:7778/realms/solace/protocol/openid-connect/token")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID, "solace")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET, "solace-secret")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL, "5")
                    .option(SolaceSparkStreamingProperties.QUEUE, SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME)
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_CLIENT_CERTIFICATE, resources.toAbsolutePath().toString() + "/keycloak.crt")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_TRUSTSTORE_PASSWORD, "changeit")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_TLS_VERSION, "invalidtls")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();

            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {}).start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    void Should_Fail_When_TrustStorePasswordIsNull() {
        Path resources = Paths.get("src", "test", "resources");
        Path path = Paths.get(resources.toAbsolutePath().toString(), "spark-checkpoint-1");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF_SSL))
                    .option(SolaceSparkStreamingProperties.VPN, containerResource.getSolaceOAuthContainer().getVpn())
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false)
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL, "https://localhost:7778/realms/solace/protocol/openid-connect/token")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID, "solace")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET, "solace-secret")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL, "5")
                    .option(SolaceSparkStreamingProperties.QUEUE, SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME)
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_CLIENT_CERTIFICATE, resources.toAbsolutePath().toString() + "/keycloak.crt")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_TRUSTSTORE_FILE, resources.toAbsolutePath().toString() + "/custom_truststore.jks")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();

            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {}).start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    void Should_Fail_When_AccessTokenIsInvalid() throws IOException {
        Path resources = Paths.get("src", "test", "resources");
        Path path = Paths.get(resources.toAbsolutePath().toString(), "spark-checkpoint-1");

        OAuthClient oAuthClient = new OAuthClient("https://localhost:7778/realms/solace/protocol/openid-connect/token", "solace", "solace-secret");

        oAuthClient.buildRequest(10,
                resources.toAbsolutePath().toString() + "/keycloak.crt",
                null, null, "TLSv1.2",
                "JKS", false);

        String accessToken = oAuthClient.getAccessToken().getValue();
        Files.write(Paths.get(resources.toAbsolutePath().toString(), "accesstoken.txt"), accessToken.getBytes(StandardCharsets.UTF_8));

//        assertThrows(StreamingQueryException.class, () -> {
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF_SSL))
                    .option(SolaceSparkStreamingProperties.VPN, containerResource.getSolaceOAuthContainer().getVpn())
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false)
                    .option(SolaceSparkStreamingProperties.QUEUE, SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME)
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_ACCESSTOKEN, resources.toAbsolutePath().toString() + "/accesstoken.txt")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL, "5")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "1")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            final long[] count = {0};
            final Object lock = new Object();
            Dataset<Row> dataset = reader.load();

            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                synchronized (lock) {
                    count[0] = count[0] + dataset1.count();
                }
                Files.write(Paths.get(resources.toAbsolutePath().toString(), "accesstoken.txt"), "Invalid Token".getBytes(StandardCharsets.UTF_8));
            }).start();
            streamingQuery.awaitTermination();
        } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
//        });

        Files.delete(Paths.get(resources.toAbsolutePath().toString(), "accesstoken.txt"));
    }

    @Test
    void Should_Fail_When_MultipleAccessTokensArePresentInFile() throws IOException {
        Path resources = Paths.get("src", "test", "resources");
        Path path = Paths.get(resources.toAbsolutePath().toString(), "spark-checkpoint-1");

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

        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF_SSL))
                    .option(SolaceSparkStreamingProperties.VPN, containerResource.getSolaceOAuthContainer().getVpn())
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false)
                    .option(SolaceSparkStreamingProperties.QUEUE, SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME)
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_ACCESSTOKEN, resources.toAbsolutePath().toString() + "/accesstoken.txt")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL, "50")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "5")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();

            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            }).start();
            streamingQuery.awaitTermination();
        });

        Files.delete(Paths.get(resources.toAbsolutePath().toString(), "accesstoken.txt"));
    }

    @Test
    void Should_Fail_IfMandatoryOAuthURLIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF_SSL))
                    .option(SolaceSparkStreamingProperties.VPN, containerResource.getSolaceOAuthContainer().getVpn())
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false)
//                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL, "https://localhost:7778/realms/solace/protocol/openid-connect/token")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID, "solace")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET, "solace-secret")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL, "5")
                    .option(SolaceSparkStreamingProperties.QUEUE, SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME)
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_SSL_VALIDATE_CERTIFICATE, false)
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "5")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();

            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            }).start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    void Should_Fail_IfMandatoryOAuthURLIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF_SSL))
                    .option(SolaceSparkStreamingProperties.VPN, containerResource.getSolaceOAuthContainer().getVpn())
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false)
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL, "")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID, "solace")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET, "solace-secret")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL, "5")
                    .option(SolaceSparkStreamingProperties.QUEUE, SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME)
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_SSL_VALIDATE_CERTIFICATE, false)
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "5")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();

            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            }).start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    void Should_Fail_IfMandatoryOAuthClientIdIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF_SSL))
                    .option(SolaceSparkStreamingProperties.VPN, containerResource.getSolaceOAuthContainer().getVpn())
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false)
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL, "https://localhost:7778/realms/solace/protocol/openid-connect/token")
//                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID, "solace")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET, "solace-secret")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL, "5")
                    .option(SolaceSparkStreamingProperties.QUEUE, SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME)
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_SSL_VALIDATE_CERTIFICATE, false)
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "5")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();

            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            }).start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    void Should_Fail_IfMandatoryOAuthClientIdIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF_SSL))
                    .option(SolaceSparkStreamingProperties.VPN, containerResource.getSolaceOAuthContainer().getVpn())
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false)
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL, "https://localhost:7778/realms/solace/protocol/openid-connect/token")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID, "")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET, "solace-secret")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL, "5")
                    .option(SolaceSparkStreamingProperties.QUEUE, SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME)
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_SSL_VALIDATE_CERTIFICATE, false)
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "5")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();

            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            }).start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    void Should_Fail_IfMandatoryOAuthClientSecretIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF_SSL))
                    .option(SolaceSparkStreamingProperties.VPN, containerResource.getSolaceOAuthContainer().getVpn())
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false)
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL, "https://localhost:7778/realms/solace/protocol/openid-connect/token")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID, "solace")
//                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET, "solace-secret")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL, "5")
                    .option(SolaceSparkStreamingProperties.QUEUE, SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME)
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_SSL_VALIDATE_CERTIFICATE, false)
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "5")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();

            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            }).start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    void Should_Fail_IfMandatoryOAuthClientSecretIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF_SSL))
                    .option(SolaceSparkStreamingProperties.VPN, containerResource.getSolaceOAuthContainer().getVpn())
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false)
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL, "https://localhost:7778/realms/solace/protocol/openid-connect/token")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID, "solace")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET, "")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL, "5")
                    .option(SolaceSparkStreamingProperties.QUEUE, SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME)
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_SSL_VALIDATE_CERTIFICATE, false)
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "5")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();

            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            }).start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    void Should_Fail_IfAccessTokenFileIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF_SSL))
                    .option(SolaceSparkStreamingProperties.VPN, containerResource.getSolaceOAuthContainer().getVpn())
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false)
                    .option(SolaceSparkStreamingProperties.QUEUE, SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME)
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_ACCESSTOKEN, "")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL, "50")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "5")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();

            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            }).start();
            streamingQuery.awaitTermination();
        });
    }
}
