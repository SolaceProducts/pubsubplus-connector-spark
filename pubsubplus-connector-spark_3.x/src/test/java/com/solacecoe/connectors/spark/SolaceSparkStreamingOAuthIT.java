package com.solacecoe.connectors.spark;

import com.solace.semp.v2.config.ApiException;
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertThrows;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SolaceSparkStreamingOAuthIT {
    private SparkSession sparkSession;
    private ContainerResource containerResource = new ContainerResource();
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
        containerResource.stop();
    }

    @BeforeEach
    public void beforeEach() throws JCSMPException {
        if(containerResource.getSolaceOAuthContainer().isRunning()) {
            SolaceSession session = new SolaceSession(containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF), containerResource.getSolaceOAuthContainer().getVpn(), containerResource.getSolaceOAuthContainer().getUsername(), containerResource.getSolaceOAuthContainer().getPassword());
            XMLMessageProducer messageProducer = session.getSession().getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
                @Override
                public void responseReceivedEx(Object o) {

                }

                @Override
                public void handleErrorEx(Object o, JCSMPException e, long l) {

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

    @Test
    public void testSolaceSparkStreamingDisableOAuthCertValidation() throws TimeoutException, StreamingQueryException {
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
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "10")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        final boolean[] runProcess = {true};
        final Object lock = new Object();
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            synchronized (lock) {
                count[0] = count[0] + dataset1.count();
            }
        }).start();

        ExecutorService executorService = Executors.newFixedThreadPool(1);
        executorService.execute(() -> {
            do {
                if(count[0] == 100L) {
                    runProcess[0] = false;
                    try {
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        streamingQuery.stop();
//                        sparkSession.close();
                        executorService.shutdown();
                    } catch (TimeoutException e) {
                        throw new RuntimeException(e);
                    }
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } while (runProcess[0]);
        });
        streamingQuery.awaitTermination();

//        assertEquals("Number of events consumed from Solace is not equal to number of records written to Spark", 100L, count[0]);
    }

    @Test
    public void testSolaceSparkStreamingInsecureOAuthEndpoint() throws TimeoutException, StreamingQueryException {
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
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "10")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        final boolean[] runProcess = {true};
        final Object lock = new Object();
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            synchronized (lock) {
                count[0] = count[0] + dataset1.count();
            }
        }).start();

        ExecutorService executorService = Executors.newFixedThreadPool(1);
        executorService.execute(() -> {
            do {
                if(count[0] == 100L) {
                    runProcess[0] = false;
                    try {
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        streamingQuery.stop();
//                        sparkSession.close();
                        executorService.shutdown();
                    } catch (TimeoutException e) {
                        throw new RuntimeException(e);
                    }
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } while (runProcess[0]);
        });
        streamingQuery.awaitTermination();

//        assertEquals("Number of events consumed from Solace is not equal to number of records written to Spark", 100L, count[0]);
    }

    @Test
    public void testSolaceSparkStreamingOAuthCertValidation() throws TimeoutException, StreamingQueryException {
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
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "10")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        final boolean[] runProcess = {true};
        final Object lock = new Object();
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            synchronized (lock) {
                count[0] = count[0] + dataset1.count();
            }
        }).start();

        ExecutorService executorService = Executors.newFixedThreadPool(1);
        executorService.execute(() -> {
            do {
                if(count[0] == 100L) {
                    runProcess[0] = false;
                    try {
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        streamingQuery.stop();
//                        sparkSession.close();
                        executorService.shutdown();
                    } catch (TimeoutException e) {
                        throw new RuntimeException(e);
                    }
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } while (runProcess[0]);
        });
        streamingQuery.awaitTermination();

//        assertEquals("Number of events consumed from Solace is not equal to number of records written to Spark", 100L, count[0]);
    }

    @Test
    public void testSolaceSparkStreamingOAuthCertValidationCustomTrustStore() throws TimeoutException, StreamingQueryException {
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
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "10")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        final boolean[] runProcess = {true};
        final Object lock = new Object();
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            synchronized (lock) {
                count[0] = count[0] + dataset1.count();
            }
        }).start();

        ExecutorService executorService = Executors.newFixedThreadPool(1);
        executorService.execute(() -> {
            do {
                if(count[0] == 100L) {
                    runProcess[0] = false;
                    try {
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        Files.delete(Paths.get(resources.toAbsolutePath().toString() + "/custom_truststore.jks"));
                        streamingQuery.stop();
//                        sparkSession.close();
                        executorService.shutdown();
                    } catch (TimeoutException | IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } while (runProcess[0]);
        });
        streamingQuery.awaitTermination();

//        assertEquals("Number of events consumed from Solace is not equal to number of records written to Spark", 100L, count[0]);
    }

    @Test
    public void testSolaceSparkStreamingOAuthFileSource() throws TimeoutException, StreamingQueryException, JCSMPException, IOException {
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
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_ACCESSTOKEN, resources.toAbsolutePath().toString() + "/accesstoken.txt")
                .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL, "50")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "10")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        final boolean[] runProcess = {true};
        final Object lock = new Object();
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            synchronized (lock) {
                count[0] = count[0] + dataset1.count();
            }
        }).start();

        ExecutorService executorService = Executors.newFixedThreadPool(1);
        executorService.execute(() -> {
            do {
                if(count[0] == 100L) {
                    runProcess[0] = false;
                    try {
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        Files.delete(Paths.get(resources.toAbsolutePath().toString() + "/accesstoken.txt"));
                        streamingQuery.stop();
//                        sparkSession.close();
                        executorService.shutdown();
                    } catch (TimeoutException | IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } while (runProcess[0]);
        });
        streamingQuery.awaitTermination();

//        assertEquals("Number of events consumed from Solace is not equal to number of records written to Spark", 100L, count[0]);
    }

    @Test
    public void testSolaceSparkStreamingOAuthFileSourceMultipleAccessToken() throws TimeoutException, StreamingQueryException, JCSMPException, IOException {
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

        assertThrows(RuntimeException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF_SSL))
                    .option(SolaceSparkStreamingProperties.VPN, containerResource.getSolaceOAuthContainer().getVpn())
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false)
                    .option(SolaceSparkStreamingProperties.QUEUE, SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME)
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_ACCESSTOKEN, resources.toAbsolutePath().toString() + "/accesstoken.txt")
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL, "50")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "10")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();

            StreamingQuery streamingQuery = dataset.writeStream().start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    public void testSolaceSparkStreamingDNoOAuthUrl() throws TimeoutException, StreamingQueryException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(RuntimeException.class, () -> {
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
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "10")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();

            StreamingQuery streamingQuery = dataset.writeStream().start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    public void testSolaceSparkStreamingDNoOAuthClientId() throws TimeoutException, StreamingQueryException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(RuntimeException.class, () -> {
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
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "10")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();

            StreamingQuery streamingQuery = dataset.writeStream().start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    public void testSolaceSparkStreamingDNoOAuthClientSecret() throws TimeoutException, StreamingQueryException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(RuntimeException.class, () -> {
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
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "10")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();

            StreamingQuery streamingQuery = dataset.writeStream().start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    public void testSolaceSparkStreamingDNoOAuthAccessTokenFile() throws TimeoutException, StreamingQueryException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(RuntimeException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF_SSL))
                    .option(SolaceSparkStreamingProperties.VPN, containerResource.getSolaceOAuthContainer().getVpn())
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false)
                    .option(SolaceSparkStreamingProperties.QUEUE, SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME)
                    .option(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL, "50")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "10")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();

            StreamingQuery streamingQuery = dataset.writeStream().start();
            streamingQuery.awaitTermination();
        });
    }
}
