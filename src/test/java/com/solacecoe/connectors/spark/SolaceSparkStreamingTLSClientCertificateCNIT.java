package com.solacecoe.connectors.spark;

import com.solacecoe.connectors.spark.base.SolaceSession;
import com.solacecoe.connectors.spark.oauth.CertificateContainerResource;
import com.solacecoe.connectors.spark.oauth.SolaceOAuthContainer;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacesystems.jcsmp.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SolaceSparkStreamingTLSClientCertificateCNIT {

    private SparkSession sparkSession;
    private final CertificateContainerResource containerResource = new CertificateContainerResource(true);
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
        Path path3 = Paths.get("src", "test", "resources", "solace.jks");
        Path path4 = Paths.get("src", "test", "resources", "solace_keystore.jks");
        if(Files.exists(path)) {
            FileUtils.deleteDirectory(path.toAbsolutePath().toFile());
        }
        if(Files.exists(path1)) {
            FileUtils.deleteDirectory(path1.toAbsolutePath().toFile());
        }
        if(Files.exists(path2)) {
            FileUtils.deleteDirectory(path2.toAbsolutePath().toFile());
        }

        if(Files.exists(path3)) {
            FileUtils.delete(path3.toAbsolutePath().toFile());
        }

        if(Files.exists(path4)) {
            FileUtils.delete(path4.toAbsolutePath().toFile());
        }
    }

    @Test
    void Should_ConnectUsingClientCertificateCommonName() throws TimeoutException, InterruptedException {
        Path resources = Paths.get("src", "test", "resources");
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path writePath = Paths.get("src", "test", "resources", "spark-checkpoint-3");
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, containerResource.getSolaceOAuthContainer().getOrigin(SolaceOAuthContainer.Service.SMF_SSL))
                .option(SolaceSparkStreamingProperties.VPN, containerResource.getSolaceOAuthContainer().getVpn())
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_CLIENT_CERTIFICATE)
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_TRUST_STORE, resources.toAbsolutePath() + "/solace.jks")
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_TRUST_STORE_FORMAT, "jks")
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_TRUST_STORE_PASSWORD, "password")
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_KEY_STORE, resources.toAbsolutePath() + "/solace_keystore.jks")
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_KEY_STORE_FORMAT, "jks")
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_KEY_STORE_PASSWORD, "password")
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false)
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE_HOST, false)
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE_DATE, false)
                .option(SolaceSparkStreamingProperties.QUEUE, SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_NAME)
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
                .option(SolaceSparkStreamingProperties.USERNAME, "certificate-user")
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_CLIENT_CERTIFICATE)
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_TRUST_STORE, resources.toAbsolutePath() + "/solace.jks")
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_TRUST_STORE_FORMAT, "jks")
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_TRUST_STORE_PASSWORD, "password")
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_KEY_STORE, resources.toAbsolutePath() + "/solace_keystore.jks")
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_KEY_STORE_FORMAT, "jks")
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_KEY_STORE_PASSWORD, "password")
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false)
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE_HOST, false)
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.SSL_VALIDATE_CERTIFICATE_DATE, false)
                .option(SolaceSparkStreamingProperties.MESSAGE_ID, "my-default-id")
                .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
                .option("checkpointLocation", writePath.toAbsolutePath().toString())
                .format("solace").start();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertTrue(count[0] > 0));
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }
}
