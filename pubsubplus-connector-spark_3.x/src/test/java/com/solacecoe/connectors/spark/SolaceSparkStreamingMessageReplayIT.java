package com.solacecoe.connectors.spark;

import com.solace.semp.v2.config.ApiException;
import com.solace.semp.v2.config.client.model.MsgVpnQueue;
import com.solace.semp.v2.config.client.model.MsgVpnQueueSubscription;
import com.solace.semp.v2.config.client.model.MsgVpnReplayLog;
import com.solacecoe.connectors.spark.base.SempV2Api;
import com.solacecoe.connectors.spark.base.SolaceSession;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
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
import org.testcontainers.solace.Service;
import org.testcontainers.solace.SolaceContainer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertThrows;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SolaceSparkStreamingMessageReplayIT {
    private SolaceContainer solaceContainer = new SolaceContainer("solace/solace-pubsub-standard:latest").withExposedPorts(8080, 55555);
    private SparkSession sparkSession;
    private String replicationGroupMessageId = "";
    private String messageTimestamp = "";
    @BeforeAll
    public void beforeAll() throws ApiException {
        solaceContainer.start();
        if(solaceContainer.isRunning()) {
            sparkSession = SparkSession.builder()
                    .appName("data_source_test")
                    .master("local[*]")
                    .getOrCreate();
            sparkSession.sparkContext().setLogLevel("INFO");
            SempV2Api sempV2Api = new SempV2Api(String.format("http://%s:%d", solaceContainer.getHost(), solaceContainer.getMappedPort(8080)), "admin", "admin");
            MsgVpnQueue queue = new MsgVpnQueue();
            queue.queueName("Solace/Queue/0");
            queue.accessType(MsgVpnQueue.AccessTypeEnum.EXCLUSIVE);
            queue.permission(MsgVpnQueue.PermissionEnum.DELETE);
            queue.ingressEnabled(true);
            queue.egressEnabled(true);

            MsgVpnQueue queue1 = new MsgVpnQueue();
            queue1.queueName("Solace/Queue/1");
            queue1.accessType(MsgVpnQueue.AccessTypeEnum.EXCLUSIVE);
            queue1.permission(MsgVpnQueue.PermissionEnum.DELETE);
            queue1.ingressEnabled(true);
            queue1.egressEnabled(true);

            MsgVpnQueue queue2 = new MsgVpnQueue();
            queue2.queueName("Solace/Queue/2");
            queue2.accessType(MsgVpnQueue.AccessTypeEnum.EXCLUSIVE);
            queue2.permission(MsgVpnQueue.PermissionEnum.DELETE);
            queue2.ingressEnabled(true);
            queue2.egressEnabled(true);

            MsgVpnQueue queue3 = new MsgVpnQueue();
            queue3.queueName("Solace/Queue/3");
            queue3.accessType(MsgVpnQueue.AccessTypeEnum.EXCLUSIVE);
            queue3.permission(MsgVpnQueue.PermissionEnum.DELETE);
            queue3.ingressEnabled(true);
            queue3.egressEnabled(true);

            MsgVpnQueueSubscription subscription = new MsgVpnQueueSubscription();
            subscription.setSubscriptionTopic("solace/spark/streaming");

            sempV2Api.config().createMsgVpnQueue("default", queue, null, null);
            sempV2Api.config().createMsgVpnQueueSubscription("default", "Solace/Queue/0", subscription, null, null);

            sempV2Api.config().createMsgVpnQueue("default", queue1, null, null);
            sempV2Api.config().createMsgVpnQueueSubscription("default", "Solace/Queue/1", subscription, null, null);

            sempV2Api.config().createMsgVpnQueue("default", queue2, null, null);
            sempV2Api.config().createMsgVpnQueueSubscription("default", "Solace/Queue/2", subscription, null, null);

            sempV2Api.config().createMsgVpnQueue("default", queue3, null, null);
            sempV2Api.config().createMsgVpnQueueSubscription("default", "Solace/Queue/3", subscription, null, null);

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
        solaceContainer.stop();
    }

    @BeforeEach
    public void beforeEach() throws JCSMPException, ParseException {
        if(solaceContainer.isRunning()) {
            SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
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
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                Date date = new Date(timestamp.getTime());
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                format.setTimeZone(TimeZone.getTimeZone(          // Capture the current moment in the wall-clock time used by the people of a certain region (a time zone).
                        ZoneId.systemDefault()   // Get the JVM’s current default time zone. Can change at any moment during runtime. If important, confirm with the user.
                ));
                textMessage.setSenderTimestamp(format.parse(format.format(date)).getTime());
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
    public void afterEach() throws IOException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path path1 = Paths.get("src", "test", "resources", "spark-checkpoint-2");
        Path path2 = Paths.get("src", "test", "resources", "spark-checkpoint-3");
        Path path3 = Paths.get("src", "test", "resources", "spark-parquet");
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
            FileUtils.deleteDirectory(path3.toAbsolutePath().toFile());
        }
    }

    @Test
    @Order(1)
    void Should_ProcessData() throws TimeoutException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");

        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "10")
                .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES, "1")
                .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES, "1")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().option("checkpointLocation", path.toAbsolutePath().toString())
                .format("parquet").outputMode("append").queryName("SolaceSparkStreaming").option("path", Paths.get("src", "test", "resources", "spark-parquet").toAbsolutePath().toString())
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                    count[0] = count[0] + dataset1.count();
                    replicationGroupMessageId = dataset1.head().getString(0);
                }).start();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100L, count[0]));
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    @Order(2)
    void Should_InitiateReplay_ALL_STRATEGY_And_ProcessData() throws TimeoutException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/1")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option(SolaceSparkStreamingProperties.REPLAY_STRATEGY, "BEGINNING")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            count[0] = count[0] + dataset1.count();
        }).start();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(200L,count[0]));
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    @Order(3)
    void Should_InitiateReplay_TIMEBASED_STRATEGY_And_ProcessData() throws TimeoutException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        String timezone = ZoneId.systemDefault().toString();
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/2")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option(SolaceSparkStreamingProperties.REPLAY_STRATEGY, "TIMEBASED")
                .option(SolaceSparkStreamingProperties.REPLAY_STRATEGY_START_TIME, messageTimestamp)
                .option(SolaceSparkStreamingProperties.REPLAY_STRATEGY_TIMEZONE, timezone)
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            count[0] = count[0] + dataset1.count();
        }).start();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(300L, count[0]));
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();

    }

    @Test
    @Order(4)
    void Should_InitiateReplay_REPLICATIONGROUPMESSAGEID_STRATEGY_And_ProcessData() throws TimeoutException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        System.out.println("Replicationgroupmessage id " + replicationGroupMessageId);
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/3")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option(SolaceSparkStreamingProperties.REPLAY_STRATEGY, "REPLICATION-GROUP-MESSAGE-ID")
                .option(SolaceSparkStreamingProperties.REPLAY_STRATEGY_REPLICATION_GROUP_MESSAGE_ID, replicationGroupMessageId)
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            count[0] = count[0] + dataset1.count();
        }).start();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(400L, count[0]));
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();

    }

    @Test
    @Order(5)
    void Should_Fail_IfReplayStrategyIsInvalid() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES_PER_HOST, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES_WAIT_TIME, 100)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX+"sub_ack_window_threshold", 75)
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "0")
                    .option(SolaceSparkStreamingProperties.REPLAY_STRATEGY, "invalid-replay-strategy")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                System.out.println(dataset1.count());
            }).start();
            streamingQuery.awaitTermination();
        });
    }
}

