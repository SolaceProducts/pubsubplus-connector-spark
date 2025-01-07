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
import org.testcontainers.solace.Service;
import org.testcontainers.solace.SolaceContainer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SolaceSparkStreamingMessageReplayIT {
    private SempV2Api sempV2Api = null;
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
            sempV2Api = new SempV2Api(String.format("http://%s:%d", solaceContainer.getHost(), solaceContainer.getMappedPort(8080)), "admin", "admin");
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
        } else {
            throw new RuntimeException("Solace Container is not started yet");
        }
    }

    @AfterAll
    public void afterAll() {
        solaceContainer.stop();
    }

    @BeforeEach
    public void beforeEach() throws JCSMPException {
        if(solaceContainer.isRunning()) {
            SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
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
                textMessage.setSenderTimestamp(System.currentTimeMillis());
                Topic topic = JCSMPFactory.onlyInstance().createTopic("solace/spark/streaming");
                messageProducer.send(textMessage, topic);
            }

            messageProducer.close();
            session.getSession().closeSession();
        } else {
            throw new RuntimeException("Solace Container is not started yet");
        }
    }

    @Test
    @Order(1)
    public void Should_ProcessData() throws TimeoutException, StreamingQueryException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
//        SparkSession sparkSession = SparkSession.builder()
//                .appName("data_source_test")
//                .master("local[*]")
//                .getOrCreate();
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
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
                if(count[0] == 90) {
                    replicationGroupMessageId = dataset1.first().getString(0);
                }
                if(count[0] == 80) {
                    Object obj = dataset1.first().get(4);
                    Timestamp timestamp = (Timestamp) obj;
                    Date date = new Date(timestamp.getTime());
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                    format.setTimeZone(TimeZone.getTimeZone("UTC"));
                    messageTimestamp = format.format(date);
                }
            }
        }).start();

        ExecutorService executorService = Executors.newFixedThreadPool(1);
        executorService.execute(() -> {
            do {
                if(count[0] == 100L) {
                    runProcess[0] = false;
                    try {
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


    }

    @Test
    @Order(2)
    public void Should_InitiateReplay_ALL_STRATEGY_And_ProcessData() throws TimeoutException, StreamingQueryException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
//        SparkSession sparkSession = SparkSession.builder()
//                .appName("data_source_test")
//                .master("local[*]")
//                .getOrCreate();
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "10")
                .option(SolaceSparkStreamingProperties.REPLAY_STRATEGY, "BEGINNING")
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
                if(count[0] == 200L) {
                    runProcess[0] = false;
                    try {
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


    }

    @Test
    @Order(3)
    public void Should_InitiateReplay_TIMEBASED_STRATEGY_And_ProcessData() throws TimeoutException, StreamingQueryException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
//        SparkSession sparkSession = SparkSession.builder()
//                .appName("data_source_test")
//                .master("local[*]")
//                .getOrCreate();
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "10")
                .option(SolaceSparkStreamingProperties.REPLAY_STRATEGY, "TIMEBASED")
                .option(SolaceSparkStreamingProperties.REPLAY_STRATEGY_START_TIME, messageTimestamp)
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
                if(count[0] == 299L) {
                    runProcess[0] = false;
                    try {
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


    }

    @Test
    @Order(4)
    public void Should_InitiateReplay_REPLICATIONGROUPMESSAGEID_STRATEGY_And_ProcessData() throws TimeoutException, StreamingQueryException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
//        SparkSession sparkSession = SparkSession.builder()
//                .appName("data_source_test")
//                .master("local[*]")
//                .getOrCreate();
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "10")
                .option(SolaceSparkStreamingProperties.REPLAY_STRATEGY, "REPLICATION-GROUP-MESSAGE-ID")
                .option(SolaceSparkStreamingProperties.REPLAY_STRATEGY_REPLICATION_GROUP_MESSAGE_ID, replicationGroupMessageId)
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
                if(count[0] == 319L) {
                    runProcess[0] = false;
                    try {
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


    }
}

