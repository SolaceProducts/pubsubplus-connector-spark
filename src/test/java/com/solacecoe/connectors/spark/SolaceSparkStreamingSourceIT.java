package com.solacecoe.connectors.spark;

import com.github.dockerjava.api.model.Ulimit;
import com.solace.semp.v2.config.ApiException;
import com.solace.semp.v2.config.client.model.MsgVpnQueue;
import com.solace.semp.v2.config.client.model.MsgVpnQueueSubscription;
import com.solace.semp.v2.monitor.client.model.MsgVpnQueueTxFlowsResponse;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SolaceSparkStreamingSourceIT {
    private SempV2Api sempV2Api = null;
    private static final Long SHM_SIZE = (long) Math.pow(1024, 3);
    private SolaceContainer solaceContainer = new SolaceContainer("solace/solace-pubsub-standard:latest").withCreateContainerCmdModifier(cmd ->{
        Ulimit ulimit = new Ulimit("nofile", 2448, 1048576);
        List<Ulimit> ulimitList = new ArrayList<>();
        ulimitList.add(ulimit);
        cmd.getHostConfig()
                .withShmSize(SHM_SIZE)
                .withUlimits(ulimitList)
                .withCpuCount(1l);
    }).withExposedPorts(8080, 55555).withTopic("solace/spark/streaming", Service.SMF).withTopic("solace/spark/connector/offset", Service.SMF).withTopic("solace/spark/streaming/offset", Service.SMF);
    private SparkSession sparkSession;
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

            MsgVpnQueue lvq = new MsgVpnQueue();
            lvq.queueName("Solace/Queue/lvq/0");
            lvq.accessType(MsgVpnQueue.AccessTypeEnum.EXCLUSIVE);
            lvq.permission(MsgVpnQueue.PermissionEnum.NO_ACCESS);
            lvq.setOwner("default");
            lvq.ingressEnabled(true);
            lvq.egressEnabled(true);
            lvq.setMaxMsgSpoolUsage(0l);

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
        solaceContainer.stop();
    }

    @BeforeEach
    public void beforeEach() throws JCSMPException {
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
        Thread.sleep(3000);
        streamingQuery.stop();
    }

    @Test
    @Order(2)
    void Should_ProcessData_With_CustomOffsetIndicator() throws TimeoutException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option(SolaceSparkStreamingProperties.OFFSET_INDICATOR, "custom-sequence")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final Long[] count = {0L};
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            count[0] = count[0] + dataset1.count();
        }).start();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals(100, count[0]));
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    @Order(3)
    void Should_ProcessSolaceTextMessage() throws TimeoutException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");

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
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        Dataset<Row> dataset = reader.load();
        final long[] count = {0};
        AtomicReference<String> payload = new AtomicReference<>("");
        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            if(payload.get().isEmpty() && !dataset1.isEmpty()) {
                String msg = dataset1.select(dataset1.col("Payload").cast("string")).collectAsList().get(0).getString(0);
                payload.set(msg);
            } else {
                count[0] = count[0] + dataset1.count();
            }
        }).start();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertEquals("Hello Spark!", payload.get()));
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }



    @Test
    @Order(4)
    void Should_CreateMultipleConsumersOnDifferentSessions_And_ProcessData() throws TimeoutException, InterruptedException, com.solace.semp.v2.monitor.ApiException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-2");
        DataStreamReader reader = sparkSession.readStream()
                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .option(SolaceSparkStreamingProperties.PARTITIONS, 2)
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
        MsgVpnQueueTxFlowsResponse msgVpnQueueTxFlowResponse =  sempV2Api.monitor().getMsgVpnQueueTxFlows("default", "Solace/Queue/0", 10, null, null, null);
        if(msgVpnQueueTxFlowResponse.getData() != null && !msgVpnQueueTxFlowResponse.getData().isEmpty()) {
            Assertions.assertEquals(2, msgVpnQueueTxFlowResponse.getData().size(), "Number of consumer flows should be 2");
            Assertions.assertNotEquals(msgVpnQueueTxFlowResponse.getData().get(0).getClientName(), msgVpnQueueTxFlowResponse.getData().get(1).getClientName(), "Client Name of two solace sessions should not be the same");
        }
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    void Should_Fail_IfQueueIsUnknown() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Unknown.Q")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "1")
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                long count = dataset1.count();
            }).start();
            streamingQuery.awaitTermination();
        } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_IfSolaceHostIsInvalid() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
//        assertThrows(StreamingQueryException.class, () -> {
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, "tcp://invalid-host:55555")
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES_PER_HOST, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES_WAIT_TIME, 100)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + "sub_ack_window_threshold", 75)
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "1")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                long count = dataset1.count();
            }).start();
            streamingQuery.awaitTermination();
        }  catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
//        });
    }

    @Test
    void Should_Fail_If_CorrelationIdOffsetIsInvalid() {
        test_custom_offset_indicator("CORRELATION_ID");
        test_custom_offset_indicator("APPLICATION_MESSAGE_ID");
        test_custom_offset_indicator("invalid-message-property");
        test_custom_offset_indicator("null-custom-property");
        test_custom_offset_indicator("empty-custom-property");
    }



    void test_custom_offset_indicator(String offsetIndicator) {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
//        assertThrows(StreamingQueryException.class, () -> {
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES_PER_HOST, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES_WAIT_TIME, 100)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + "sub_ack_window_threshold", 75)
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "1")
                    .option(SolaceSparkStreamingProperties.OFFSET_INDICATOR, offsetIndicator)
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                long count = dataset1.count();
            }).start();
            streamingQuery.awaitTermination();
        } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
//        });
    }

    @Test
    void Should_Fail_IfMandatoryHostIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
//        assertThrows(StreamingQueryException.class, () -> {
        try {
            DataStreamReader reader = sparkSession.readStream()
//                    .option(SolaceSparkStreamingProperties.HOST, "tcp://invalid-host:55555")
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES_PER_HOST, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES_WAIT_TIME, 100)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + "sub_ack_window_threshold", 75)
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "1")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                System.out.println(dataset1.count());
            }).start();
            streamingQuery.awaitTermination();
        }  catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
//        });
    }


    @Test
    void Should_Fail_IfMandatoryHostIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
//        assertThrows(StreamingQueryException.class, () -> {
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, "")
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES_PER_HOST, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES_WAIT_TIME, 100)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + "sub_ack_window_threshold", 75)
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "1")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                System.out.println(dataset1.count());
            }).start();
            streamingQuery.awaitTermination();
        } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
//        });
    }

    @Test
    void Should_Fail_IfMandatoryVpnIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
//        assertThrows(StreamingQueryException.class, () -> {
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
//                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES_PER_HOST, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES_WAIT_TIME, 100)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + "sub_ack_window_threshold", 75)
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "1")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                System.out.println(dataset1.count());
            }).start();
            streamingQuery.awaitTermination();
        } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
//        });
    }

    @Test
    void Should_Fail_IfMandatoryVpnIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
//        assertThrows(StreamingQueryException.class, () -> {
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, "")
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES_PER_HOST, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES_WAIT_TIME, 100)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + "sub_ack_window_threshold", 75)
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "1")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                System.out.println(dataset1.count());
            }).start();
            streamingQuery.awaitTermination();
        } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
//        });
    }

    @Test
    void Should_Fail_IfMandatoryUsernameIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
//        assertThrows(StreamingQueryException.class, () -> {
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
//                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES_PER_HOST, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES_WAIT_TIME, 100)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + "sub_ack_window_threshold", 75)
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "1")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                System.out.println(dataset1.count());
            }).start();
            streamingQuery.awaitTermination();
        }  catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
//        });
    }

    @Test
    void Should_Fail_IfMandatoryUsernameIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
//        assertThrows(StreamingQueryException.class, () -> {
        try {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, "")
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES_PER_HOST, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES_WAIT_TIME, 100)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + "sub_ack_window_threshold", 75)
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "1")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                System.out.println(dataset1.count());
            }).start();
            streamingQuery.awaitTermination();
        } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
//        });
    }

    @Test
    void Should_Fail_IfMandatoryPasswordIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        try{
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
//                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES_PER_HOST, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES_WAIT_TIME, 100)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX+"sub_ack_window_threshold", 75)
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "1")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                System.out.println(dataset1.count());
            }).start();
            streamingQuery.awaitTermination();
        } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_IfMandatoryPasswordIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        try{
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, "")
                    .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES_PER_HOST, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES_WAIT_TIME, 100)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX+"sub_ack_window_threshold", 75)
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "1")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                System.out.println(dataset1.count());
            }).start();
            streamingQuery.awaitTermination();
        } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_IfMandatoryQueueIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        try{
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
//                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "1")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                System.out.println(dataset1.count());
            }).start();
            streamingQuery.awaitTermination();
        } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_IfMandatoryQueueIsNull() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        try{
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
                    .option(SolaceSparkStreamingProperties.QUEUE, null)
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "1")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                System.out.println(dataset1.count());
            }).start();
            streamingQuery.awaitTermination();
        } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_IfMandatoryQueueIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        try{
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "")
                    .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES_PER_HOST, 1)
                    .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES_WAIT_TIME, 100)
                    .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX+"sub_ack_window_threshold", 75)
//                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "1")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                System.out.println(dataset1.count());
            }).start();
            streamingQuery.awaitTermination();
        } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_IfBatchSizeLessThan0() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        try{
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
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "-1")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                System.out.println(dataset1.count());
            }).start();
            streamingQuery.awaitTermination();
        } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_IfLVQTopic_Has_No_Permission_To_Publish() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        try{
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
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "1")
                    .option(SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_TOPIC, "invalid/topic")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                System.out.println(dataset1.count());
            }).start();
            streamingQuery.awaitTermination();
        } catch (Exception e) {
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_IfLVQ_Has_No_Permission_To_Access() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        try{
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
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "1")
                    .option(SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_NAME, "Solace/Queue/lvq/0")
                    .option(SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_TOPIC, "solace/spark/streaming/offset")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                System.out.println(dataset1.count());
            }).start();
            streamingQuery.awaitTermination();
        } catch (Exception e) {
            System.out.println(e);
            assertTrue(e instanceof StreamingQueryException);
        }
    }

    @Test
    void Should_Fail_IfLVQ_Has_No_Permission_To_Add_Subscription() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        try{
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
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "1")
                    .option(SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_NAME, "Solace/Queue/lvq/0")
                    .option(SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_TOPIC, "invalid/topic")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                System.out.println(dataset1.count());
            }).start();
            streamingQuery.awaitTermination();
        } catch (Exception e) {
            System.out.println(e);
            assertTrue(e instanceof StreamingQueryException);
        }
    }
}