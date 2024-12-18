package com.solacecoe.connectors.spark;

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
import org.apache.spark.sql.SaveMode;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertThrows;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SolaceSparkStreamingSinkIT {
    private SempV2Api sempV2Api = null;
    private SolaceContainer solaceContainer = new SolaceContainer("solace/solace-pubsub-standard:latest").withExposedPorts(8080, 55555);
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
                textMessage.setPriority(1);
//                textMessage.setCorrelationId("test-correlation-id");
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
        sparkSession.close();
        sparkSession.stop();
        SparkSession.clearDefaultSession();
        SparkSession.clearActiveSession();
        sparkSession = SparkSession.builder()
                .appName("data_source_test")
                .master("local[*]")
                .getOrCreate();
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
    public void Should_ProcessData_And_Publish_As_Stream_To_Solace() throws TimeoutException, StreamingQueryException, InterruptedException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path writePath = Paths.get("src", "test", "resources", "spark-checkpoint-3");
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
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        final boolean[] runProcess = {true};
        final String[] messageId = {""};
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
//                .option(SolaceSparkStreamingProperties.BATCH_SIZE, dataset.count())
                .option(SolaceSparkStreamingProperties.MESSAGE_ID, "my-default-id")
                .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
                .option("checkpointLocation", writePath.toAbsolutePath().toString())
//                .mode(SaveMode.Append)
                .format("solace").start();

        SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
        Topic topic = JCSMPFactory.onlyInstance().createTopic("random/topic");
        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                    if(count[0] == 100) {
                        messageId[0] = bytesXMLMessage.getApplicationMessageId();
                    }
                }

                @Override
                public void onException(JCSMPException e) {

                }
            });
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

//        ExecutorService executorService = Executors.newFixedThreadPool(1);
//        executorService.execute(() -> {
//            do {
//                if(count[0] == 100L) {
//                    runProcess[0] = false;
//                    try {
//                        Assertions.assertEquals("my-default-id", messageId[0], "MessageId mismatch");
//                        streamingQuery.stop();
////                        sparkSession.close();
//                        executorService.shutdown();
//                    } catch (TimeoutException e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//                try {
//                    Thread.sleep(100);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//            } while (runProcess[0]);
//        });
//        streamingQuery.awaitTermination();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> count[0] == 100);
        Assertions.assertEquals("my-default-id", messageId[0], "MessageId mismatch");
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    public void Should_ProcessData_And_Publish_With_CustomId_To_Solace() throws TimeoutException, StreamingQueryException, InterruptedException {
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
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        final boolean[] runProcess = {true};
        final String[] messageId = {""};
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            dataset1.write()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, dataset1.count())
                    .option(SolaceSparkStreamingProperties.MESSAGE_ID, "my-default-id")
                    .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
                    .mode(SaveMode.Append)
                    .format("solace").save();
        }).start();

        SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
        Topic topic = JCSMPFactory.onlyInstance().createTopic("random/topic");
        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                    if(count[0] == 100) {
                        messageId[0] = bytesXMLMessage.getApplicationMessageId();
                    }
                }

                @Override
                public void onException(JCSMPException e) {

                }
            });
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

//        ExecutorService executorService = Executors.newFixedThreadPool(1);
//        executorService.execute(() -> {
//            do {
//                if(count[0] == 100L) {
//                    runProcess[0] = false;
//                    try {
//                        Assertions.assertEquals("my-default-id", messageId[0], "MessageId mismatch");
//                        streamingQuery.stop();
////                        sparkSession.close();
//                        executorService.shutdown();
//                    } catch (TimeoutException e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//                try {
//                    Thread.sleep(100);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//            } while (runProcess[0]);
//        });
//        streamingQuery.awaitTermination();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> count[0] == 100);
        Assertions.assertEquals("my-default-id", messageId[0], "MessageId mismatch");
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    public void Should_ProcessData_And_Publish_With_DataFrameId_To_Solace() throws TimeoutException, StreamingQueryException, InterruptedException {
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
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        final boolean[] runProcess = {true};
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            dataset1.write()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, dataset1.count())
                    .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
                    .mode(SaveMode.Append)
                    .format("solace").save();
        }).start();

        SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
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

                }
            });
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

//        ExecutorService executorService = Executors.newFixedThreadPool(1);
//        executorService.execute(() -> {
//            do {
//                if(count[0] == 100L) {
//                    runProcess[0] = false;
//                    try {
//                        streamingQuery.stop();
////                        sparkSession.close();
//                        executorService.shutdown();
//                    } catch (TimeoutException e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//                try {
//                    Thread.sleep(100);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//            } while (runProcess[0]);
//        });
//        streamingQuery.awaitTermination();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> count[0] == 100);
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

//    @Test
//    public void Should_ProcessData_And_Publish_To_DataFrameTopic_Solace() throws TimeoutException, StreamingQueryException, InterruptedException {
//        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
////        SparkSession sparkSession = SparkSession.builder()
////                .appName("data_source_test")
////                .master("local[*]")
////                .getOrCreate();
//        DataStreamReader reader = sparkSession.readStream()
//                .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
//                .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
//                .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
//                .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
//                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
//                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
//                .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
//                .option("checkpointLocation", path.toAbsolutePath().toString())
//                .format("solace");
//        final long[] count = {0};
//        final boolean[] runProcess = {true};
//        Dataset<Row> dataset = reader.load();
//
//        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
//            dataset1.write()
//                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
//                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
//                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
//                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
//                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, dataset1.count())
//                    .mode(SaveMode.Append)
//                    .format("solace").save();
//        }).start();
//
//        SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
//        Topic topic = JCSMPFactory.onlyInstance().createTopic("solace/spark/streaming");
//        XMLMessageConsumer messageConsumer = null;
//        try {
//            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
//                @Override
//                public void onReceive(BytesXMLMessage bytesXMLMessage) {
//                    count[0] = count[0] + 1;
//                }
//
//                @Override
//                public void onException(JCSMPException e) {
//
//                }
//            });
//            session.getSession().addSubscription(topic);
//            messageConsumer.start();
//        } catch (JCSMPException e) {
//            throw new RuntimeException(e);
//        }
//
////        ExecutorService executorService = Executors.newFixedThreadPool(1);
////        executorService.execute(() -> {
////            do {
////                if(count[0] == 100L) {
////                    runProcess[0] = false;
////                    try {
////                        streamingQuery.stop();
//////                        sparkSession.close();
////                        executorService.shutdown();
////                    } catch (TimeoutException e) {
////                        throw new RuntimeException(e);
////                    }
////                }
////                try {
////                    Thread.sleep(100);
////                } catch (InterruptedException e) {
////                    throw new RuntimeException(e);
////                }
////            } while (runProcess[0]);
////        });
////        streamingQuery.awaitTermination();
//
//        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> count[0] == 100);
//        Thread.sleep(3000); // add timeout to ack messages on queue
//        streamingQuery.stop();
//    }

    @Test
    public void Should_ProcessData_And_Publish_To_CustomTopic_Solace() throws TimeoutException, StreamingQueryException, InterruptedException {
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
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        final boolean[] runProcess = {true};
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
              dataset1.write()
                      .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                      .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                      .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                      .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                      .option(SolaceSparkStreamingProperties.BATCH_SIZE, dataset1.count())
                      .option(SolaceSparkStreamingProperties.TOPIC, "Spark/Topic/0")
                      .mode(SaveMode.Append)
                      .format("solace").save();
        }).start();

        SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
        Topic topic = JCSMPFactory.onlyInstance().createTopic("Spark/Topic/0");
        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                }

                @Override
                public void onException(JCSMPException e) {

                }
            });
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

//        ExecutorService executorService = Executors.newFixedThreadPool(1);
//        executorService.execute(() -> {
//            do {
//                if(count[0] == 100L) {
//                    runProcess[0] = false;
//                    try {
//                        streamingQuery.stop();
////                        sparkSession.close();
//                        executorService.shutdown();
//                    } catch (TimeoutException e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//                try {
//                    Thread.sleep(100);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//            } while (runProcess[0]);
//        });
//        streamingQuery.awaitTermination();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> count[0] == 100);
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    public void Should_ProcessData_And_Publish_With_Headers_To_Solace() throws TimeoutException, StreamingQueryException, InterruptedException {
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
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option(SolaceSparkStreamingProperties.INCLUDE_HEADERS, true)
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        final boolean[] runProcess = {true};
        final int[] messageHeader = {4};
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            dataset1.write()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, dataset1.count())
                    .option(SolaceSparkStreamingProperties.INCLUDE_HEADERS, true)
                    .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
                    .mode(SaveMode.Append)
                    .format("solace").save();
        }).start();

        SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
        Topic topic = JCSMPFactory.onlyInstance().createTopic("random/topic");
        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                    if(count[0] == 100) {
                        messageHeader[0] = bytesXMLMessage.getPriority();
                    }
                }

                @Override
                public void onException(JCSMPException e) {

                }
            });
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

//        ExecutorService executorService = Executors.newFixedThreadPool(1);
//        executorService.execute(() -> {
//            do {
//                if(count[0] == 100L) {
//                    runProcess[0] = false;
//                    try {
//                        Assertions.assertEquals(1, messageHeader[0], "Message Priority mismatch");
//                        streamingQuery.stop();
////                        sparkSession.close();
//                        executorService.shutdown();
//                    } catch (TimeoutException e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//                try {
//                    Thread.sleep(100);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//            } while (runProcess[0]);
//        });
//        streamingQuery.awaitTermination();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> count[0] == 100);
        Assertions.assertEquals(1, messageHeader[0], "Message Priority mismatch");
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    public void Should_ProcessData_And_Publish_Without_Headers_To_Solace() throws TimeoutException, StreamingQueryException, InterruptedException {
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
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option(SolaceSparkStreamingProperties.INCLUDE_HEADERS, true)
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        final boolean[] runProcess = {true};
        final int[] messageHeader = {4};
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            dataset1.write()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, dataset1.count())
                    .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
                    .mode(SaveMode.Append)
                    .format("solace").save();
        }).start();

        SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
        Topic topic = JCSMPFactory.onlyInstance().createTopic("random/topic");
        XMLMessageConsumer messageConsumer = null;
        try {
            messageConsumer = session.getSession().getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage bytesXMLMessage) {
                    count[0] = count[0] + 1;
                    if(count[0] == 100) {
                        messageHeader[0] = bytesXMLMessage.getPriority();
                    }
                }

                @Override
                public void onException(JCSMPException e) {

                }
            });
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

//        ExecutorService executorService = Executors.newFixedThreadPool(1);
//        executorService.execute(() -> {
//            do {
//                if(count[0] == 100L) {
//                    runProcess[0] = false;
//                    try {
//                        Assertions.assertEquals(4, messageHeader[0], "Message Priority mismatch");
//                        streamingQuery.stop();
////                        sparkSession.close();
//                        executorService.shutdown();
//                    } catch (TimeoutException e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//                try {
//                    Thread.sleep(100);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//            } while (runProcess[0]);
//        });
//        streamingQuery.awaitTermination();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> count[0] == 100);
        Assertions.assertEquals(4, messageHeader[0], "Message Priority mismatch");
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    public void Should_ProcessData_And_Publish_With_Only_PayloadColumn_To_Solace() throws TimeoutException, StreamingQueryException, InterruptedException {
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
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                .option(SolaceSparkStreamingProperties.INCLUDE_HEADERS, true)
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        final boolean[] runProcess = {true};
//        final int[] messageHeader = {4};
        Dataset<Row> dataset = reader.load();

        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            dataset1 = dataset1.drop("TimeStamp", "PartitionKey", "Headers");
            dataset1.write()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, dataset1.count())
                    .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
                    .mode(SaveMode.Append)
                    .format("solace").save();
        }).start();

        SolaceSession session = new SolaceSession(solaceContainer.getOrigin(Service.SMF), solaceContainer.getVpn(), solaceContainer.getUsername(), solaceContainer.getPassword());
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

                }
            });
            session.getSession().addSubscription(topic);
            messageConsumer.start();
        } catch (JCSMPException e) {
            throw new RuntimeException(e);
        }

//        ExecutorService executorService = Executors.newFixedThreadPool(1);
//        executorService.execute(() -> {
//            do {
//                if(count[0] == 100L) {
//                    runProcess[0] = false;
//                    try {
//                        streamingQuery.stop();
////                        sparkSession.close();
//                        executorService.shutdown();
//                    } catch (TimeoutException e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//                try {
//                    Thread.sleep(100);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//            } while (runProcess[0]);
//        });
//        streamingQuery.awaitTermination();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> count[0] == 100);
        Thread.sleep(3000); // add timeout to ack messages on queue
        streamingQuery.stop();
    }

    @Test
    public void Should_Fail_Publish_IfMessageIdIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1 = dataset1.drop("Id");
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, dataset1.count())
                        .option(SolaceSparkStreamingProperties.TOPIC, "random/topic")
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    public void Should_Fail_Publish_IfMessageTopicIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1 = dataset1.drop("Topic");
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, dataset1.count())
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    public void Should_Fail_Publish_IfMessagePayloadIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1 = dataset1.drop("Payload");
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, dataset1.count())
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    public void Should_Fail_Publish_IfSolaceHostIsInvalid() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1 = dataset1.drop("Payload");
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, "tcp://invalid-host:55555")
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, dataset1.count())
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    public void Should_Fail_Publish_IfMandatoryHostIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1 = dataset1.drop("Payload");
                dataset1.write()
//                        .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, dataset1.count())
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    public void Should_Fail_Publish_IfMandatoryHostIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1 = dataset1.drop("Payload");
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, "")
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, dataset1.count())
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    public void Should_Fail_Publish_IfMandatoryVpnIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1 = dataset1.drop("Payload");
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
//                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, dataset1.count())
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    public void Should_Fail_Publish_IfMandatoryVpnIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1 = dataset1.drop("Payload");
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                        .option(SolaceSparkStreamingProperties.VPN, "")
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, dataset1.count())
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    public void Should_Fail_Publish_IfMandatoryUsernameIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1 = dataset1.drop("Payload");
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
//                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, dataset1.count())
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    public void Should_Fail_Publish_IfMandatoryUsernameIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1 = dataset1.drop("Payload");
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, "")
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, dataset1.count())
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    public void Should_Fail_Publish_IfMandatoryPasswordIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1 = dataset1.drop("Payload");
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
//                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, dataset1.count())
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    public void Should_Fail_Publish_IfMandatoryPasswordIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                dataset1 = dataset1.drop("Payload");
                dataset1.write()
                        .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, "")
                        .option(SolaceSparkStreamingProperties.BATCH_SIZE, dataset1.count())
                        .mode(SaveMode.Append)
                        .format("solace").save();
            }).start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    public void Should_Fail_Publish_Stream_IfSolaceHostIsInvalid() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path writePath = Paths.get("src", "test", "resources", "spark-checkpoint-3");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream()
                        .option(SolaceSparkStreamingProperties.HOST, "tcp://invalid-host:55555")
                        .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                        .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                        .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                        .option("checkpointLocation", writePath.toAbsolutePath().toString())
                        .format("solace").start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    public void Should_Fail_Publish_Stream_IfMandatoryHostIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path writePath = Paths.get("src", "test", "resources", "spark-checkpoint-3");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream()
//                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option("checkpointLocation", writePath.toAbsolutePath().toString())
                    .format("solace").start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    public void Should_Fail_Publish_Stream_IfMandatoryHostIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path writePath = Paths.get("src", "test", "resources", "spark-checkpoint-3");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream()
                    .option(SolaceSparkStreamingProperties.HOST, "")
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option("checkpointLocation", writePath.toAbsolutePath().toString())
                    .format("solace").start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    public void Should_Fail_Publish_Stream_IfMandatoryVpnIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path writePath = Paths.get("src", "test", "resources", "spark-checkpoint-3");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
//                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option("checkpointLocation", writePath.toAbsolutePath().toString())
                    .format("solace").start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    public void Should_Fail_Publish_Stream_IfMandatoryVpnIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path writePath = Paths.get("src", "test", "resources", "spark-checkpoint-3");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, "")
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option("checkpointLocation", writePath.toAbsolutePath().toString())
                    .format("solace").start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    public void Should_Fail_Publish_Stream_IfMandatoryUsernameIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path writePath = Paths.get("src", "test", "resources", "spark-checkpoint-3");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
//                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option("checkpointLocation", writePath.toAbsolutePath().toString())
                    .format("solace").start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    public void Should_Fail_Publish_Stream_IfMandatoryUsernameIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path writePath = Paths.get("src", "test", "resources", "spark-checkpoint-3");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, "")
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option("checkpointLocation", writePath.toAbsolutePath().toString())
                    .format("solace").start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    public void Should_Fail_Publish_Stream_IfMandatoryPasswordIsMissing() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path writePath = Paths.get("src", "test", "resources", "spark-checkpoint-3");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
//                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option("checkpointLocation", writePath.toAbsolutePath().toString())
                    .format("solace").start();
            streamingQuery.awaitTermination();
        });
    }

    @Test
    public void Should_Fail_Publish_Stream_IfMandatoryPasswordIsEmpty() {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-1");
        Path writePath = Paths.get("src", "test", "resources", "spark-checkpoint-3");
        assertThrows(StreamingQueryException.class, () -> {
            DataStreamReader reader = sparkSession.readStream()
                    .option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, solaceContainer.getPassword())
                    .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                    .option(SolaceSparkStreamingProperties.BATCH_SIZE, "50")
                    .option("checkpointLocation", path.toAbsolutePath().toString())
                    .format("solace");
            Dataset<Row> dataset = reader.load();
            StreamingQuery streamingQuery = dataset.writeStream().option(SolaceSparkStreamingProperties.HOST, solaceContainer.getOrigin(Service.SMF))
                    .option(SolaceSparkStreamingProperties.VPN, solaceContainer.getVpn())
                    .option(SolaceSparkStreamingProperties.USERNAME, solaceContainer.getUsername())
                    .option(SolaceSparkStreamingProperties.PASSWORD, "")
                    .option("checkpointLocation", writePath.toAbsolutePath().toString())
                    .format("solace").start();
            streamingQuery.awaitTermination();
        });
    }
}
