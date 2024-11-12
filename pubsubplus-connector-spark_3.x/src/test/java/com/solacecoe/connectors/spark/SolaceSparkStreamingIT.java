package com.solacecoe.connectors.spark;

import com.solace.semp.v2.config.ApiException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.solace.Service;
import org.testcontainers.solace.SolaceContainer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import com.solace.semp.v2.config.client.model.MsgVpnQueue;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SolaceSparkStreamingIT {
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
    public void testSolaceSparkStreaming() throws TimeoutException, StreamingQueryException {
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

//        assertEquals("Number of events consumed from Solace is not equal to number of records written to Spark", 100L, count[0]);
    }

    @Test
    public void testSolaceSparkStreamingCheckPayload() throws TimeoutException, StreamingQueryException {
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
                .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES, 1)
                .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES, 1)
                .option(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES_PER_HOST, 1)
                .option(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES_WAIT_TIME, 100)
                .option(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX+"sub_ack_window_threshold", 75)
                .option(SolaceSparkStreamingProperties.QUEUE, "Solace/Queue/0")
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, "1")
                .option("checkpointLocation", path.toAbsolutePath().toString())
                .format("solace");
        final long[] count = {0};
        final boolean[] runProcess = {true};
        final Object lock = new Object();
        Dataset<Row> dataset = reader.load();
        AtomicReference<String> payload = new AtomicReference<>("");
        StreamingQuery streamingQuery = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
            synchronized (lock) {
                count[0] = count[0] + dataset1.count();
                if(dataset1.count() == 1) {
                    payload.set(dataset1.select(dataset1.col("Payload").cast("string")).collectAsList().get(0).getString(0));
                }
            }
        }).start();

        ExecutorService executorService = Executors.newFixedThreadPool(1);
        executorService.execute(() -> {
            do {
                if(count[0] == 100L) {
                    runProcess[0] = false;
                    try {
                        Assertions.assertEquals("Hello Spark!", payload.get());
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
    public void testSolaceSparkStreamingMultipleConsumersOnSameSession() throws TimeoutException, StreamingQueryException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-2");
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
                .option(SolaceSparkStreamingProperties.BATCH_SIZE, 2)
                .option("createFlowsOnSameSession", true)
                .option(SolaceSparkStreamingProperties.PARTITIONS, 2)
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
                    try {
                        MsgVpnQueueTxFlowsResponse msgVpnQueueTxFlowResponse =  sempV2Api.monitor().getMsgVpnQueueTxFlows("default", "Solace/Queue/0", 10, null, null, null);
                        if(msgVpnQueueTxFlowResponse.getData() != null && !msgVpnQueueTxFlowResponse.getData().isEmpty()) {
                            Assertions.assertEquals(2, msgVpnQueueTxFlowResponse.getData().size(), "Number of consumer flows should be 2");
                            Assertions.assertEquals(msgVpnQueueTxFlowResponse.getData().get(0).getClientName(), msgVpnQueueTxFlowResponse.getData().get(1).getClientName(), "Client Name of two solace sessions should be same");
                        }
                    } catch (com.solace.semp.v2.monitor.ApiException e) {
                        throw new RuntimeException(e);
                    }
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
    public void testSolaceSparkStreamingMultipleConsumersOnDifferentSession() throws TimeoutException, StreamingQueryException {
        Path path = Paths.get("src", "test", "resources", "spark-checkpoint-3");
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
                .option(SolaceSparkStreamingProperties.PARTITIONS, 2)
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
                    try {
                        MsgVpnQueueTxFlowsResponse msgVpnQueueTxFlowResponse =  sempV2Api.monitor().getMsgVpnQueueTxFlows("default", "Solace/Queue/0", 10, null, null, null);
                        if(msgVpnQueueTxFlowResponse.getData() != null && !msgVpnQueueTxFlowResponse.getData().isEmpty()) {
                            Assertions.assertEquals(2, msgVpnQueueTxFlowResponse.getData().size(), "Number of consumer flows should be 2");
                            Assertions.assertNotEquals(msgVpnQueueTxFlowResponse.getData().get(0).getClientName(), msgVpnQueueTxFlowResponse.getData().get(1).getClientName(), "Client Name of two solace sessions should not be the same");
                        }
                    } catch (com.solace.semp.v2.monitor.ApiException e) {
                        throw new RuntimeException(e);
                    }
                    runProcess[0] = false;
                    try {
                        streamingQuery.stop();
//                        SparkSession.clearActiveSession();
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
