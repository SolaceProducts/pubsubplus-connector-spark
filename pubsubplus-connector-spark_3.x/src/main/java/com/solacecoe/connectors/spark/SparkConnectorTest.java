package com.solacecoe.connectors.spark;


//import com.solace.connector.spark.receivers.SolaceReliableReceiver;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.Trigger;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class SparkConnectorTest implements Runnable{

    private final SparkSession sparkSession;

    SparkConnectorTest(SparkSession sparkSession){
        this.sparkSession = sparkSession;
    }

    @Override
    public void run() {
    }

//    private static JavaStreamingContext createContext() throws Exception {
//        final SparkConf sparkConf = new SparkConf().setAppName("JavaCustomReceiver").setMaster("local[*]");
//
//        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(10000));
//
//        JavaDStream<SolaceRecord> customReceiverStream = streamingContext.receiverStream(new SolaceReliableReceiver("tcps://mr-connection-h0zr2jc6v7f.messaging.solace.cloud:55443", "sjthotak_solace", "solace-cloud-client", "qu03808nfjfprlk3ck458u7bv4", "queue.orders.incoming"));
//
//        JavaDStream<SolaceRecord> messages = customReceiverStream.map(message -> message);
//
////        JavaDStream<String> statusKeyValues = messages.map(msg -> msg);
//
//        messages.foreachRDD(rdd -> {
//            rdd.collect().forEach(s -> {
//                System.out.println(s.getDestination());
//            });
//            rdd.cache();
//            rdd.checkpoint();
//        });
//
//        streamingContext.checkpoint("/Users/sravanthotakura/Work/streamcheckpoint.txt");
//        return streamingContext;
//    }

    public static void main(String[] args) throws Exception {

        SparkSession sparkSession = SparkSession.builder()
                .appName("data_source_test")
                .config("spark.default.parallelism", "1")
                .master("local[*]")
                .getOrCreate();
//        sparkSession.sparkContext().defaultMinPartitions();
//        sparkSession.sparkContext().setLogLevel("INFO");
//        sparkSession.sparkContext().execut

//        statusKeyValues.reduceByWindow( "_" + "_", "_" -"_", Seconds(30), Seconds(5))
//        JavaDStream<Message> messages = customReceiverStream.flatMap(message -> {
//            System.out.println(message.message.getDestination());
//            message.message.ackMessage();
//            return Arrays.asList(message).iterator();
//        });

//        List<Message> consumedRDDs = new ArrayList<>();
//        customReceiverStream.foreachRDD(rdd -> rdd.collect()
//                .forEach(label -> {
//                    System.out.println(label.message);
//                    consumedRDDs.add(label);
//                }));

//        messages.foreachRDD(rdd -> {
//            rdd.toLocalIterator().forEachRemaining(message -> {
//                System.out.println(message.getDestination());
//            });
//            List<BytesXMLMessage> messageList = rdd.collect();
//            messageList.forEach(message -> {
//
//                message.ackMessage();
//            });
//
//            rdd.checkpoint();
//        });
//        messages.print(100);

//        Function0<JavaStreamingContext> createContextFunc = () -> createContext();
//
////        JavaStreamingContext streamingContext =
////                JavaStreamingContext.getOrCreate("/Users/sravanthotakura/Work/streamcheckpoint.txt", createContextFunc);
//        JavaStreamingContext streamingContext = createContext();
//        streamingContext.start();
//        streamingContext.awaitTermination();

//        System.out.println(consumedRDDs.size());
        Dataset<Row> dataset = sparkSession.readStream()
                .option("host", "tcps://mr-connection-09hs04i84ht.messaging.solace.cloud:55443")
                .option("vpn", "dev-100-nvirginia-demo")
                .option("username", "solace-cloud-client")
                .option("password", "oks6u1qlplrlhen326f5ac9b70")
                .option("queue", "queue.foobar")
                .option("batchSize", "2000")
                .option("partitions", "10")
//                .option("checkpointLocation", "/Users/sravanthotakura/Documents/Spark152")
                .format("solace").load();

        dataset.writeStream()
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                    dataset1.write().option("host", "tcps://mr-connection-09hs04i84ht.messaging.solace.cloud:55443")
                            .option("vpn", "dev-100-nvirginia-demo")
                            .option("username", "solace-cloud-client")
                            .option("password", "oks6u1qlplrlhen326f5ac9b70")
//                            .option("id", UUID.randomUUID().toString())
//                            .option("batchSize", dataset1.count())
                            .option("batchId", batchId)
                            .option("topic", "debezium/cdc/mysql/1")
                            .mode(SaveMode.Append)
                            .format("solace").save();
                })
                .option("checkpointLocation", "/Users/sravanthotakura/Documents/Spark159")
//                .option("checkpointLocation", "/Users/sravanthotakura/Documents/Spark156")
//                .option("host", "tcps://mr-connection-09hs04i84ht.messaging.solace.cloud:55443")
//                .option("vpn", "dev-100-nvirginia-demo")
//                .option("username", "solace-cloud-client")
//                .option("password", "oks6u1qlplrlhen326f5ac9b70")
////                            .option("id", UUID.randomUUID().toString())
////                .option("batchSize", updatedDs.count())
//                .option("topic", "debezium/cdc/mysql/1")
////                .mode(SaveMode.Append)
//                .format("solace")
                .start().awaitTermination();

//        final long[] count = {0};
//        customReceiverStream.flatMap(s)
//        DataStreamWriter dsw = dataset.writeStream()
//                //.option("checkpointLocation", "/Users/sravanthotakura/Documents/Spark35")
//                //.trigger(Trigger.ProcessingTime("60 seconds"))
//                //.option("mergeSchema", "true")
////                //.trigger(Trigger.ProcessingTime("60 Seconds"))
//                .foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
//                    public void call(Dataset<Row> dataset, Long batchId) {
//                        System.out.println("Reading records " + dataset.count());
//                        dataset.show();
//                    }
//                });
//        try {
//            dsw.start().awaitTermination();
//        } catch (Exception e) {
//            e.printStackTrace();
//            sparkSession.stop();
//        }

//        try {
//            File randomAccessFile = new File("/Users/sravanthotakura/Documents/application1234.log");
//            FileReader fileReader = new FileReader(randomAccessFile);
//            BufferedReader bufferedReader = new BufferedReader(fileReader);
//            String line;
//            while((line=bufferedReader.readLine())!=null)
//            {
//                  if(line.contains("Acknowledged Solace Message with ID")) {
//                      System.out.println(line.split("Acknowledged Solace Message with ID")[1].trim());
//                  }
//            }
//            fileReader.close();
//
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    }

}





