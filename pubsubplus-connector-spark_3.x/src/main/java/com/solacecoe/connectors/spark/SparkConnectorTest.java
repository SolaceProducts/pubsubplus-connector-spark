package com.solacecoe.connectors.spark;


//import com.solace.connector.spark.receivers.SolaceReliableReceiver;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.rdd.ReliableCheckpointRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

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
                .master("local[*]")
                .getOrCreate();
        sparkSession.sparkContext().defaultMinPartitions();
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
                .option("host", "tcps://mr-connection-h0zr2jc6v7f.messaging.solace.cloud:55443")
                .option("vpn", "sjthotak_solace")
                .option("username", "solace-cloud-client")
                .option("password", "qu03808nfjfprlk3ck458u7bv4")
                .option("queue", "Q/Test")
                .option("batchSize", "10")
                .option("checkpointLocation", "/Users/sravanthotakura/Documents/Spark45")
                .format("solace").load();

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
