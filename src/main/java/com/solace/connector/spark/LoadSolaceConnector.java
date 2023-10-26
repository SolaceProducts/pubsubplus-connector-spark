package com.solace.connector.spark;

import com.solace.connector.spark.receivers.SolaceReliableReceiver;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.List;

public class LoadSolaceConnector implements Runnable {

    private final SparkSession sparkSession;

    LoadSolaceConnector(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
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

    public static void readParquet() {
        SparkSession sparkSession = SparkSession.builder()
                .appName("data_source_test")
                .master("local[*]")
                .getOrCreate();
        sparkSession.sparkContext().defaultMinPartitions();
        sparkSession.sparkContext().setLogLevel("WARN");

        Dataset<Row> pf = sparkSession.read().parquet("/Users/sravanthotakura/Documents/output");

        pf.registerTempTable("employee");

        SQLContext sqlContext = new SQLContext(sparkSession);
        sqlContext.sql("select count(distinct MessageId) from employee").show();
    }

    public static void processStream() {
        SparkSession sparkSession = SparkSession.builder()
                .appName("data_source_test")
                .master("local[*]")
                .getOrCreate();
        sparkSession.sparkContext().defaultMinPartitions();
//        sparkSession.sparkContext().setLogLevel("WARN");
        sparkSession.sparkContext().setCheckpointDir("/Users/sravanthotakura/Documents/Spark35");

        Dataset<Row> dataset = sparkSession.readStream()
                .option("host", "tcps://mr-connection-h0zr2jc6v7f.messaging.solace.cloud:55443")
                .option("vpn", "sjthotak_solace")
                .option("username", "solace-cloud-client")
                .option("password", "qu03808nfjfprlk3ck458u7bv4")
                .option("queue", "queue.orders.incoming")
                .option("batchSize", 100)
                .option("partitions", 0)
                .format("com.solace.connector.spark.SolaceSparkStream").load();

        DataStreamWriter dsw = dataset.writeStream()
                .option("path", "/Users/sravanthotakura/Documents/output")
//                .outputMode("append")
//                .format("console")
                .option("checkpointLocation", "/Users/sravanthotakura/Documents/Spark35")
                .outputMode("append");
//                .option("checkpointLocation", "/Users/sravanthotakura/Documents/Spark35")
//                .trigger(Trigger.ProcessingTime("60 seconds"))
//                .option("mergeSchema", "true")
//                //.trigger(Trigger.ProcessingTime("60 Seconds"))
//                .foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
//                    public void call(Dataset<Row> dataset, Long batchId) {
//                        System.out.println("Reading records " + dataset.count());
//                        Row test = (Row) dataset.collect();
////                        dataset.checkpoint();
//                    }
//                });
        try {
            dsw.start().awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
            sparkSession.stop();
        }
    }

    public static void main(String[] args) throws Exception {

        processStream();
//        readParquet();


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

    @Override
    public void run() {
    }

}
