

package com.solacecoe.connectors.spark;
//import com.solace.connector.spark.receivers.SolaceReliableReceiver;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
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
//        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(10000));
//        JavaDStream<SolaceRecord> customReceiverStream = streamingContext.receiverStream(new SolaceReliableReceiver("tcps://mr-connection-h0zr2jc6v7f.messaging.solace.cloud:55443", "sjthotak_solace", "solace-cloud-client", "qu03808nfjfprlk3ck458u7bv4", "queue.orders.incoming"));
//        JavaDStream<SolaceRecord> messages = customReceiverStream.map(message -> message);
////        JavaDStream<String> statusKeyValues = messages.map(msg -> msg);
//        messages.foreachRDD(rdd -> {
    public static void main(String[] args) throws Exception {
        SparkSession sparkSession = SparkSession.builder()
                .appName("data_source_test").master("local[*]").getOrCreate();
        sparkSession.sparkContext().defaultMinPartitions();
        sparkSession.sparkContext().setLogLevel("TRACE");
        Dataset<Row> dataset = sparkSession.readStream()
                .option("host", "tcps://mr-connection-09hs04i84ht.messaging.solace.cloud:55443")
                .option("vpn", "dev-100-nvirginia-demo")
                .option("username", "solace-cloud-client")
                .option("password", "oks6u1qlplrlhen326f5ac9b70")
                .option("queue", "testQ")
                .option("connectRetries", 2)
                .option("reconnectRetries", 2)
                .option("batchSize", 5000)
                .option("ackLastProcessedMessages", false)
                .option("skipDuplicates", false)
                // .option("includeHeaders", true)
                .option("partitions", 2)
                .option("createFlowsOnSameSession", false)
                .option("checkpointLocation", "/Users/sravanthotakura/Documents/Spark48")
                .format("solace").load();

        DataStreamWriter dsw = dataset.writeStream()
                .option("checkpointLocation", "/Users/sravanthotakura/Documents/Spark48")
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataset1, batchId) -> {
                    System.out.println("Reading records " + dataset1.count());
                    dataset1.show();
                });
        try {
            dsw.start().awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
            sparkSession.stop();
        }
    }
}
