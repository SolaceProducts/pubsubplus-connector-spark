package com.solace.connector.spark;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.*;

import java.util.concurrent.TimeoutException;

public class LoadSolaceConnector implements Runnable{

    private final SparkSession sparkSession;

    LoadSolaceConnector(SparkSession sparkSession){
        this.sparkSession = sparkSession;
    }

    @Override
    public void run() {
    }

    public static void main(String[] args){

        SparkSession sparkSession = SparkSession.builder()
                .appName("data_source_test")
                .master("local[*]")
                .getOrCreate();
        sparkSession.sparkContext().defaultMinPartitions();

        Dataset<Row> dataset = sparkSession.readStream()
                .option("host", "")
                .option("vpn", "")
                .option("username", "")
                .option("password", "")
                .option("queue", "")
                .option("batchSize", 10)
                .option("checkpointLocation", "")
                .format("com.solace.connector.spark.SolaceSparkStream").load();

        DataStreamWriter dsw = dataset.writeStream()
                .option("checkpointLocation", "")
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (dataFrame, batchId) ->
                        dataFrame.show()
                );
        try {
            dsw.start().awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
            sparkSession.stop();
        }

    }

}
