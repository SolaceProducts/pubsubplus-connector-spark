package com.solacecoe.connectors.spark;

import com.solacecoe.connectors.spark.streaming.SolaceMicroBatch;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SolaceScan implements Scan {
    private final Logger logger = LoggerFactory.getLogger(SolaceScan.class);
    private final StructType schema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;

    public SolaceScan(StructType schema,
                   Map<String, String> properties,
                   CaseInsensitiveStringMap options) {

        this.schema = schema;
        this.properties = properties;
        this.options = options;
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public String description() {
        return Scan.super.description();
    }

    @Override
    public Batch toBatch() {
        return new SolaceBatch(schema, properties, options);
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
        boolean isDatabricks = false;
        boolean isUCVolume = false;
        if(this.properties.getOrDefault(SolaceSparkStreamingProperties.SPARK_RUNTIME_PLATFORM, SolaceSparkStreamingProperties.SPARK_RUNTIME_PLATFORM_DEFAULT).equals(SolaceSparkStreamingProperties.SPARK_RUNTIME_PLATFORM_DEFAULT)) {
            // Get Spark session and its config
            SparkSession spark = SparkSession.active();
            RuntimeConfig sparkConf = spark.conf();
            // Access Spark config properties
            String databricksRuntime = sparkConf.getOption(
                    "spark.databricks.clusterUsageTags.clusterId").getOrElse(() -> null);
            if(databricksRuntime != null) {
                isDatabricks = true;
            } else {
                logger.warn("SolaceSparkConnector - SPARK_RUNTIME_PLATFORM is set to DATABRICKS but not able to find Databricks Cluster Id in default Spark Configuration. The configured checkpoint location will be considered as native file path.");
            }

            if(checkpointLocation.contains(SolaceSparkStreamingProperties.DATABRICKS_VOLUME_PREFIX)) {
                isUCVolume = true;
            }
        }
        return new SolaceMicroBatch(properties, checkpointLocation, isDatabricks, isUCVolume);
    }
}
