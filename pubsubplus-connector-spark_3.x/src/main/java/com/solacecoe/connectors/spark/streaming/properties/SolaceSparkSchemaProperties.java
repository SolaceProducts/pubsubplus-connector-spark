package com.solacecoe.connectors.spark.streaming.properties;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

public class SolaceSparkSchemaProperties {
    public static StructField id() {
        return new StructField("Id", DataTypes.StringType, true, Metadata.empty());
    }

    public static StructField payload() {
        return new StructField("Payload", DataTypes.BinaryType, true, Metadata.empty());
    }

    public static StructField partitionKey() {
        return new StructField("PartitionKey", DataTypes.StringType, true, Metadata.empty());
    }

    public static StructField topic() {
        return new StructField("Topic", DataTypes.StringType, true, Metadata.empty());
    }

    public static StructField timestamp() {
        return new StructField("TimeStamp", DataTypes.TimestampType, true, Metadata.empty());
    }

    public static StructField headers() {
        return new StructField("Headers", new MapType(DataTypes.StringType, DataTypes.BinaryType, false), true, Metadata.empty());
    }

    public static StructField[] structFields(boolean includeHeaders) {
        if(includeHeaders) {
            return new StructField[] {id(), payload(), partitionKey(), topic(), timestamp(), headers()};
        } else {
            return new StructField[] {id(), payload(), partitionKey(), topic(), timestamp()};
        }

    }
}
