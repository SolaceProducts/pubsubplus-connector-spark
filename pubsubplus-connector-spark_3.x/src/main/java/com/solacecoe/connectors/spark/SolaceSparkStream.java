package com.solacecoe.connectors.spark;

import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkSchemaProperties;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.execution.streaming.Source;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Option;
import scala.Tuple2;

import java.util.Map;

public class SolaceSparkStream implements TableProvider, DataSourceRegister {

    public SolaceSparkStream(){
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return getTable(new StructType(), new Transform[]{}, options.asCaseSensitiveMap()).schema();
        //return new StructType();
    }

//    @Override
//    public Transform[] inferPartitioning(CaseInsensitiveStringMap options) {
//        return TableProvider.super.inferPartitioning(options);
//    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new SolaceStreamStructure(schema, properties);
    }

    @Override
    public String shortName() {
        return "solace";
    }

//    @Override
//    public boolean supportsExternalMetadata() {
//        return true;
//    }
}
