package com.solace.connector.spark;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class SolaceSparkStream implements TableProvider {

    public SolaceSparkStream(){
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return getTable(new StructType().add("Test", DataTypes.StringType), new Transform[]{}, options.asCaseSensitiveMap()).schema();
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

//    @Override
//    public boolean supportsExternalMetadata() {
//        return true;
//    }
}
