package com.solacecoe.connectors.spark.streaming.write;

import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkSchemaProperties;
import com.solacecoe.connectors.spark.streaming.solace.SolaceBroker;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.types.DataTypeUtils;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SolaceDataWriter implements DataWriter<InternalRow>, Serializable {
    private final StructType schema;
    private final Map<String, String> properties;
    private final SolaceBroker solaceBroker;
    private final UnsafeProjection projection;
    public SolaceDataWriter(StructType schema, Map<String, String> properties) {
        this.schema = schema;
        this.properties = properties;

        this.solaceBroker = new SolaceBroker(properties.get("host"), properties.get("vpn"), properties.get("username"), properties.get("password"), properties.get("topic"), properties);
        this.solaceBroker.initProducer();

        this.projection = createProjection();
    }

    @Override
    public void write(InternalRow row) throws IOException {
        UnsafeRow projectedRow = this.projection.apply(row);
        this.solaceBroker.publishMessage(projectedRow.getString(3), projectedRow.getBinary(1), projectedRow.getMap(5));
    }

    @Override
    public WriterCommitMessage commit() {
        return null;
    }

    @Override
    public void abort() {

    }

    @Override
    public void close() {
        this.solaceBroker.close();
    }

    private UnsafeProjection createProjection() {
        List<Attribute> attributeList = new ArrayList<>();
        this.schema.foreach(field -> attributeList.add(DataTypeUtils.toAttribute(field)));
        Seq<Attribute> attributes = JavaConverters.asScalaIteratorConverter(attributeList.iterator()).asScala().toSeq();

        return UnsafeProjection.create(JavaConverters.asScalaIteratorConverter(Arrays.stream(getExpressions(attributes)).iterator()).asScala().toSeq(),
                JavaConverters.asScalaIteratorConverter(attributeList.iterator()).asScala().toSeq()
        );
    }

    private Expression[] getExpressions(Seq<Attribute> attributes) {

        return new Expression[] {
                // DataTypeUtils.toAttribute(new StructField("Id", DataTypes.StringType, true, Metadata.empty()))
                new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.id().name(), SolaceSparkSchemaProperties.id().dataType(), null).getExpression(),
                new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.payload().name(), SolaceSparkSchemaProperties.payload().dataType(), null).getExpression(),
                new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.partitionKey().name(), SolaceSparkSchemaProperties.partitionKey().dataType(), null).getExpression(),
                new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.topic().name(), SolaceSparkSchemaProperties.topic().dataType(), null).getExpression(),
                new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.timestamp().name(), SolaceSparkSchemaProperties.timestamp().dataType(), null).getExpression(),
                new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.headers().name(), SolaceSparkSchemaProperties.headers().dataType(), null).getExpression()
        };
    }
}
