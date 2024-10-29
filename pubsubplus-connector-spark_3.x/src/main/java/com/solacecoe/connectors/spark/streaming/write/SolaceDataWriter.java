package com.solacecoe.connectors.spark.streaming.write;

import com.solacecoe.connectors.spark.streaming.solace.SolaceBroker;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.types.DataTypeUtils;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.*;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SolaceDataWriter implements DataWriter<InternalRow> {
    private final StructType schema;
    private final Map<String, String> options;
    private final SolaceBroker solaceBroker;
    private final UnsafeProjection projection;
    public SolaceDataWriter(StructType schema, Map<String, String> options) {
        this.schema = schema;
        this.options = options;

        this.solaceBroker = new SolaceBroker(options.get("host"), options.get("vpn"), options.get("username"), options.get("password"), options.get("topic"), options);
        this.solaceBroker.initProducer();

        this.projection = createProjection();
    }

    @Override
    public void write(InternalRow row) throws IOException {
        UnsafeRow projectedRow = this.projection.apply(row);
        this.solaceBroker.publishMessage(projectedRow.get(3, DataTypes.StringType).toString(), projectedRow.get(1, DataTypes.BinaryType));
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        return null;
    }

    @Override
    public void abort() throws IOException {

    }

    @Override
    public void close() throws IOException {
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
                new SolaceRowExpression(attributes, "Id", DataTypes.StringType, null).getExpression(),
                new SolaceRowExpression(attributes, "Payload", DataTypes.BinaryType, null).getExpression(),
                new SolaceRowExpression(attributes, "PartitionKey", DataTypes.StringType, null).getExpression(),
                new SolaceRowExpression(attributes, "Topic", DataTypes.StringType, null).getExpression(),
                new SolaceRowExpression(attributes, "TimeStamp", DataTypes.TimestampType, null).getExpression(),
                new SolaceRowExpression(attributes, "Headers", new MapType(DataTypes.StringType, DataTypes.BinaryType, false), null).getExpression()
        };
    }
}
