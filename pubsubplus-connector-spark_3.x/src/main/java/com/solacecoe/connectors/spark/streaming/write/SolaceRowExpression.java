package com.solacecoe.connectors.spark.streaming.write;

import org.apache.spark.sql.catalyst.types.DataTypeUtils;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import scala.collection.Seq;


public class SolaceRowExpression {
    private final Seq<Attribute> attributes;
    private final String attributeName;
    private final DataType dataType;
    private final Attribute defaultValue;
    public SolaceRowExpression(Seq<Attribute> attributes, String attributeName, DataType dataType, Attribute defaultValue) {
        this.attributes = attributes;
        this.attributeName = attributeName;
        this.dataType = dataType;
        this.defaultValue = defaultValue;
    }

    public Attribute getExpression() {
        Attribute attribute = this.attributes.find(field -> field.name().equals(attributeName)).getOrElse(() -> this.defaultValue);
        if(attribute == null) {
            throw new RuntimeException("Could not find attribute " + attributeName);
        }
        if(!DataTypeUtils.sameType(attribute.dataType(), this.dataType)) {
            throw new IllegalArgumentException("Attribute " + attributeName + " is not of type " + dataType);
        }

        return attribute;
    }
}
