package com.solacecoe.connectors.spark.streaming.write;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.types.DataTypeUtils;
import org.apache.spark.sql.types.DataType;
import scala.collection.Seq;

import java.util.Locale;


public class SolaceRowExpression {
    private final Seq<Attribute> attributes;
    private final String attributeName;
    private final DataType dataType;
    private final Attribute defaultValue;
    private final boolean isMandatory;
    public SolaceRowExpression(Seq<Attribute> attributes, String attributeName, DataType dataType, Attribute defaultValue, boolean isMandatory) {
        this.attributes = attributes;
        this.attributeName = attributeName;
        this.dataType = dataType;
        this.defaultValue = defaultValue;
        this.isMandatory = isMandatory;
    }

    public Expression getExpression() {
        Attribute attribute = this.attributes.find(field -> field.name().equals(attributeName)).getOrElse(() -> this.defaultValue);
        if(isMandatory && attribute == null) {
            throw new RuntimeException("SolaceSparkConnector - Could not find attribute " + attributeName + " either in data frame column or options. Please use "+ attributeName.toLowerCase(Locale.ROOT) +" option for setting a " + attributeName.toLowerCase(Locale.ROOT));
        }
        if(isMandatory && !DataTypeUtils.sameType(attribute.dataType(), this.dataType)) {
            throw new IllegalArgumentException("SolaceSparkConnector - Attribute " + attributeName + " is not of type " + dataType);
        }

        return attribute == null ? new Literal(defaultValue, this.dataType) : attribute;
    }
}
