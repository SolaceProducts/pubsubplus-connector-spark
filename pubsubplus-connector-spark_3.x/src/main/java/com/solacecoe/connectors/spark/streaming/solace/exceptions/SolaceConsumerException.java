package com.solacecoe.connectors.spark.streaming.solace.exceptions;

public class SolaceConsumerException extends RuntimeException{
    public SolaceConsumerException(Throwable cause) {
        super(cause);
    }
}
