package com.solacecoe.connectors.spark.streaming.solace.exceptions;

public class SolaceMessageException extends RuntimeException{
    public SolaceMessageException(Throwable cause) {
        super(cause);
    }
}
