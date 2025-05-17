package com.solacecoe.connectors.spark.streaming.solace.exceptions;

public class SolaceSessionException extends RuntimeException{
    public SolaceSessionException(Throwable cause) {
        super(cause);
    }

    public SolaceSessionException(String message) {
        super(message);
    }
}
