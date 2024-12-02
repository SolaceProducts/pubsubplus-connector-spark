package com.solacecoe.connectors.spark.streaming.solace.exceptions;

public class SolaceSecurityException extends RuntimeException{

    public SolaceSecurityException(String message) {
        super(message);
    }

    public SolaceSecurityException(Throwable cause) {
        super(cause);
    }
}
