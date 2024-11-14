package com.solacecoe.connectors.spark.streaming.solace.exceptions;

public class SolaceSecurityException extends RuntimeException{
    public SolaceSecurityException() {
        super();
    }

    public SolaceSecurityException(String message) {
        super(message);
    }

    public SolaceSecurityException(String message, Throwable cause) {
        super(message, cause);
    }

    public SolaceSecurityException(Throwable cause) {
        super(cause);
    }

    protected SolaceSecurityException(String message, Throwable cause,
                                      boolean enableSuppression,
                                      boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
