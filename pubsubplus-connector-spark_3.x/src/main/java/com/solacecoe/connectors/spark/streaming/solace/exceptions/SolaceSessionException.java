package com.solacecoe.connectors.spark.streaming.solace.exceptions;

public class SolaceSessionException extends RuntimeException{
    public SolaceSessionException() {
        super();
    }

    public SolaceSessionException(String message) {
        super(message);
    }

    public SolaceSessionException(String message, Throwable cause) {
        super(message, cause);
    }

    public SolaceSessionException(Throwable cause) {
        super(cause);
    }

    protected SolaceSessionException(String message, Throwable cause,
                                     boolean enableSuppression,
                                     boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
