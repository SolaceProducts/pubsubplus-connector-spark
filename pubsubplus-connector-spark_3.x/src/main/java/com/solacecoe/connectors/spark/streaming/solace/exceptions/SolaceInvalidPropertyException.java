package com.solacecoe.connectors.spark.streaming.solace.exceptions;

public class SolaceInvalidPropertyException extends RuntimeException{
    public SolaceInvalidPropertyException() {
        super();
    }

    public SolaceInvalidPropertyException(String message) {
        super(message);
    }

    public SolaceInvalidPropertyException(String message, Throwable cause) {
        super(message, cause);
    }

    public SolaceInvalidPropertyException(Throwable cause) {
        super(cause);
    }

    protected SolaceInvalidPropertyException(String message, Throwable cause,
                               boolean enableSuppression,
                               boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
