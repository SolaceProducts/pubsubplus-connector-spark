package com.solacecoe.connectors.spark.streaming.solace.exceptions;

public class SolaceRecordMapperException extends RuntimeException{
    public SolaceRecordMapperException() {
        super();
    }

    public SolaceRecordMapperException(String message) {
        super(message);
    }

    public SolaceRecordMapperException(String message, Throwable cause) {
        super(message, cause);
    }

    public SolaceRecordMapperException(Throwable cause) {
        super(cause);
    }

    protected SolaceRecordMapperException(String message, Throwable cause,
                                          boolean enableSuppression,
                                          boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
