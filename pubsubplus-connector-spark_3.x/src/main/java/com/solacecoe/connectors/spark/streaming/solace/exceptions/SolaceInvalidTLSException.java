package com.solacecoe.connectors.spark.streaming.solace.exceptions;

public class SolaceInvalidTLSException extends RuntimeException{
    public SolaceInvalidTLSException() {
        super();
    }

    public SolaceInvalidTLSException(String message) {
        super(message);
    }

    public SolaceInvalidTLSException(String message, Throwable cause) {
        super(message, cause);
    }

    public SolaceInvalidTLSException(Throwable cause) {
        super(cause);
    }

    protected SolaceInvalidTLSException(String message, Throwable cause,
                                        boolean enableSuppression,
                                        boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
