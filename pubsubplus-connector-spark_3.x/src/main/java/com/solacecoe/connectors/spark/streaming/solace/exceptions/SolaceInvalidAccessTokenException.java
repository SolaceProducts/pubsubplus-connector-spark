package com.solacecoe.connectors.spark.streaming.solace.exceptions;

public class SolaceInvalidAccessTokenException extends RuntimeException{
    public SolaceInvalidAccessTokenException() {
        super();
    }

    public SolaceInvalidAccessTokenException(String message) {
        super(message);
    }

    public SolaceInvalidAccessTokenException(String message, Throwable cause) {
        super(message, cause);
    }

    public SolaceInvalidAccessTokenException(Throwable cause) {
        super(cause);
    }

    protected SolaceInvalidAccessTokenException(String message, Throwable cause,
                                                boolean enableSuppression,
                                                boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
