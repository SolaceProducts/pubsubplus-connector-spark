package com.solacecoe.connectors.spark.streaming.solace.exceptions;

public class SolaceInvalidAccessTokenException extends RuntimeException{

    public SolaceInvalidAccessTokenException(String message) {
        super(message);
    }

    public SolaceInvalidAccessTokenException(Throwable cause) {
        super(cause);
    }

}
