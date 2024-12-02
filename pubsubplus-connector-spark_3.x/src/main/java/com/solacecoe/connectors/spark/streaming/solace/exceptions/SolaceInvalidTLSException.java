package com.solacecoe.connectors.spark.streaming.solace.exceptions;

public class SolaceInvalidTLSException extends RuntimeException{
    public SolaceInvalidTLSException(String message) {
        super(message);
    }
}
