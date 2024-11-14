package com.solacecoe.connectors.spark.streaming.solace.exceptions;

public class SolaceInvalidPropertyException extends RuntimeException{

    public SolaceInvalidPropertyException(String message) {
        super(message);
    }
}
