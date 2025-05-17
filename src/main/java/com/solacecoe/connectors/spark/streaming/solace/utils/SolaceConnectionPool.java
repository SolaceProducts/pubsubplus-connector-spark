package com.solacecoe.connectors.spark.streaming.solace.utils;

import com.solacecoe.connectors.spark.streaming.solace.SolaceBroker;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

import java.util.Map;

public class SolaceConnectionPool {
    private static GenericKeyedObjectPool<String, SolaceBroker> pool;

    public static synchronized GenericKeyedObjectPool<String, SolaceBroker> getInstance(Map<String, String> props, String clientType) {
        if (pool == null) {
            try {
                pool = new GenericKeyedObjectPool<>(new SolaceBroker(props, clientType));
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize Solace pool", e);
            }
        }
        return pool;
    }
}
