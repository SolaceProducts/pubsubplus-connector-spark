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
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    reset();
                }));
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize Solace pool", e);
            }
        }
        return pool;
    }

    public static boolean isKeyPresent(String key) {
        return pool != null && pool.getKeys().contains(key);
    }

    public static void invalidateKey(String key) {
        if(pool != null) {
            if(isKeyPresent(key)) {
                pool.clear(key);
            }
        }
    }

    public static synchronized void reset() {
        if (pool != null) {
            pool.close();
            pool = null;
        }
    }
}
