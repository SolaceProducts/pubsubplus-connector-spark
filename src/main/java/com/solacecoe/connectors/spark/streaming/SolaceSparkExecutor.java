package com.solacecoe.connectors.spark.streaming;

import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceConnectionPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.plugin.ExecutorPlugin;

public class SolaceSparkExecutor implements ExecutorPlugin {
    private final transient Logger log = LogManager.getLogger(SolaceSparkExecutor.class);
    private final String key;

    public SolaceSparkExecutor(String key) {
        this.key = key;
    }
    @Override
    public void shutdown() {
        log.info("SolaceSparkConnector - Executor is shutting down, Closing connection to solace");
        SolaceConnectionPool.invalidateKey(this.key);
    }
}
