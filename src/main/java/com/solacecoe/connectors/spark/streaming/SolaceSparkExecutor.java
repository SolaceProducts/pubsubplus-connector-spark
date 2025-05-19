package com.solacecoe.connectors.spark.streaming;

import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceConnectionPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.TaskFailedReason;
import org.apache.spark.api.plugin.ExecutorPlugin;

import java.io.Serializable;

public class SolaceSparkExecutor implements ExecutorPlugin, Serializable {
    private final transient Logger log = LogManager.getLogger(SolaceSparkExecutor.class);
    private String key;

    public SolaceSparkExecutor() {
        // No-arg constructor
    }
    public SolaceSparkExecutor(String key) {
        this.key = key;
    }
    @Override
    public void shutdown() {
        log.info("SolaceSparkConnector - Executor is shutting down, Closing connection to solace");
        cleanUp();
    }

    @Override
    public void onTaskFailed(TaskFailedReason failureReason) {
        log.info("SolaceSparkConnector - Executor is shutting down as task is failed {}, Closing connection to solace", failureReason.toErrorString());
        cleanUp();
    }

    private void cleanUp() {
        SolaceConnectionPool.invalidateKey(this.key);
    }
}
