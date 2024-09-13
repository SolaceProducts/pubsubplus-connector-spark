package com.solacecoe.connectors.spark.base;

import com.solace.semp.v2.config.ApiClient;
import com.solace.semp.v2.config.client.AllApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SempV2Api {
    private final com.solace.semp.v2.config.client.AllApi configApi;
    private final com.solace.semp.v2.monitor.client.AllApi monitorApi;
    private static final Logger LOG = LoggerFactory.getLogger(SempV2Api.class);

    public SempV2Api(String mgmtHost, String mgmtUsername, String mgmtPassword) {
        LOG.info("Creating Config API Clients for {}", mgmtHost);
        com.solace.semp.v2.config.ApiClient configApiClient = new com.solace.semp.v2.config.ApiClient();
        configApiClient.setBasePath(String.format("%s/SEMP/v2/config", mgmtHost));
        configApiClient.setUsername(mgmtUsername);
        configApiClient.setPassword(mgmtPassword);
        this.configApi = new com.solace.semp.v2.config.client.AllApi(configApiClient);


        com.solace.semp.v2.monitor.ApiClient monitorApiClient = new com.solace.semp.v2.monitor.ApiClient();
        monitorApiClient.setBasePath(String.format("%s/SEMP/v2/monitor", mgmtHost));
        monitorApiClient.setUsername(mgmtUsername);
        monitorApiClient.setPassword(mgmtPassword);
        this.monitorApi = new com.solace.semp.v2.monitor.client.AllApi(monitorApiClient);
    }

    public com.solace.semp.v2.config.client.AllApi config() {
        return this.configApi;
    }

    public com.solace.semp.v2.monitor.client.AllApi monitor() {
        return this.monitorApi;
    }
}
