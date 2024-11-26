package com.solacecoe.connectors.spark.streaming.solace.utils;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;

public class SolaceNoopHostnameVerifier implements HostnameVerifier {
    @Override
    public boolean verify(String hostname, SSLSession session) {
        return true;
    }
}
