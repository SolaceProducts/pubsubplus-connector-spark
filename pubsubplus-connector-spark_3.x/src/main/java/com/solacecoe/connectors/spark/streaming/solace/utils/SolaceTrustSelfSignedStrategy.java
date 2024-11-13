package com.solacecoe.connectors.spark.streaming.solace.utils;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class SolaceTrustSelfSignedStrategy {
    public static final SolaceTrustSelfSignedStrategy INSTANCE = new SolaceTrustSelfSignedStrategy();

    public SolaceTrustSelfSignedStrategy() {
    }

    public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        return chain.length == 1;
    }
}
