package com.solacecoe.connectors.spark.streaming.solace.utils;

import javax.net.ssl.X509TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class SolaceTrustManagerDelegate implements X509TrustManager {
    private final X509TrustManager trustManager;
    private final SolaceTrustSelfSignedStrategy trustStrategy;

    public SolaceTrustManagerDelegate(X509TrustManager trustManager, SolaceTrustSelfSignedStrategy trustStrategy) {
        this.trustManager = trustManager;
        this.trustStrategy = trustStrategy;
    }

    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        this.trustManager.checkClientTrusted(chain, authType);
    }

    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        if (!this.trustStrategy.isTrusted(chain, authType)) {
            this.trustManager.checkServerTrusted(chain, authType);
        }

    }

    public X509Certificate[] getAcceptedIssuers() {
        return this.trustManager.getAcceptedIssuers();
    }
}
