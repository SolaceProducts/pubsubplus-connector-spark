package com.solacecoe.connectors.spark;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({
        SolaceSparkStreamingMessageReplayIT.class,
        SolaceSparkStreamingSinkIT.class,
        SolaceSparkStreamingSourceIT.class,
        SolaceSparkStreamingTLSClientCertificateCNIT.class,
        SolaceSparkStreamingTLSUsernameAndPasswordAuthenticationIT.class,
        SolaceSparkStreamingTLSUsernameAuthenticationIT.class,
        SolaceSparkStreamingOAuthIT.class
})
public class SolaceSparkIntegrationTestSuite {
}
