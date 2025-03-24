package com.solacecoe.connectors.spark.streaming.write;

import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolaceInvalidPropertyException;
import com.solacesystems.jcsmp.JCSMPProperties;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

public class SolaceStreamingWrite implements StreamingWrite, Serializable {
    private static Logger log = LoggerFactory.getLogger(SolaceBatchWrite.class);
    private final StructType schema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;
    public SolaceStreamingWrite(StructType schema, Map<String, String> properties, CaseInsensitiveStringMap options) {
        this.schema = schema;
        this.properties = properties;
        this.options = options;

        if(!properties.containsKey("host") || properties.get("host") == null || properties.get("host").isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace Host name in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace Host name in configuration options");
        }
        if(!properties.containsKey("vpn") || properties.get("vpn") == null || properties.get("vpn").isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace VPN name in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace VPN name in configuration options");
        }

        if(properties.containsKey(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX+ JCSMPProperties.AUTHENTICATION_SCHEME) &&
                properties.get(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX+ JCSMPProperties.AUTHENTICATION_SCHEME).equals(JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)) {
            if(!properties.containsKey(SolaceSparkStreamingProperties.OAUTH_CLIENT_ACCESSTOKEN)) {
                if(!properties.containsKey(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL) || properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL) == null || properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL).isEmpty()) {
                    throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide OAuth Client Authentication Server URL");
                }

                if(!properties.containsKey(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID) || properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID) == null || properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID).isEmpty()) {
                    throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide OAuth Client ID");
                }

                if(!properties.containsKey(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET) || properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET) == null || properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET).isEmpty()) {
                    throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide OAuth Client Credentials Secret");
                }

                String clientCertificate = properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_CLIENT_CERTIFICATE, null);
                if(clientCertificate != null) {
                    String trustStoreFilePassword = properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_TRUSTSTORE_PASSWORD, null);
                    if (trustStoreFilePassword == null || trustStoreFilePassword.isEmpty()) {
                        throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide OAuth Client TrustStore Password. If TrustStore file path is not configured, please provide password for default java truststore");
                    }
                }
            } else if(properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_ACCESSTOKEN, null) == null || properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_ACCESSTOKEN, null).isEmpty()) {
                throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide valid access token input");
            }
        } else if(properties.containsKey(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX+ JCSMPProperties.AUTHENTICATION_SCHEME) &&
                properties.getOrDefault(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX+ JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_BASIC).equals(JCSMPProperties.AUTHENTICATION_SCHEME_BASIC)){
            if (!properties.containsKey(SolaceSparkStreamingProperties.USERNAME) || properties.get(SolaceSparkStreamingProperties.USERNAME) == null || properties.get(SolaceSparkStreamingProperties.USERNAME).isEmpty()) {
                throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide Solace Username in configuration options");
            }

            if (!properties.containsKey(SolaceSparkStreamingProperties.PASSWORD) || properties.get(SolaceSparkStreamingProperties.PASSWORD) == null || properties.get(SolaceSparkStreamingProperties.PASSWORD).isEmpty()) {
                throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide Solace Password in configuration options");
            }
        }

//        if(!properties.containsKey("topic") || properties.get("topic") == null || properties.get("topic").isEmpty()) {
//            log.error("SolaceSparkConnector - Please provide Solace Queue name in configuration options");
//            throw new RuntimeException("SolaceSparkConnector - Please provide Solace Queue in configuration options");
//        }
    }

    @Override
    public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo physicalWriteInfo) {
        return new SolaceStreamingDataWriterFactory(schema, properties, options);
    }

    @Override
    public void commit(long l, WriterCommitMessage[] writerCommitMessages) {

    }

    @Override
    public void abort(long l, WriterCommitMessage[] writerCommitMessages) {

    }
}
