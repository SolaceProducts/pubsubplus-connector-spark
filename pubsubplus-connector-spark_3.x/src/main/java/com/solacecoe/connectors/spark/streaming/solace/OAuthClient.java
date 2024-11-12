package com.solacecoe.connectors.spark.streaming.solace;

import com.nimbusds.oauth2.sdk.*;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.util.tls.TLSUtils;
import com.nimbusds.oauth2.sdk.util.tls.TLSVersion;
import com.solacesystems.jcsmp.*;
import org.apache.hadoop.shaded.org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

public class OAuthClient implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(OAuthClient.class);
    private AuthorizationGrant clientGrant;
    private Scope scope;
    private URI tokenEndpoint;
    private transient HTTPRequest httpRequest;
    private ClientID clientID;
    private Secret secret;

    public OAuthClient(String url, String clientId, String clientSecret) {
        init(url, clientId, clientSecret);
    }

    public void init(String url, String clientId, String clientSecret) {
        clientGrant = new ClientCredentialsGrant();
        clientID = new ClientID(clientId);
        secret = new Secret(clientSecret);
        // The token endpoint
        scope = new Scope();
        try {
            tokenEndpoint = new URI(url);
        } catch (URISyntaxException e) {
            log.error("SolaceSparkConnector - URI Syntax exception", e);
            throw new RuntimeException(e);
        }
    }

    public void buildRequest(int timeout, String clientCertificatePath, String trustStoreFilePath, String trustStoreFilePassword, String tlsVersion, String trustStoreType, boolean validateSSLCertificate) {
        if(clientCertificatePath != null) {
            readClientCertificate(timeout, clientCertificatePath, trustStoreFilePath, trustStoreFilePassword, tlsVersion, trustStoreType, validateSSLCertificate);
        } else {
            initHttpRequest(timeout, trustStoreFilePath, trustStoreFilePassword, tlsVersion, trustStoreType, validateSSLCertificate, null);
        }
    }

    private void readClientCertificate(int timeout, String clientCertificatePath, String trustStoreFilePath, String trustStoreFilePassword, String tlsVersion, String trustStoreType, boolean validateSSLCertificate) {
        try {
            File clientCert = new File(clientCertificatePath);
            FileOutputStream fileOutputStream = new FileOutputStream(trustStoreFilePath);
            KeyStore keyStore = createKeyStore(Files.readAllBytes(clientCert.toPath()));
            if(keyStore == null) {
                log.error("SolaceSparkConnector - Unable to create keystore from file {}", clientCertificatePath);
                throw new RuntimeException("Unable to create keystore from file " + clientCertificatePath);
            }
            keyStore.store(fileOutputStream, trustStoreFilePassword.toCharArray());
            initHttpRequest(timeout, trustStoreFilePath, trustStoreFilePassword, tlsVersion, trustStoreType, validateSSLCertificate, keyStore);
        } catch (IOException | CertificateException | KeyStoreException | NoSuchAlgorithmException e) {
            log.error("SolaceSparkConnector - Failed to read client certificate", e);
            throw new RuntimeException(e);
        }
    }

    private KeyStore createKeyStore(byte[] ca) {
        try {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(null);
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            if (ca != null) {
                keyStore.setCertificateEntry("client-auth",
                        cf.generateCertificate(new ByteArrayInputStream(ca)));
            }
            return keyStore;
        } catch (Exception ignored) {
            return null;
        }
    }

    private void initHttpRequest(int timeout, String trustStoreFilePath, String trustStoreFilePassword, String tlsVersion, String truststoreType, boolean validateSSLCertificate, KeyStore keyStore) {
        // Make the token request
        ClientAuthentication clientAuth = new ClientSecretBasic(clientID, secret);
        TokenRequest request = new TokenRequest(tokenEndpoint, clientAuth, clientGrant, scope);
        try {
            httpRequest = request.toHTTPRequest();
            httpRequest.setConnectTimeout(timeout);
            SSLSocketFactory sslSocketFactory;
            if(validateSSLCertificate) {
                File trustStoreFile = new File(trustStoreFilePath);
                char[] trustStorePassword = trustStoreFilePassword != null ? new char[trustStoreFilePassword.length()] : null; // assuming no trust store password

                if(keyStore == null) {
                    // Load the trust store, the default type is "pkcs12", the alternative is "jks"
                    keyStore = KeyStore.getInstance(truststoreType);
                    keyStore.load(new FileInputStream(trustStoreFile), trustStorePassword);
                }

                sslSocketFactory = TLSUtils.createSSLSocketFactory(
                        keyStore,
                        getTLSVersion(tlsVersion));
                httpRequest.setSSLSocketFactory(sslSocketFactory);
            } else {
                SSLContext sslContext = SSLContext.getInstance(tlsVersion);
                TrustManager trustManager = new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {

                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {

                    }

                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }
                };
                sslContext.init(null, new TrustManager[]{trustManager}, new SecureRandom());
                httpRequest.setHostnameVerifier(new NoopHostnameVerifier());
                httpRequest.setSSLSocketFactory(sslContext.getSocketFactory());
            }
        } catch (IOException | NoSuchAlgorithmException | KeyManagementException | CertificateException | KeyStoreException |
                 UnrecoverableKeyException e) {
            log.error("SolaceSparkConnector - Exception occurred when building access token request", e);
            throw new RuntimeException(e);
        }
    }

    private TLSVersion getTLSVersion(String tlsVersion) {
        switch (tlsVersion) {
            case "TLS":
                return TLSVersion.TLS;
            case "TLSv1":
                return TLSVersion.TLS_1;
            case "TLSv1.1":
                return TLSVersion.TLS_1_1;
            case "TLSv1.2":
                return TLSVersion.TLS_1_2;
            case "TLSv1.3":
                return TLSVersion.TLS_1_3;
            default:
                throw new RuntimeException("SolaceSparkConnector - Invalid TLS version " + tlsVersion);
        }
    }

    public AccessToken getAccessToken() throws JCSMPException {
        TokenResponse response = null;
        try {
            response = TokenResponse.parse(httpRequest.send());
            if (! response.indicatesSuccess()) {
                // We got an error response...
                TokenErrorResponse errorResponse = response.toErrorResponse();
                log.error("SolaceSparkConnector - Exception when fetching access token: {}", errorResponse.getErrorObject().toString());
                throw new IOException(errorResponse.getErrorObject().toString());
            }

            AccessTokenResponse successResponse = response.toSuccessResponse();

            // Get the access token
            return successResponse.getTokens().getAccessToken();
        } catch (ParseException | IOException e) {
            log.error("SolaceSparkConnector - Exception occurred when fetching access token", e);
            throw new RuntimeException(e);
        }
    }
}
