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
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

public class OAuthClient {
    private static Logger log = LoggerFactory.getLogger(OAuthClient.class);
    AuthorizationGrant clientGrant;
    ClientAuthentication clientAuth;
    Scope scope;
    URI tokenEndpoint;
    HTTPRequest httpRequest;

    public OAuthClient(String url, String clientId, String clientSecret) {
        init(url, clientId, clientSecret);
    }

    public void init(String url, String clientId, String clientSecret) {
        clientGrant = new ClientCredentialsGrant();
        ClientID clientID = new ClientID(clientId);
        Secret secret = new Secret(clientSecret);
        clientAuth = new ClientSecretBasic(clientID, secret);
        // The token endpoint
        scope = new Scope();
        try {
            tokenEndpoint = new URI(url);
        } catch (URISyntaxException e) {
            log.error("SolaceSparkConnector - URI Syntax exception", e);
            throw new RuntimeException(e);
        }
    }

    public void buildRequest(int timeout, String clientCertificatePath, String trustStoreFilePath, String trustStoreFilePassword, String tlsVersion, boolean validateSSLCertificate) {
        if(clientCertificatePath != null) {
            readClientCertificate(timeout, clientCertificatePath, trustStoreFilePath, trustStoreFilePassword, tlsVersion, validateSSLCertificate);
        } else {
            initHttpRequest(timeout, trustStoreFilePath, trustStoreFilePassword, tlsVersion, validateSSLCertificate);
        }
    }

    private void readClientCertificate(int timeout, String clientCertificatePath, String trustStoreFilePath, String trustStoreFilePassword, String tlsVersion, boolean validateSSLCertificate) {
        try {
            File clientCert = new File(clientCertificatePath);
            FileOutputStream fileOutputStream = new FileOutputStream(trustStoreFilePath);
            createKeyStore(Files.readAllBytes(clientCert.toPath())).store(fileOutputStream, trustStoreFilePassword.toCharArray());

            initHttpRequest(timeout, trustStoreFilePath, trustStoreFilePassword, tlsVersion, validateSSLCertificate);
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

    private void initHttpRequest(int timeout, String trustStoreFilePath, String trustStoreFilePassword, String tlsVersion, boolean validateSSLCertificate) {
        // Make the token request
        TokenRequest request = new TokenRequest(tokenEndpoint, clientAuth, clientGrant, scope);
        try {
            httpRequest = request.toHTTPRequest();
            httpRequest.setConnectTimeout(timeout);
            SSLSocketFactory sslSocketFactory;
            if(validateSSLCertificate) {
                File trustStoreFile = new File(trustStoreFilePath);
                char[] trustStorePassword = trustStoreFilePassword != null ? new char[trustStoreFilePassword.length()] : null; // assuming no trust store password

                // Load the trust store, the default type is "pkcs12", the alternative is "jks"
                KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
                trustStore.load(new FileInputStream(trustStoreFile), trustStorePassword);

                sslSocketFactory = TLSUtils.createSSLSocketFactory(
                        trustStore,
                        TLSVersion.valueOf(tlsVersion));
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
