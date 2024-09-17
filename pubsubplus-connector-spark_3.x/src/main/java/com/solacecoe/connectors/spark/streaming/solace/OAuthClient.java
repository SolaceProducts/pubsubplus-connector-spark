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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class OAuthClient {
    private static Logger log = LoggerFactory.getLogger(OAuthClient.class);
    AuthorizationGrant clientGrant;
    ClientAuthentication clientAuth;
    Scope scope;
    URI tokenEndpoint;
    public static void main(String[] args) throws JCSMPException {
        OAuthClient oAuthClient = new OAuthClient("https://localhost:7778/realms/solace/protocol/openid-connect/token", "solace", "solace-secret");
        AccessToken accessToken = oAuthClient.getAccessToken(30);
        System.out.println(accessToken.getValue());

        JCSMPProperties jcsmpProperties = new JCSMPProperties();
        jcsmpProperties.setProperty(JCSMPProperties.HOST, "tcps://localhost:57694");            // host:port
        jcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, "default");    //
        jcsmpProperties.setProperty(JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2);
        jcsmpProperties.setProperty(JCSMPProperties.OAUTH2_ACCESS_TOKEN, accessToken.getValue());
        jcsmpProperties.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false);

        JCSMPSession session = JCSMPFactory.onlyInstance().createSession(jcsmpProperties);
        session.connect();
        System.out.println("Session running " + session.isCapable(CapabilityType.ACTIVE_FLOW_INDICATION));
    }
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

    public AccessToken getAccessToken(int timeout) {
        // Make the token request
        TokenRequest request = new TokenRequest(tokenEndpoint, clientAuth, clientGrant, scope);

        TokenResponse response = null;
        try {
            HTTPRequest httpRequest = request.toHTTPRequest();
            httpRequest.setConnectTimeout(timeout);
            File trustStoreFile = new File("/Users/sravanthotakura/Work/tech-coe/connectors/tech-coe-git/quarkus/solace-quarkus/target/keycloak.jks");
            char[] trustStorePassword = null; // assuming no trust store password

// Load the trust store, the default type is "pkcs12", the alternative is "jks"
            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStore.load(new FileInputStream(trustStoreFile), trustStorePassword);

// Create a new SSLSocketFactory, you can keep it around for each HTTPS
// request as it's thread-safe. Use the most recent TLS version supported by
// the web server. TLS 1.3 has been standard since 2018 and is recommended.
            SSLSocketFactory sslSocketFactory = TLSUtils.createSSLSocketFactory(
                    trustStore,
                    TLSVersion.TLS_1_2);
            httpRequest.setSSLSocketFactory(sslSocketFactory);

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
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        } catch (KeyManagementException e) {
            throw new RuntimeException(e);
        } catch (CertificateException e) {
            throw new RuntimeException(e);
        } catch (KeyStoreException e) {
            throw new RuntimeException(e);
        } catch (UnrecoverableKeyException e) {
            throw new RuntimeException(e);
        }
    }
}
