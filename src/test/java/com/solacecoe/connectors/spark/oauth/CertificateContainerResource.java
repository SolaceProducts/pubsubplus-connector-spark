package com.solacecoe.connectors.spark.oauth;

import org.testcontainers.shaded.org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.testcontainers.shaded.org.bouncycastle.openssl.PEMParser;
import org.testcontainers.shaded.org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.testcontainers.utility.MountableFile;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

public class CertificateContainerResource {
    private SolaceOAuthContainer solaceOAuthContainer;
    private boolean cnAsUsername;
    public CertificateContainerResource(boolean cnAsUsername) {
        this.cnAsUsername = cnAsUsername;
    }
    public void start() {
        writeTrustStore();
        writeKeyStore();

        if(!cnAsUsername) {
            solaceOAuthContainer = new SolaceOAuthContainer("solace/solace-pubsub-standard:latest");
            solaceOAuthContainer.withCredentials("user", "pass")
                    .withClientCert(MountableFile.forClasspathResource("serverCertCombined.pem"),
                            MountableFile.forClasspathResource("MyRootCaCert.pem"), true)
                    .withExposedPorts(SolaceOAuthContainer.Service.SMF.getPort(), SolaceOAuthContainer.Service.SMF_SSL.getPort(), 1943, 8080)
                    .withPublishTopic(SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_SUBSCRIPTION, SolaceOAuthContainer.Service.SMF)
                    .withPublishTopic("random/topic", SolaceOAuthContainer.Service.SMF)
                    .withPublishTopic("solace/spark/connector/offset", SolaceOAuthContainer.Service.SMF);
        } else {
            solaceOAuthContainer = new SolaceOAuthContainer("solace/solace-pubsub-standard:latest");
            solaceOAuthContainer.withCredentials("user", "pass")
                    .withClientCert(MountableFile.forClasspathResource("serverCertCombined.pem"),
                            MountableFile.forClasspathResource("MyRootCaCert.pem"), true)
                    .withCNAsUsernameSource()
                    .withExposedPorts(SolaceOAuthContainer.Service.SMF.getPort(), SolaceOAuthContainer.Service.SMF_SSL.getPort(), 1943, 8080)
                    .withPublishTopic(SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_SUBSCRIPTION, SolaceOAuthContainer.Service.SMF)
                    .withPublishTopic("random/topic", SolaceOAuthContainer.Service.SMF)
                    .withPublishTopic("solace/spark/connector/offset", SolaceOAuthContainer.Service.SMF);
        }

        solaceOAuthContainer.start();
        await().until(() -> solaceOAuthContainer.isRunning());

    }

    private static void writeTrustStore() {
        Path resources = Paths.get("src", "test", "resources");
        String absolutePath = resources.toFile().getAbsolutePath();
        File file = new File(absolutePath + "/MyRootCaCert.pem");
        File keyFile = new File(absolutePath + "/MyRootCaKey.key");
        try {
            String path = absolutePath +"/solace.jks";
            File yourFile = new File(absolutePath +"/solace.jks");
            if(!yourFile.exists()) {
                yourFile.createNewFile();
            }
            FileOutputStream fos = new FileOutputStream(path);
            PrivateKey privateKey = loadPrivateKey(keyFile.getAbsolutePath());
            createKeyStore(Files.readAllBytes(file.toPath()), privateKey, null, "solace", "jks").store(fos, "password".toCharArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void writeKeyStore() {
        Path resources = Paths.get("src", "test", "resources");
        String absolutePath = resources.toFile().getAbsolutePath();
        File file = new File(absolutePath + "/clientCert1.pem");
        File keyFile = new File(absolutePath + "/client1.key");
        try {
            File yourFile = new File(absolutePath +"/solace_keystore.jks");
            if(!yourFile.exists()) {
                yourFile.createNewFile();
            }
            FileOutputStream fos = new FileOutputStream(yourFile);
            PrivateKey privateKey = loadPrivateKey(keyFile.getAbsolutePath());
            createKeyStore(Files.readAllBytes(file.toPath()), privateKey, null, "solace-client", "jks").store(fos, "password".toCharArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Method to load the private key from a PEM file
    private static PrivateKey loadPrivateKey(String privateKeyPath) throws Exception {
        try (FileReader keyReader = new FileReader(privateKeyPath)) {
            PEMParser pemParser = new PEMParser(keyReader);
            PrivateKeyInfo privateKeyInfo = (PrivateKeyInfo) pemParser.readObject();
            JcaPEMKeyConverter converter = new JcaPEMKeyConverter();
            return converter.getPrivateKey(privateKeyInfo);
        }
    }

    private static KeyStore createKeyStore(byte[] ca, PrivateKey key, byte[] serviceCa, String name, String instance) {
        try {
            KeyStore keyStore = KeyStore.getInstance(instance);
            keyStore.load(null);
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            if (ca != null) {
                X509Certificate cert = (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(ca));
                keyStore.setCertificateEntry(name, cert);
                if(key != null) {
                    keyStore.setKeyEntry(name + "-key", key , "password".toCharArray() ,new Certificate[]{cert});
                }
            }
            if (serviceCa != null) {
                keyStore.setCertificateEntry("service-ca",
                        cf.generateCertificate(new ByteArrayInputStream(serviceCa)));
            }
            return keyStore;
        } catch (Exception ignored) {
            return null;
        }
    }

    public void stop() {
        solaceOAuthContainer.stop();
    }

    public boolean isRunning() {
        return solaceOAuthContainer.isRunning();
    }


    public SolaceOAuthContainer getSolaceOAuthContainer() {
        return solaceOAuthContainer;
    }
}