package com.solacecoe.connectors.spark.oauth;

import org.testcontainers.utility.MountableFile;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;

import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

public class ContainerResource {
    KeyCloakContainer keyCloakContainer;
    private SolaceOAuthContainer solaceOAuthContainer;

    public void start() {
        writeJks();

        keyCloakContainer = new KeyCloakContainer();
        keyCloakContainer.start();
        keyCloakContainer.createHostsFile();
        await().until(() -> keyCloakContainer.isRunning());
        solaceOAuthContainer = new SolaceOAuthContainer("solace/solace-pubsub-standard:latest");
        solaceOAuthContainer.withCredentials("user", "pass")
                .withClientCert(MountableFile.forClasspathResource("solace.pem"),
                        MountableFile.forClasspathResource("keycloak.crt"), false)
                .withOAuth()
                .withExposedPorts(SolaceOAuthContainer.Service.SMF.getPort(), SolaceOAuthContainer.Service.SMF_SSL.getPort(), 1943, 8080)
                .withPublishTopic(SolaceOAuthContainer.INTEGRATION_TEST_QUEUE_SUBSCRIPTION, SolaceOAuthContainer.Service.SMF)
                .withPublishTopic("random/topic", SolaceOAuthContainer.Service.SMF);

        solaceOAuthContainer.start();
        await().until(() -> solaceOAuthContainer.isRunning());

    }

    private static void writeJks() {
        File file = new File(ContainerResource.class.getResource("/keycloak.crt").getFile());
        try {
            FileOutputStream fos = new FileOutputStream("target/keycloak.jks");
            createKeyStore(Files.readAllBytes(file.toPath()), null).store(fos, "password".toCharArray());
        } catch (IOException | CertificateException | KeyStoreException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private static KeyStore createKeyStore(byte[] ca, byte[] serviceCa) {
        try {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(null);
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            if (ca != null) {
                keyStore.setCertificateEntry("keycloak",
                        cf.generateCertificate(new ByteArrayInputStream(ca)));
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
        keyCloakContainer.stop();
        solaceOAuthContainer.stop();
    }

    public boolean isRunning() {
        return keyCloakContainer.isRunning() && solaceOAuthContainer.isRunning();
    }

    public KeyCloakContainer getKeyCloakContainer() {
        return keyCloakContainer;
    }

    public SolaceOAuthContainer getSolaceOAuthContainer() {
        return solaceOAuthContainer;
    }
}