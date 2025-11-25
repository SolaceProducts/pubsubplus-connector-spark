package com.solacecoe.connectors.spark.oauth;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.Ulimit;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.Pair;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class SolaceOAuthContainer extends GenericContainer<SolaceOAuthContainer> {

    public static final String INTEGRATION_TEST_QUEUE_NAME = "integration-test-queue";
    public static final String INTEGRATION_TEST_QUEUE_SUBSCRIPTION = "quarkus/integration/test/provisioned/queue/topic";

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("solace/solace-pubsub-standard");

    private static final String DEFAULT_VPN = "default";

    private static final String DEFAULT_USERNAME = "default";

    private static final String SOLACE_READY_MESSAGE = ".*Running pre-startup checks:.*";

    private static final String SOLACE_ACTIVE_MESSAGE = "Primary Virtual Router is now active";

    private static final String TMP_SCRIPT_LOCATION = "/tmp/script.cli";

    private static final Long SHM_SIZE = (long) Math.pow(1024, 3);

    private String username = "root";

    private String password = "password";

    private String vpn = DEFAULT_VPN;

    private final List<Pair<String, Service>> publishTopicsConfiguration = new ArrayList<>();
    private final List<Pair<String, Service>> subscribeTopicsConfiguration = new ArrayList<>();

    private boolean withClientCert;
    private boolean withOAuth;
    private boolean clientCertificateAuthority;
    private boolean withCNAsUsernameSource;

    /**
     * Create a new solace container with the specified image name.
     *
     * @param dockerImageName the image name that should be used.
     */
    public SolaceOAuthContainer(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public SolaceOAuthContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);
        Ulimit ulimit = new Ulimit("nofile", 2448, 1048576);
        List<Ulimit> ulimitList = new ArrayList<>();
        ulimitList.add(ulimit);
        withCreateContainerCmdModifier(cmd -> {
            cmd.withUser("1000");
            cmd.getHostConfig()
                    .withShmSize(SHM_SIZE)
                    .withUlimits(ulimitList)
                    .withCpuCount(1l);
        });
        this.waitStrategy = Wait.forLogMessage(SOLACE_READY_MESSAGE, 1).withStartupTimeout(Duration.ofSeconds(60));
        withExposedPorts(8080);
        withEnv("system_scaling_maxconnectioncount", "100");
        withEnv("logging_system_output", "all");
        withEnv("username_admin_globalaccesslevel", "admin");
        withEnv("username_admin_password", "admin");
        withNetwork(KeyCloakContainer.network);
        withNetworkAliases("solace");
    }

    @Override
    protected void configure() {
        withCopyToContainer(createConfigurationScript(), TMP_SCRIPT_LOCATION);
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        executeCommand("chown 1000:0 -R /var/lib/solace");
        if (withClientCert) {
            executeCommand("cp", "/tmp/solace.pem", "/usr/sw/jail/certs/solace.pem");
            executeCommand("cp", "/tmp/rootCA.crt", "/usr/sw/jail/certs/rootCA.crt");
        }
        executeCommand("cp", TMP_SCRIPT_LOCATION, "/usr/sw/jail/cliscripts/script.cli");
        waitOnCommandResult(SOLACE_ACTIVE_MESSAGE, "grep", "-R", SOLACE_ACTIVE_MESSAGE, "/usr/sw/jail/logs/system.log");
        executeCommand("/usr/sw/loads/currentload/bin/cli", "-A", "-es", "script.cli");
    }

    private Transferable createConfigurationScript() {
        StringBuilder scriptBuilder = new StringBuilder();
        updateConfigScript(scriptBuilder, "enable");
        updateConfigScript(scriptBuilder, "configure");

        // telemetry configuration
        updateConfigScript(scriptBuilder, "message-vpn default");
        updateConfigScript(scriptBuilder, "create telemetry-profile trace");
        updateConfigScript(scriptBuilder, "trace");
        updateConfigScript(scriptBuilder, "no shutdown");
        updateConfigScript(scriptBuilder, "create filter default");
        updateConfigScript(scriptBuilder, "no shutdown");
        updateConfigScript(scriptBuilder, "create subscription \">\"");
        updateConfigScript(scriptBuilder, "end");

        updateConfigScript(scriptBuilder, "configure");
        // create replay log
        updateConfigScript(scriptBuilder, "message-spool message-vpn default");
        updateConfigScript(scriptBuilder, "create replay-log integration-test-replay-log");
        updateConfigScript(scriptBuilder, "max-spool-usage 10");
        updateConfigScript(scriptBuilder, "no shutdown");
        updateConfigScript(scriptBuilder, "exit");
        updateConfigScript(scriptBuilder, "exit");

        // Create Queue
        updateConfigScript(scriptBuilder, "message-spool message-vpn default");
        updateConfigScript(scriptBuilder, "create queue " + INTEGRATION_TEST_QUEUE_NAME);
        updateConfigScript(scriptBuilder, "access-type exclusive");
        updateConfigScript(scriptBuilder, "subscription topic " + INTEGRATION_TEST_QUEUE_SUBSCRIPTION);
        updateConfigScript(scriptBuilder, "max-spool-usage 300");
        updateConfigScript(scriptBuilder, "permission all consume");
        updateConfigScript(scriptBuilder, "no shutdown");
        updateConfigScript(scriptBuilder, "exit");
        updateConfigScript(scriptBuilder, "exit");

        // Create VPN if not default
        if (!vpn.equals(DEFAULT_VPN)) {
            updateConfigScript(scriptBuilder, "create message-vpn " + vpn);
            updateConfigScript(scriptBuilder, "no shutdown");
            updateConfigScript(scriptBuilder, "exit");
        }

        // Configure username and password
        if (username.equals(DEFAULT_USERNAME)) {
            throw new RuntimeException("Cannot override password for default client");
        }
        updateConfigScript(scriptBuilder, "create client-username " + username + " message-vpn " + vpn);
        updateConfigScript(scriptBuilder, "password " + password);
        updateConfigScript(scriptBuilder, "acl-profile default");
        updateConfigScript(scriptBuilder, "client-profile default");
        updateConfigScript(scriptBuilder, "no shutdown");
        updateConfigScript(scriptBuilder, "exit");

        updateConfigScript(scriptBuilder, "create client-username certificate-user message-vpn " + vpn);
        updateConfigScript(scriptBuilder, "acl-profile default");
        updateConfigScript(scriptBuilder, "client-profile default");
        updateConfigScript(scriptBuilder, "no shutdown");
        updateConfigScript(scriptBuilder, "exit");

        updateConfigScript(scriptBuilder, "create client-username certificate-user-with-password message-vpn " + vpn);
        updateConfigScript(scriptBuilder, "password certificate-user-with-password");
        updateConfigScript(scriptBuilder, "acl-profile default");
        updateConfigScript(scriptBuilder, "client-profile default");
        updateConfigScript(scriptBuilder, "no shutdown");
        updateConfigScript(scriptBuilder, "exit");

        updateConfigScript(scriptBuilder, "create client-username localhost message-vpn " + vpn);
        updateConfigScript(scriptBuilder, "acl-profile default");
        updateConfigScript(scriptBuilder, "client-profile default");
        updateConfigScript(scriptBuilder, "no shutdown");
        updateConfigScript(scriptBuilder, "exit");

        if (withClientCert) {
            if (clientCertificateAuthority) {
                updateConfigScript(scriptBuilder, "configure");
                // Client certificate authority configuration
                updateConfigScript(scriptBuilder, "authentication");
                updateConfigScript(scriptBuilder, "create client-certificate-authority RootCA");
                updateConfigScript(scriptBuilder, "certificate file rootCA.crt");
                updateConfigScript(scriptBuilder, "show client-certificate-authority RootCA *");
                updateConfigScript(scriptBuilder, "end");

                updateConfigScript(scriptBuilder, "configure");
                updateConfigScript(scriptBuilder, "message-vpn " + vpn);
                // Enable client certificate authentication
                updateConfigScript(scriptBuilder, "authentication client-certificate");
                if(!withCNAsUsernameSource) {
                    updateConfigScript(scriptBuilder, "allow-api-provided-username");
                }
                updateConfigScript(scriptBuilder, "no shutdown");
                updateConfigScript(scriptBuilder, "end");
            } else {
                updateConfigScript(scriptBuilder, "configure");
                // Domain certificate authority configuration
                updateConfigScript(scriptBuilder, "ssl");
                updateConfigScript(scriptBuilder, "create domain-certificate-authority RootCA");
                updateConfigScript(scriptBuilder, "certificate file rootCA.crt");
                updateConfigScript(scriptBuilder, "show domain-certificate-authority RootCA *");
                updateConfigScript(scriptBuilder, "end");
            }

            // Server certificates configuration
            updateConfigScript(scriptBuilder, "configure");
            updateConfigScript(scriptBuilder, "ssl");
            updateConfigScript(scriptBuilder, "server-certificate solace.pem");
            updateConfigScript(scriptBuilder, "cipher-suite msg-backbone name AES128-SHA");
            updateConfigScript(scriptBuilder, "exit");
            updateConfigScript(scriptBuilder, "end");

        }

        if (withOAuth) {
            // Configure OAuth authentication
            updateConfigScript(scriptBuilder, "configure");
            updateConfigScript(scriptBuilder, "message-vpn " + vpn);
            updateConfigScript(scriptBuilder, "authentication oauth");
            updateConfigScript(scriptBuilder, "no shutdown");
            updateConfigScript(scriptBuilder, "end");

            // Configure VPN Basic authentication
            updateConfigScript(scriptBuilder, "configure");
            updateConfigScript(scriptBuilder, "message-vpn " + vpn);
            updateConfigScript(scriptBuilder, "authentication basic auth-type internal");
            updateConfigScript(scriptBuilder, "no shutdown");
            updateConfigScript(scriptBuilder, "end");
        }

        // create OAuth profile
        updateConfigScript(scriptBuilder, "configure");
        updateConfigScript(scriptBuilder, "message-vpn " + vpn);
        updateConfigScript(scriptBuilder, "authentication");
        updateConfigScript(scriptBuilder, "create oauth profile integration_test_oauth_profile");
        updateConfigScript(scriptBuilder, "authorization-groups-claim-name \"\" ");
        updateConfigScript(scriptBuilder, "oauth-role resource-server");
        updateConfigScript(scriptBuilder, "issuer https://localhost:7778/realms/solace");
        updateConfigScript(scriptBuilder, "disconnect-on-token-expiration");
        updateConfigScript(scriptBuilder, "endpoints");
        updateConfigScript(scriptBuilder, "discovery https://keycloak:8443/realms/solace/.well-known/openid-configuration");
        updateConfigScript(scriptBuilder, "exit");
        updateConfigScript(scriptBuilder, "username-claim-name sub");
        updateConfigScript(scriptBuilder, "resource-server");
        updateConfigScript(scriptBuilder, "required-audience pubsub+aud");
        updateConfigScript(scriptBuilder, "no validate-type");
        updateConfigScript(scriptBuilder, "required-type JWT");
        updateConfigScript(scriptBuilder, "exit");
        updateConfigScript(scriptBuilder, "client-id solace");
        updateConfigScript(scriptBuilder, "client-secret solace-secret");
        updateConfigScript(scriptBuilder, "no shutdown");
        updateConfigScript(scriptBuilder, "end");

        if (!publishTopicsConfiguration.isEmpty() || !subscribeTopicsConfiguration.isEmpty()) {
            // Enable services
            updateConfigScript(scriptBuilder, "configure");
            // Configure default ACL
            updateConfigScript(scriptBuilder, "acl-profile default message-vpn " + vpn);
            // Configure default action to disallow
            if (!subscribeTopicsConfiguration.isEmpty()) {
                updateConfigScript(scriptBuilder, "subscribe-topic default-action disallow");
            }
            if (!publishTopicsConfiguration.isEmpty()) {
                updateConfigScript(scriptBuilder, "publish-topic default-action disallow");
            }
            updateConfigScript(scriptBuilder, "exit");

            updateConfigScript(scriptBuilder, "message-vpn " + vpn);
            updateConfigScript(scriptBuilder, "service");
            for (Pair<String, Service> topicConfig : publishTopicsConfiguration) {
                Service service = topicConfig.getValue();
                String topicName = topicConfig.getKey();
                updateConfigScript(scriptBuilder, service.getName());
                if (service.isSupportSSL()) {
                    if (withClientCert) {
                        updateConfigScript(scriptBuilder, "ssl");
                    } else {
                        updateConfigScript(scriptBuilder, "plain-text");
                    }
                }
                updateConfigScript(scriptBuilder, "no shutdown");
                updateConfigScript(scriptBuilder, "end");
                // Add publish/subscribe topic exceptions
                updateConfigScript(scriptBuilder, "configure");
                updateConfigScript(scriptBuilder, "acl-profile default message-vpn " + vpn);
                updateConfigScript(
                        scriptBuilder,
                        String.format("publish-topic exceptions %s list %s", service.getName(), topicName));
                updateConfigScript(scriptBuilder, "end");
            }

            updateConfigScript(scriptBuilder, "configure");
            updateConfigScript(scriptBuilder, "message-vpn " + vpn);
            updateConfigScript(scriptBuilder, "service");
            for (Pair<String, Service> topicConfig : subscribeTopicsConfiguration) {
                Service service = topicConfig.getValue();
                String topicName = topicConfig.getKey();
                updateConfigScript(scriptBuilder, service.getName());
                if (service.isSupportSSL()) {
                    if (withClientCert) {
                        updateConfigScript(scriptBuilder, "ssl");
                    } else {
                        updateConfigScript(scriptBuilder, "plain-text");
                    }
                }
                updateConfigScript(scriptBuilder, "no shutdown");
                updateConfigScript(scriptBuilder, "end");
                // Add publish/subscribe topic exceptions
                updateConfigScript(scriptBuilder, "configure");
                updateConfigScript(scriptBuilder, "acl-profile default message-vpn " + vpn);
                updateConfigScript(
                        scriptBuilder,
                        String.format("subscribe-topic exceptions %s list %s", service.getName(), topicName));
                updateConfigScript(scriptBuilder, "end");
            }
        }
        return Transferable.of(scriptBuilder.toString());
    }

    private void executeCommand(String... command) {
        try {
            ExecResult execResult = execInContainer(command);
            if (execResult.getExitCode() != 0) {
                logCommandError(execResult.getStderr(), command);
            }
        } catch (IOException | InterruptedException e) {
            logCommandError(e.getMessage(), command);
        }
    }

    private void updateConfigScript(StringBuilder scriptBuilder, String command) {
        scriptBuilder.append(command).append("\n");
    }

    private void waitOnCommandResult(String waitingFor, String... command) {
        Awaitility
                .await()
                .pollInterval(Duration.ofMillis(500))
                .timeout(Duration.ofSeconds(30))
                .until(() -> {
                    try {
                        return execInContainer(command).getStdout().contains(waitingFor);
                    } catch (IOException | InterruptedException e) {
                        logCommandError(e.getMessage(), command);
                        return true;
                    }
                });
    }

    private void logCommandError(String error, String... command) {
        logger().error("Could not execute command {}: {}", command, error);
    }

    /**
     * Sets the client credentials
     *
     * @param username Client username
     * @param password Client password
     * @return This container.
     */
    public SolaceOAuthContainer withCredentials(final String username, final String password) {
        this.username = username;
        this.password = password;
        return this;
    }

    /**
     * Adds the topic configuration
     *
     * @param topic Name of the topic
     * @param service Service to be supported on provided topic
     * @return This container.
     */
    //    public SolaceContainer withTopic(String topic, Service service) {
    //        topicsConfiguration.add(Pair.of(topic, service));
    //        addExposedPort(service.getPort());
    //        return this;
    //    }

    /**
     * Adds the publish topic exceptions configuration
     *
     * @param topic Name of the topic
     * @param service Service to be supported on provided topic
     * @return This container.
     */
    public SolaceOAuthContainer withPublishTopic(String topic, Service service) {
        publishTopicsConfiguration.add(Pair.of(topic, service));
        addExposedPort(service.getPort());
        return this;
    }

    /**
     * Adds the subscribe topic exceptions configuration
     *
     * @param topic Name of the topic
     * @param service Service to be supported on provided topic
     * @return This container.
     */
    public SolaceOAuthContainer withSubscribeTopic(String topic, Service service) {
        subscribeTopicsConfiguration.add(Pair.of(topic, service));
        addExposedPort(service.getPort());
        return this;
    }

    /**
     * Sets the VPN name
     *
     * @param vpn VPN name
     * @return This container.
     */
    public SolaceOAuthContainer withVpn(String vpn) {
        this.vpn = vpn;
        return this;
    }

    /**
     * Sets the solace server ceritificates
     *
     * @param certFile Server certificate
     * @param caFile Certified Authority ceritificate
     * @return This container.
     */
    public SolaceOAuthContainer withClientCert(final MountableFile certFile, final MountableFile caFile,
                                               boolean clientCertificateAuthority) {
        this.withClientCert = true;
        this.clientCertificateAuthority = clientCertificateAuthority;
        return withCopyFileToContainer(certFile, "/tmp/solace.pem").withCopyFileToContainer(caFile, "/tmp/rootCA.crt");
    }

    /**
     * Sets OAuth authentication
     */
    public SolaceOAuthContainer withOAuth() {
        this.withOAuth = true;
        return this;
    }

    /**
     * Sets Common Name as username source
     */
    public SolaceOAuthContainer withCNAsUsernameSource() {
        this.withCNAsUsernameSource = true;
        return this;
    }

    /**
     * Configured VPN
     *
     * @return the configured VPN that should be used for connections
     */
    public String getVpn() {
        return this.vpn;
    }

    /**
     * Host address for provided service
     *
     * @param service - service for which host needs to be retrieved
     * @return host address exposed from the container
     */
    public String getOrigin(Service service) {
        return String.format("%s://%s:%s", service.getProtocol(), getHost(), getMappedPort(service.getPort()));
    }

    /**
     * Configured username
     *
     * @return the standard username that should be used for connections
     */
    public String getUsername() {
        return this.username;
    }

    /**
     * Configured password
     *
     * @return the standard password that should be used for connections
     */
    public String getPassword() {
        return this.password;
    }

    public enum Service {
        AMQP("amqp", 5672, "amqp", false),
        MQTT("mqtt", 1883, "tcp", false),
        REST("rest", 9000, "http", false),
        SMF("smf", 55555, "tcp", true),
        SMF_SSL("smf", 55443, "tcps", true);

        private final String name;
        private final Integer port;
        private final String protocol;
        private final boolean supportSSL;

        Service(String name, Integer port, String protocol, boolean supportSSL) {
            this.name = name;
            this.port = port;
            this.protocol = protocol;
            this.supportSSL = supportSSL;
        }

        /**
         * @return Port assigned for the service
         */
        public Integer getPort() {
            return this.port;
        }

        /**
         * @return Protocol of the service
         */
        public String getProtocol() {
            return this.protocol;
        }

        /**
         * @return Name of the service
         */
        public String getName() {
            return this.name;
        }

        /**
         * @return Is SSL for this service supported ?
         */
        public boolean isSupportSSL() {
            return this.supportSSL;
        }

        public static void main(String[] args) {
            SolaceOAuthContainer solaceOAuthContainer = new SolaceOAuthContainer("solace/solace-pubsub-standard:latest");
            solaceOAuthContainer.withCredentials("user", "pass")
                    .withClientCert(MountableFile.forClasspathResource("solace.pem"),
                            MountableFile.forClasspathResource("keycloak.crt"), false)
                    .withOAuth()
                    .withExposedPorts(Service.SMF.getPort(), Service.SMF_SSL.getPort(), 1943, 8080)
                    .withPublishTopic("hello/direct", Service.SMF)
                    .withPublishTopic("hello/persistent", Service.SMF)
                    .withPublishTopic("solace/spark/connector/offset", SolaceOAuthContainer.Service.SMF);

            solaceOAuthContainer.start();

            while(true) {

            }
        }
    }
}
