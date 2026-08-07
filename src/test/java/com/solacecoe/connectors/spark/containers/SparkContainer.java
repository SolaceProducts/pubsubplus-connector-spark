package com.solacecoe.connectors.spark.containers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Optional;

public class SparkContainer extends GenericContainer<SparkContainer> {
    public static Network network = Network.newNetwork();
    public SparkContainer(boolean copyKeyCloakCerts, boolean copySolaceCerts) throws IOException {
        super("apache/spark:4.0.0");
        addFixedExposedPort(8080, 8080);
        addFixedExposedPort(7077, 7077);
        addFixedExposedPort(4040, 4040);
        addFixedExposedPort(6066, 6066);

        addEnv("SPARK_RPC_AUTHENTICATION_ENABLED", "no");
        addEnv("HOME", "/tmp");
        addEnv("KRB5_CONFIG", "");
        addEnv("KRB5CCNAME", "");
        addEnv("JAVA_TOOL_OPTIONS", "");
        addEnv("SPARK_MASTER_OPTS", "-Dspark.master.rest.enabled=true");

        // ✅ Start Spark Master explicitly
        withCommand(
                "/opt/spark/bin/spark-class",
                "org.apache.spark.deploy.master.Master",
                "--host", "spark-master",
                "--port", "7077",
                "--webui-port", "8080"
        );

        // Path to the Maven target directory
        Path targetDir = Paths.get("target");

        // Find the jar starting with your artifact name (no version hardcoded)
        Optional<Path> jarPath = Files.list(targetDir)
                .filter(f -> f.getFileName().toString().startsWith("pubsubplus-connector-spark")
                        && f.getFileName().toString().endsWith(".jar") && !f.getFileName().toString().contains("javadoc") && !f.getFileName().toString().contains("sources"))
                .findFirst();

        // Copy the jar into the container
        jarPath.ifPresent(path -> withCopyFileToContainer(
                MountableFile.forHostPath(path.toString()), "/opt/spark/jars/pubsubplus-connector-spark.jar"));

        withCopyFileToContainer(MountableFile.forClasspathResource("SolaceSparkSource.py"), "/opt/spark/work-dir/");
        withCopyFileToContainer(MountableFile.forClasspathResource("SolaceSparkSink.py"), "/opt/spark/work-dir/");
        withCopyFileToContainer(MountableFile.forClasspathResource("SolaceSparkSink_ForEachBatch.py"), "/opt/spark/work-dir/");
        withCopyFileToContainer(MountableFile.forClasspathResource("SolaceSparkSourceOAuth.py"), "/opt/spark/work-dir/");
        withCopyFileToContainer(MountableFile.forClasspathResource("SolaceSparkSourceTLS.py"), "/opt/spark/work-dir/");
        if(copyKeyCloakCerts) {
            withCopyFileToContainer(MountableFile.forClasspathResource("keycloak.crt"), "/opt/spark/work-dir/");
            withCopyFileToContainer(MountableFile.forClasspathResource("keycloak.key"), "/opt/spark/work-dir/");
        }
        if(copySolaceCerts) {
            Path solacejks = Paths.get(System.getProperty("java.io.tmpdir"), "solace.jks");
            String solaceJksAbsolutePath = solacejks.toFile().getAbsolutePath();

            Path solaceKeyStore = Paths.get(System.getProperty("java.io.tmpdir"), "solace_keystore.jks");
            String solaceKeyStoreAbsolutePath = solaceKeyStore.toFile().getAbsolutePath();

            withCopyFileToContainer(MountableFile.forHostPath(solaceJksAbsolutePath), "/opt/spark/work-dir/");
            withCopyFileToContainer(MountableFile.forHostPath(solaceKeyStoreAbsolutePath), "/opt/spark/work-dir/");
        }
        withNetwork(network);
        withNetworkAliases("spark-master");
        Wait.forLogMessage(".*Successfully started service.*", 1).withStartupTimeout(Duration.ofSeconds(60));
    }
}

