package com.solacecoe.connectors.spark.containers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.time.Duration;

public class SparkWorkerContainer extends GenericContainer<SparkWorkerContainer> {
    public SparkWorkerContainer(boolean copyCerts) {
        super("apache/spark:3.5.2");
        addFixedExposedPort(8087, 8081);
        addEnv("SPARK_MASTER_URL", "spark://spark-master:7077");
        addEnv("SPARK_WORKER_MEMORY", "16G");
        addEnv("SPARK_WORKER_CORES", "4");
        addEnv("SPARK_WORKER_JAVA_OPTS", "-Djava.net.preferIPv4Stack=true");

        withCommand(
                "/opt/spark/bin/spark-class",
                "org.apache.spark.deploy.worker.Worker",
                "spark://spark-master:7077"
        );

        if(copyCerts) {
            withCopyFileToContainer(MountableFile.forClasspathResource("keycloak.crt"), "/opt/spark/work-dir/");
            withCopyFileToContainer(MountableFile.forClasspathResource("keycloak.key"), "/opt/spark/work-dir/");
//            withCopyFileToContainer(MountableFile.forClasspathResource("solace.jks"), "/opt/spark/work-dir/");
//            withCopyFileToContainer(MountableFile.forClasspathResource("solace_keystore.jks"), "/opt/spark/work-dir/");
        }

        withNetwork(SparkContainer.network);
        Wait.forLogMessage(".*Successfully started service.*", 1).withStartupTimeout(Duration.ofSeconds(60));
    }
}

