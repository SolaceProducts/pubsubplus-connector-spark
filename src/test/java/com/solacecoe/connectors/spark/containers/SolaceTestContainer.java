package com.solacecoe.connectors.spark.containers;

import com.github.dockerjava.api.model.Ulimit;
import org.testcontainers.solace.Service;
import org.testcontainers.solace.SolaceContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SolaceTestContainer extends SolaceContainer {
    private static final Long SHM_SIZE = (long) Math.pow(1024, 3);
    public SolaceTestContainer(String dockerImageName, Map<String, Service> topics) {
        super(dockerImageName);
        withCreateContainerCmdModifier(cmd ->{
            Ulimit ulimit = new Ulimit("nofile", 2448, 1048576);
            List<Ulimit> ulimitList = new ArrayList<>();
            ulimitList.add(ulimit);
            cmd.getHostConfig()
                    .withShmSize(SHM_SIZE)
                    .withUlimits(ulimitList)
                    .withCpuCount(1l);
        });
        withExposedPorts(8080, 55555);
        topics.forEach(this::withTopic);
        withNetwork(SparkContainer.network);
        withNetworkAliases("solace-broker");
    }
}
