package com.mongodb.dao.mongo;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ConnectionReplicas {

    private final Logger logger = Logger.getLogger(this.getClass().getName());

    private Boolean enabled;
    private Set<ConnectionAddress> replicaAddresses;

    public ConnectionReplicas(Set<ConnectionAddress> replicaAddresses, Boolean enabled) {
        this.replicaAddresses = replicaAddresses;
        this.enabled = enabled;
    }

    public ConnectionReplicas(Set<ConnectionAddress> replicaAddresses) {
        this(replicaAddresses, !replicaAddresses.isEmpty());
    }

    public ConnectionReplicas(String replicaAddresses) {
        this(new ReplicasDefinition(replicaAddresses).replicasSet());
    }

    public String replicasTarget() {
        return (enabled && !replicaAddresses.isEmpty()) ?
                ("," +
                        String.join(",", replicaAddresses.stream().map(
                                ra -> ra.urlServerAddress()).collect(Collectors.toCollection(LinkedHashSet::new)))
                ) : "";
    }

}
