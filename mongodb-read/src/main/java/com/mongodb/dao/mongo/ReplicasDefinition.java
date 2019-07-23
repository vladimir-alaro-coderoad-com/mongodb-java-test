package com.mongodb.dao.mongo;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.Socket;
import java.nio.channels.IllegalBlockingModeException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ReplicasDefinition {

    private final Logger logger = Logger.getLogger(this.getClass().getName());

    String replicasDefinition;

    public ReplicasDefinition(String replicasDefinition) {
        this.replicasDefinition = replicasDefinition;
    }

    public Set<ConnectionAddress> replicasSet() {
        Set<ConnectionAddress> connectionAddresses = new LinkedHashSet<>();
        String[] mongoAddress = replicasDefinition.isEmpty()? new String[0]: replicasDefinition.split(",");
        SocketFactory socketFactory = SSLSocketFactory.getDefault();
        for (String hostPortAddress : mongoAddress) {
            try {
                ConnectionAddress connectionAddress = new ConnectionAddress(hostPortAddress);
                Socket socket = socketFactory.createSocket();
                socket.connect(connectionAddress.serverAddress().getSocketAddress());
                socket.close();
                connectionAddresses.add(connectionAddress);
            } catch (IllegalBlockingModeException e) {
                logger.log(Level.SEVERE, hostPortAddress + " mongo replica socket has an associated channel," +
                        " and the channel is in non-blocking mode.", e);
            } catch (IOException e) {
                logger.log(Level.SEVERE, "An error occurs during the connection to mongo replica.", e);
            }
        }
        return connectionAddresses;
    }

}
