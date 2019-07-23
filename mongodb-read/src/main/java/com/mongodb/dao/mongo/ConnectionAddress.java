package com.mongodb.dao.mongo;

import com.mongodb.ServerAddress;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ConnectionAddress {

    private final Logger logger = Logger.getLogger(this.getClass().getName());

    private String host;
    private int port;


    public ConnectionAddress(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public ConnectionAddress(String serverAddress) {
        this.host = serverAddress;
        this.port = -1;
    }

    public ServerAddress serverAddress() {
        if (port == -1 && host.contains(":")) {
            try {
                String[] addressParts = host.split(":");
                host = addressParts[0];
                port = Integer.parseInt(addressParts[1]);
            } catch (NumberFormatException e) {
                logger.log(Level.WARNING, "Host port isn't an Integer.", e);
            } catch (IndexOutOfBoundsException e) {
                logger.log(Level.WARNING, "Host port isn't defined.", e);
            } catch (NullPointerException e) {
                logger.log(Level.WARNING, "Bad Host:Port definition.", e);
            }
        }
        return (port > -1 ? new ServerAddress(host, port) : new ServerAddress(host));
    }

    public String urlServerAddress() {
        return serverAddress().toString();
    }

}
