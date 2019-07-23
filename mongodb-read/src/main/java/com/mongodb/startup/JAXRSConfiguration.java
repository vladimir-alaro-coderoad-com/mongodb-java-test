package com.mongodb.startup;

import com.mongodb.util.Constants;
import org.eclipse.microprofile.openapi.annotations.OpenAPIDefinition;
import org.eclipse.microprofile.openapi.annotations.info.Info;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.server.spi.Container;
import org.glassfish.jersey.server.spi.ContainerLifecycleListener;

import javax.ws.rs.ApplicationPath;

@OpenAPIDefinition(
        info = @Info(title = "mongodb-read", version = "1.0.0", description = "Application developed for tests with mongodb")
)
@ApplicationPath("")
public class JAXRSConfiguration extends ResourceConfig {

    static ResourceConfig resourceConfig;
    static Container container;
    public static ClassLoader classLoader;

    public JAXRSConfiguration() {
        property(ServerProperties.OUTBOUND_CONTENT_LENGTH_BUFFER, 0);
        packages(Constants.DEFAULT_PACKAGE);
        if (resourceConfig == null) {
            registerReloader(this);
            classLoader = this.getClassLoader();
        }
    }

    private void registerReloader(ResourceConfig resourceConfig) {
        JAXRSConfiguration.resourceConfig = resourceConfig;
        resourceConfig.registerInstances(new ContainerLifecycleListener() {
            @Override
            public void onStartup(Container container) {
                JAXRSConfiguration.container = container;
            }

            @Override
            public void onReload(Container container) {
            }

            @Override
            public void onShutdown(Container container) {

            }
        });
    }
}
