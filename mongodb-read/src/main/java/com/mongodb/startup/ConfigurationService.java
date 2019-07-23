package com.mongodb.startup;

import com.mongodb.util.PropertiesService;
import fish.payara.cluster.Clustered;

import javax.annotation.PostConstruct;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

@Clustered(callPostConstructOnAttach = false)
@Singleton
@Startup
public class ConfigurationService implements Serializable {

    private static final String FILE_PROPERTIES = "configuration.properties";
    private static final long serialVersionUID = 8791600776203532042L;
    @Inject
    transient Logger logger;
    @Inject
    transient PropertiesService propertiesService;

    @PostConstruct
    private void init() {
        logger.info("Loading properties..................");
        try (InputStream is = ConfigurationService.class.getClassLoader().getResourceAsStream(FILE_PROPERTIES)) {
            Properties localProperties = new Properties();
            localProperties.load(is);
            for (Object k : localProperties.keySet()) {
                String key = (String) k;
                String value = (String) localProperties.get(k);
                propertiesService.put(key, value);
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Local properties not found", e);
        }

        Map<String, String> environment = System.getenv();
        for (String k : environment.keySet()) {
            String v = environment.get(k);
            propertiesService.put(k, v);
            logger.log(Level.INFO, "Environment variable \"{0}\" loaded with value \"{1}\"", new Object[]{k, v});
        }
    }
}
