package com.mongodb.startup;

import com.mongodb.dao.MongoDAO;

import javax.annotation.PostConstruct;
import javax.ejb.DependsOn;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.stream.Collectors;

@Singleton
@Startup
@DependsOn("ConfigurationService")
public class ApplicationStartedService {

    @Inject
    MongoDAO mongoDAO;

    @PostConstruct
    public void init() {
        mongoDAO.verifyDBConnection();
        try (BufferedReader buff = new BufferedReader(
                new InputStreamReader(getClass().getClassLoader().getResourceAsStream("start/icon.txt"), Charset.forName("UTF-8")))) {
            System.out.println(buff.lines().collect(Collectors.joining("\n")));
        } catch (IOException e) {
            System.out.println(e.getMessage());
            System.out.println("Transformer has been started successfully");
        }
        System.out.println("Transformer has been started successfully");
    }
}