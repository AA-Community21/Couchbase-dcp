package com.tcket;

import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.SystemEventHandler;
import com.couchbase.client.dcp.highlevel.DatabaseChangeListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CBConfig {

    @Value("${spring.couchbase.connection-string}")
    private String connectionString;

    @Value("${spring.couchbase.username}")
    private String username;

    @Value("${spring.couchbase.password}")
    private String password;

    private String bucketName = "UserService";

    private String collectionName = "UserService.UserService";

    @Bean(name = "databaseChangeListener")
    public DatabaseChangeListener getDatabaseChangeListener() {
        return new CouchbaseDataListener();
    }

    @Bean
    public ControlEventHandler getControlEventHandler() {
        return new CouchbaseControlHandler();
    }

    @Bean
    public SystemEventHandler getCouchbaseSystemHandler() {
        return new CouchbaseSystemHandler();
    }

    @Bean
    public DataEventHandler getCouchbaseEventHandler() {
        return new CouchbaseEventHandler();
    }

    @Bean
    public CouchbaseReader getCouchbaseReader() {
        return new CouchbaseReader(connectionString, username, password, bucketName, collectionName);
    }

}

