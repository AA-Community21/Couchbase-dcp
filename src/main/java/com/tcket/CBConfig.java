package com.tcket;

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
    @Bean
    public CouchbaseReader getCouchbaseReader()
    {
        return new CouchbaseReader(connectionString, username, password, bucketName, collectionName);
    }
}

