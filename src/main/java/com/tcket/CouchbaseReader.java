package com.tcket;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.highlevel.*;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.concurrent.CountDownLatch;

public class CouchbaseReader extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseReader.class);

    @Inject
    @Qualifier("databaseChangeListener")
    private DatabaseChangeListener couchbaseDataListener;

    private final Client client;
    private final CountDownLatch connectionAttemptComplete = new CountDownLatch(1);
    private volatile boolean connected;

    public CouchbaseReader(String connectionString, String username, String password, String bucketName, String collectionName) {
        client = Client.builder()
                .seedNodes(connectionString)
                .credentials(username, password)
                .bucket(bucketName)
                .collectionsAware(true)
                .scopeName(bucketName)
                .build();
    }

    @PostConstruct
    public void init() {
        registerListener();
        start();
    }


    private void registerListener() {
        client.nonBlockingListener(couchbaseDataListener);
    }

    @Override
    public void run() {
        try {
            LOGGER.info("Connecting to Couchbase...");
            client.connect().block();
            connected = true;
            LOGGER.info("Connected. Initializing state...");

            client.initializeState(StreamFrom.BEGINNING, StreamTo.INFINITY).block();    // <-- no StateFormat needed now
            LOGGER.info("State initialized. Starting streaming...");

            client.startStreaming().block();     // <-- no StateFormat needed now
            LOGGER.info("DCP streaming started successfully.");
        } catch (Exception ex) {
            LOGGER.error("Error during Couchbase DCP setup", ex);
        } finally {
            connectionAttemptComplete.countDown();
        }
    }


    public void shutdown() {
        try {
            if (connected) {
                LOGGER.info("Disconnecting Couchbase DCP Client...");
                client.disconnect().block();
                LOGGER.info("Disconnected successfully.");
            }
        } catch (Exception e) {
            LOGGER.error("Failed to disconnect DCP client.", e);
        }
    }
}
