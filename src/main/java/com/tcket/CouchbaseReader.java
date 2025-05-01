package com.tcket;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.highlevel.*;
import com.couchbase.client.dcp.highlevel.internal.CollectionIdAndKey;
import com.couchbase.client.dcp.highlevel.internal.CollectionsManifest;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

public class CouchbaseReader extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseReader.class);

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

//        registerListener();

        client.dataEventHandler((flowController, event) -> {
            handleChange(event);
            flowController.ack(event);
        });

        start();
    }

    private void handleChange(ByteBuf event) {
        if(DcpMutationMessage.is(event)) {
            String key = MessageUtil.getKeyAsString(event);
            String content = MessageUtil.getContentAsString(event);
            CollectionIdAndKey collecionId =  MessageUtil.getCollectionIdAndKey(event, true);
            collecionId.collectionId();
            collecionId.key();
            LOGGER.info("Received DCP change {}, {}, {}, {} : {}", key, collecionId.collectionId(), collecionId.key(), content);

        }
    }

    private void registerListener() {
        client.nonBlockingListener(new DatabaseChangeListener() {
            @Override
            public void onMutation(Mutation mutation) {
                handleChange(mutation);
            }

            @Override
            public void onDeletion(Deletion deletion) {
                handleChange(deletion);
            }

            @Override
            public void onFailure(StreamFailure failure) {
                LOGGER.error("Stream failure occurred: {}", failure.getCause().toString());
            }

            @Override
            public void onStreamEnd(StreamEnd streamEnd) {
                LOGGER.info("Stream ended: {}", streamEnd.getReason());
            }

            private void handleChange(DocumentChange change) {
                try {
                    String content = new String(change.getContent(), StandardCharsets.UTF_8);

                    CollectionsManifest.CollectionInfo collectionInfo = change.getCollection();


                    LOGGER.info("Collection ID: {}, Collection Name: {}, Scope: {}",
                            collectionInfo.id(), collectionInfo.name(), collectionInfo.scope());

                    LOGGER.info("Received DCP change {}, {}, {}, {} : {}",change.getKey(), change.getCollection(), change.getTimestamp(), change.getVbucket(), content);


                } catch (Exception ex) {
                    LOGGER.error("Error processing DCP change", ex);
                }
            }
        });
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