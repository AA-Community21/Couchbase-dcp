package com.tcket;

import com.couchbase.client.dcp.highlevel.*;
import com.couchbase.client.dcp.highlevel.internal.CollectionsManifest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class CouchbaseDataListener implements DatabaseChangeListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseDataListener.class);

    @Override
    public void onMutation(Mutation mutation) {
            handleChange(mutation);
    }

    @Override
    public void onDeletion(Deletion deletion) {
        handleChange(deletion);
    }

    @Override
    public void onFailure(StreamFailure streamFailure) {
        LOGGER.error("Stream failure occurred: {}", streamFailure.getCause().toString());
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
}
