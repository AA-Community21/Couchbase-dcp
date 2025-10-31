package com.tcket;

import com.couchbase.client.dcp.highlevel.*;
import com.couchbase.client.dcp.highlevel.internal.CollectionsManifest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CouchbaseDataListener implements DatabaseChangeListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseDataListener.class);
    public static Map<Long, String> scopeIdToName = new HashMap<>();
    public static Map<Long, String> collectionIdToName = new HashMap<>();
    private static Set<String> loggedEvents = new HashSet<>();

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

    @Override
    public void onScopeCreated(ScopeCreated scopeCreated) {
        String eventKey = "scope_created_" + scopeCreated.getScopeId();
        if (loggedEvents.contains(eventKey)) {
            return;
        }
        loggedEvents.add(eventKey);
        
        scopeIdToName.put(scopeCreated.getScopeId(), scopeCreated.getScopeName());
        LOGGER.info("Scope created: {}", scopeCreated.getScopeName());
    }

    @Override
    public void onCollectionCreated(CollectionCreated collectionCreated) {
        String eventKey = "collection_created_" + collectionCreated.getCollectionId();
        if (loggedEvents.contains(eventKey)) {
            return;
        }
        loggedEvents.add(eventKey);
        
        String scopeName = scopeIdToName.get(collectionCreated.getScopeId());
        collectionIdToName.put(collectionCreated.getCollectionId(), collectionCreated.getCollectionName());
        LOGGER.info("Collection Created: {}, for the Scope: {}", collectionCreated.getCollectionName(), scopeName);
    }

    @Override
    public void onScopeDropped(ScopeDropped scopeDropped) {
        String eventKey = "scope_dropped_" + scopeDropped.getScopeId();
        if (loggedEvents.contains(eventKey)) {
            return;
        }
        loggedEvents.add(eventKey);
        
        String scopeName = scopeIdToName.get(scopeDropped.getScopeId());
        LOGGER.info("Scope dropped: {}", scopeName);
        scopeIdToName.remove(scopeDropped.getScopeId());
    }
    @Override
    public void onCollectionDropped(CollectionDropped collectionDropped) {
        String eventKey = "collection_dropped_" + collectionDropped.getCollectionId();
        if (loggedEvents.contains(eventKey)) {
            return;
        }
        loggedEvents.add(eventKey);
        
        String scopeName = scopeIdToName.get(collectionDropped.getScopeId());
        String collectionName = collectionIdToName.get(collectionDropped.getCollectionId());
        LOGGER.info("Collection dropped: {}, for the Scope: {}", collectionName, scopeName);
        collectionIdToName.remove(collectionDropped.getCollectionId());
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
