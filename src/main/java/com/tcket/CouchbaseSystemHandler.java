package com.tcket;

import com.couchbase.client.dcp.SystemEventHandler;
import com.couchbase.client.dcp.core.event.CouchbaseEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CouchbaseSystemHandler implements SystemEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseSystemHandler.class);

    @Override
    public void onEvent(CouchbaseEvent event) {
//        LOGGER.info("SystemEventHandler: {}", event.toMap());
    }
}
