package com.tcket;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CouchbaseEventHandler implements DataEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseEventHandler.class);

    @Override
    public void onEvent(ChannelFlowController flowController, ByteBuf event) {

//        if(DcpMutationMessage.is(event)) {
//            String content = event.toString(CharsetUtil.UTF_8);
////            DcpMutationMessage.content(event);
//            LOGGER.info("DataEventHandler Mutation: {}", content);
//
//            event.release();
//        }
//
//        if(DcpDeletionMessage.is(event)) {
//            String content = event.toString(CharsetUtil.UTF_8);
//            LOGGER.info("DataEventHandler Deletion: {}", content);
//
//            event.release();
//        }

    }
}
