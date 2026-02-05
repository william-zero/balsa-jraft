package com.balsajraft.transport;

import java.io.IOException;

/**
 * Network transport for Raft RPC.
 * The default impl uses TCP with virtual threads, but you can swap in
 * your own for testing or different network stacks.
 */
public interface Transport {

    /** Start listening on the given port */
    void start(int port) throws IOException;

    /**
     * Send a message to another node.
     * Fire and forget - delivery is best effort, Raft handles retries.
     */
    void send(Object message, String destinationId);

    /** Register the handler that receives incoming messages */
    void registerHandler(TransportHandler handler);

    void stop() throws IOException;

    interface TransportHandler {
        void handleMessage(Object message, String senderId);
    }
}
