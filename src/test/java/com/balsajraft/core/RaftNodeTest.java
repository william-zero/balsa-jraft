package com.balsajraft.core;

import com.balsajraft.core.model.*;
import com.balsajraft.core.storage.LogStorage;
import com.balsajraft.core.storage.MetaStorage;
import com.balsajraft.core.storage.SnapshotStorage;
import com.balsajraft.transport.Transport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

// basic unit tests with mocks
// integration tests are in other files
class RaftNodeTest {

    private RaftNode raftNode;
    private MockTransport transport;
    private MockLogStorage logStorage;
    private MockMetaStorage metaStorage;
    private MockSnapshotStorage snapshotStorage;
    private MockStateMachine stateMachine;

    @BeforeEach
    void setUp() throws IOException {
        transport = new MockTransport();
        logStorage = new MockLogStorage();
        metaStorage = new MockMetaStorage();
        snapshotStorage = new MockSnapshotStorage();
        stateMachine = new MockStateMachine();

        raftNode = new RaftNode("localhost:8001", Collections.singletonList("localhost:8002"),
                transport, logStorage, metaStorage, snapshotStorage, stateMachine);
        raftNode.start();
    }

    @AfterEach
    void tearDown() throws IOException {
        raftNode.stop();
    }

    @Test
    void testStartAsFollower() {
        // Fresh node should timeout and ask for votes

        RequestVote msg = transport.waitForMessage(RequestVote.class);
        assertNotNull(msg);
        assertEquals(1, msg.getTerm());
        assertEquals("localhost:8001", msg.getCandidateId());
    }

    @Test
    void testBecomeLeader() throws InterruptedException {
        // Wait for election round
        RequestVote vote = transport.waitForMessage(RequestVote.class);

        // Respond with vote
        RequestVoteResponse response = new RequestVoteResponse(1, true);
        raftNode.handleMessage(response, "node2");

        // New leader should emit heartbeat quickly
        AppendEntries hb = transport.waitForMessage(AppendEntries.class);
        assertNotNull(hb);
        assertEquals(1, hb.getTerm());
        assertTrue(hb.getEntries().isEmpty());
    }

    // Mocks for unit tests

    static class MockTransport implements Transport {
        private TransportHandler handler;
        private final BlockingQueue<Object> messages = new LinkedBlockingQueue<>();

        @Override
        public void start(int port) {}

        @Override
        public void send(Object message, String destinationId) {
            messages.offer(message);
        }

        @Override
        public void registerHandler(TransportHandler handler) {
            this.handler = handler;
        }

        @Override
        public void stop() {}

        public <T> T waitForMessage(Class<T> type) {
            try {
                Object msg = messages.poll(2, TimeUnit.SECONDS);
                if (type.isInstance(msg)) return type.cast(msg);
            } catch (InterruptedException e) {}
            return null;
        }
    }

    static class MockLogStorage implements LogStorage {
        private long lastIndex = 0;
        private long lastTerm = 0;

        @Override public void append(LogEntry entry) { lastIndex = entry.getIndex(); lastTerm = entry.getTerm(); }
        @Override public LogEntry getEntry(long index) { return null; }
        @Override public long getLastIndex() { return lastIndex; }
        @Override public long getLastTerm() { return lastTerm; }
        @Override public void truncate(long index) { lastIndex = index -1; }
        @Override public void compact(long index) {}
        @Override public void reset(long index) { lastIndex = index - 1; }
        @Override public long getFirstIndex() { return 1; }
        @Override public void close() {}
    }

    static class MockMetaStorage implements MetaStorage {
        @Override public void save(long term, String votedFor) {}
        @Override public MetaState load() { return new MetaState(0, null); }
    }

    static class MockSnapshotStorage implements SnapshotStorage {
        @Override public void save(long index, long term, byte[] data) {}
        @Override public byte[] load() { return null; }
        @Override public long getLastSnapshotIndex() { return 0; }
        @Override public long getLastSnapshotTerm() { return 0; }
    }

    static class MockStateMachine implements StateMachine {
        @Override public void apply(LogEntry entry) {}
        @Override public byte[] takeSnapshot() { return new byte[0]; }
        @Override public void installSnapshot(byte[] snapshot) {}
    }
}
