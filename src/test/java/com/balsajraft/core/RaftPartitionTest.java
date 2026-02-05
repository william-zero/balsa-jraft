package com.balsajraft.core;

import com.balsajraft.core.model.LogEntry;
import com.balsajraft.core.storage.FileLogStorage;
import com.balsajraft.core.storage.FileMetaStorage;
import com.balsajraft.core.storage.FileSnapshotStorage;
import com.balsajraft.example.KVStateMachine;
import com.balsajraft.transport.Transport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

// Network partition scenarios
// These are intentionally integration-heavy and a little slow
// TODO: add test for leader in minority partition stepping down
class RaftPartitionTest {

    @TempDir
    Path tempDir;

    private final List<RaftNode> nodes = new ArrayList<>();
    private final List<PartitionableTransport> transports = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (RaftNode node : nodes) {
            try {
                node.stop();
            } catch (IOException e) {
                // best effort cleanup
                }
        }
        nodes.clear();
        transports.clear();
    }

    @Test
    void testMinorityPartitionCannotElectLeader() throws Exception {
        // Create 5-node cluster
        List<String> cluster = Arrays.asList(
            "localhost:7001", "localhost:7002", "localhost:7003",
            "localhost:7004", "localhost:7005"
        );

        createCluster(cluster);

        // Wait for initial leader
        RaftNode leader = waitForLeader(10000);
        assertNotNull(leader, "No initial leader elected");
        String leaderId = leader.getNodeId();

        // Partition: isolate 2 non-leader nodes (minority)
        // We must pick nodes that are NOT the current leader
        List<String> followers = new ArrayList<>();
        for (String id : cluster) {
            if (!id.equals(leaderId)) {
                followers.add(id);
            }
        }
        String node1 = followers.get(0);
        String node2 = followers.get(1);

        // Find transports for isolated nodes
        PartitionableTransport t1 = findTransport(node1);
        PartitionableTransport t2 = findTransport(node2);

        // Isolate node1 and node2 from everyone except each other
        for (String peer : cluster) {
            if (!peer.equals(node1) && !peer.equals(node2)) {
                t1.blockPeer(peer);
                t2.blockPeer(peer);
                findTransport(peer).blockPeer(node1);
                findTransport(peer).blockPeer(node2);
            }
        }

        // Let election timeout fire in the minority partition
        Thread.sleep(1500);

        // Minority should not be able to elect a leader
        RaftNode n1 = findNode(node1);
        RaftNode n2 = findNode(node2);

        // They may flap as candidates but should never become leaders
        assertNotEquals(RaftNode.State.LEADER, n1.getState(),
            "Minority node should not become leader");
        assertNotEquals(RaftNode.State.LEADER, n2.getState(),
            "Minority node should not become leader");

        // Majority partition should still have the leader
        assertEquals(RaftNode.State.LEADER, leader.getState(),
            "Original leader in majority should remain leader");
    }

    @Test
    void testLeaderIsolationCausesNewElection() throws Exception {
        List<String> cluster = Arrays.asList(
            "localhost:7101", "localhost:7102", "localhost:7103"
        );

        createCluster(cluster);

        // Wait for initial leader
        RaftNode leader = waitForLeader(10000);
        assertNotNull(leader, "No initial leader elected");
        String oldLeaderId = leader.getNodeId();
        long oldTerm = leader.getCurrentTerm();

        // Isolate the leader from all peers
        PartitionableTransport leaderTransport = findTransport(oldLeaderId);
        for (String peer : cluster) {
            if (!peer.equals(oldLeaderId)) {
                leaderTransport.blockPeer(peer);
                findTransport(peer).blockPeer(oldLeaderId);
            }
        }

        // Wait for new election in majority partition
        Thread.sleep(2000);

        // Find new leader in majority
        RaftNode newLeader = null;
        for (RaftNode node : nodes) {
            if (!node.getNodeId().equals(oldLeaderId) &&
                node.getState() == RaftNode.State.LEADER) {
                newLeader = node;
                break;
            }
        }

        assertNotNull(newLeader, "New leader should be elected after partition");
        assertNotEquals(oldLeaderId, newLeader.getNodeId(),
            "New leader should be different from isolated leader");
        assertTrue(newLeader.getCurrentTerm() > oldTerm,
            "New term should be higher");

        // Old leader may briefly think it still owns the world
        RaftNode oldLeaderNode = findNode(oldLeaderId);

        // Heal partition
        for (String peer : cluster) {
            if (!peer.equals(oldLeaderId)) {
                leaderTransport.unblockPeer(peer);
                findTransport(peer).unblockPeer(oldLeaderId);
            }
        }

        // Wait for old leader to discover new term and step down
        Thread.sleep(1000);

        assertNotEquals(RaftNode.State.LEADER, oldLeaderNode.getState(),
            "Old leader should step down after partition heals");
    }

    @Test
    void testWritesDuringPartitionOnlySucceedOnMajority() throws Exception {
        List<String> cluster = Arrays.asList(
            "localhost:7201", "localhost:7202", "localhost:7203"
        );

        List<KVStateMachine> machines = new ArrayList<>();

        for (String id : cluster) {
            File dir = tempDir.resolve("partition_write_" + id.replace(":", "_")).toFile();
            dir.mkdirs();

            KVStateMachine fsm = new KVStateMachine();
            machines.add(fsm);

            List<String> peers = new ArrayList<>(cluster);
            peers.remove(id);

            PartitionableTransport transport = new PartitionableTransport(id);
            transports.add(transport);

            RaftNode node = new RaftNode(id, peers, transport,
                new FileLogStorage(dir), new FileMetaStorage(dir), new FileSnapshotStorage(dir), fsm);
            nodes.add(node);
            node.start();
        }

        // Wait for leader
        RaftNode leader = waitForLeader(10000);
        assertNotNull(leader, "No leader elected");

        // Write before partition
        assertTrue(leader.replicate("PUT:key1:value1".getBytes()).get(5, TimeUnit.SECONDS));

        // Partition: isolate one follower
        String isolatedId = null;
        for (RaftNode node : nodes) {
            if (node.getState() != RaftNode.State.LEADER) {
                isolatedId = node.getNodeId();
                break;
            }
        }

        PartitionableTransport isolatedTransport = findTransport(isolatedId);
        // System.out.println("dbg isolate=" + isolatedId + " leader=" + leader.getNodeId());
        for (String peer : cluster) {
            if (!peer.equals(isolatedId)) {
                isolatedTransport.blockPeer(peer);
                findTransport(peer).blockPeer(isolatedId);
            }
        }

        // Write during partition - should still succeed (2/3 majority)
        assertTrue(leader.replicate("PUT:key2:value2".getBytes()).get(5, TimeUnit.SECONDS));

        Thread.sleep(500);

        // Verify majority has the data
        int hasKey2 = 0;
        for (int i = 0; i < machines.size(); i++) {
            if ("value2".equals(machines.get(i).get("key2"))) {
                hasKey2++;
            }
        }
        assertEquals(2, hasKey2, "Majority should have key2");

        // Heal partition
        for (String peer : cluster) {
            if (!peer.equals(isolatedId)) {
                isolatedTransport.unblockPeer(peer);
                findTransport(peer).unblockPeer(isolatedId);
            }
        }

        // Wait for catch-up with retry (CI environments may be slow)
        boolean allCaughtUp = false;
        for (int attempt = 0; attempt < 10; attempt++) {
            Thread.sleep(500);
            allCaughtUp = true;
            for (KVStateMachine fsm : machines) {
                if (!"value2".equals(fsm.get("key2"))) {
                    allCaughtUp = false;
                    break;
                }
            }
            if (allCaughtUp) break;
        }

        // Now all should have the data
        for (KVStateMachine fsm : machines) {
            assertEquals("value2", fsm.get("key2"), "All nodes should have key2 after heal");
        }
    }

    private void createCluster(List<String> cluster) throws IOException {
        for (String id : cluster) {
            File dir = tempDir.resolve("partition_" + id.replace(":", "_")).toFile();
            dir.mkdirs();

            List<String> peers = new ArrayList<>(cluster);
            peers.remove(id);

            PartitionableTransport transport = new PartitionableTransport(id);
            transports.add(transport);

            RaftNode node = new RaftNode(id, peers, transport,
                new FileLogStorage(dir), new FileMetaStorage(dir), new FileSnapshotStorage(dir), new KVStateMachine());
            nodes.add(node);
            node.start();
        }
    }

    private RaftNode waitForLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (RaftNode node : nodes) {
                if (node.getState() == RaftNode.State.LEADER) {
                    return node;
                }
            }
            Thread.sleep(100);
        }
        return null;
    }

    private PartitionableTransport findTransport(String nodeId) {
        for (PartitionableTransport t : transports) {
            if (t.getNodeId().equals(nodeId)) {
                return t;
            }
        }
        throw new IllegalArgumentException("Transport not found: " + nodeId);
    }

    private RaftNode findNode(String nodeId) {
        for (RaftNode n : nodes) {
            if (n.getNodeId().equals(nodeId)) {
                return n;
            }
        }
        throw new IllegalArgumentException("Node not found: " + nodeId);
    }

    // Transport wrapper that can simulate partitions
    static class PartitionableTransport implements Transport {
        private final String nodeId;
        private final int port;
        private final Set<String> blockedPeers = ConcurrentHashMap.newKeySet();
        private TransportHandler handler;
        private java.net.ServerSocket serverSocket;
        private volatile boolean running;
        private final Map<String, ConnectionHolder> connections = new ConcurrentHashMap<>();
        private final com.balsajraft.core.serializer.BinarySerializer serializer =
            new com.balsajraft.core.serializer.BinarySerializer();

        PartitionableTransport(String nodeId) {
            this.nodeId = nodeId;
            this.port = Integer.parseInt(nodeId.split(":")[1]);
        }

        String getNodeId() {
            return nodeId;
        }

        void blockPeer(String peer) {
            blockedPeers.add(peer);
            // Close existing connection
            ConnectionHolder holder = connections.remove(peer);
            if (holder != null) {
                holder.close();
            }
        }

        void unblockPeer(String peer) {
            blockedPeers.remove(peer);
        }

        @Override
        public void start(int port) throws IOException {
            running = true;
            serverSocket = new java.net.ServerSocket(port);

            Thread.ofVirtual().name("partition-transport-accept-" + nodeId).start(() -> {
                while (running) {
                    try {
                        java.net.Socket socket = serverSocket.accept();
                        Thread.ofVirtual().start(() -> handleConnection(socket));
                    } catch (IOException e) {
                        if (running) {
                            // Log error
                        }
                    }
                }
            });
        }

        private void handleConnection(java.net.Socket socket) {
            try {
                java.io.DataInputStream in = new java.io.DataInputStream(socket.getInputStream());
                while (running && !socket.isClosed()) {
                    String senderId = in.readUTF();

                    // Check if sender is blocked
                    if (blockedPeers.contains(senderId)) {
                        socket.close();
                        return;
                    }

                    int len = in.readInt();
                    byte[] data = new byte[len];
                    in.readFully(data);

                    Object message = serializer.deserialize(data);
                    if (handler != null) {
                        handler.handleMessage(message, senderId);
                    }
                }
            } catch (IOException e) {
                // Connection closed
            }
        }

        @Override
        public void send(Object message, String target) {
            if (blockedPeers.contains(target)) {
                return; // Silently drop - simulates network partition
            }

            try {
                ConnectionHolder holder = connections.computeIfAbsent(target, t -> {
                    try {
                        return new ConnectionHolder(t, nodeId, serializer);
                    } catch (IOException e) {
                        return null;
                    }
                });

                if (holder != null) {
                    holder.send(message);
                }
            } catch (Exception e) {
                connections.remove(target);
            }
        }

        @Override
        public void registerHandler(TransportHandler handler) {
            this.handler = handler;
        }

        @Override
        public void stop() throws IOException {
            running = false;
            if (serverSocket != null) {
                serverSocket.close();
            }
            for (ConnectionHolder holder : connections.values()) {
                holder.close();
            }
            connections.clear();
        }

        private static class ConnectionHolder {
            private final java.net.Socket socket;
            private final java.io.DataOutputStream out;
            private final String nodeId;
            private final com.balsajraft.core.serializer.BinarySerializer serializer;

            ConnectionHolder(String target, String nodeId,
                           com.balsajraft.core.serializer.BinarySerializer serializer) throws IOException {
                this.nodeId = nodeId;
                this.serializer = serializer;
                String[] parts = target.split(":");
                this.socket = new java.net.Socket();
                this.socket.connect(new java.net.InetSocketAddress(parts[0],
                    Integer.parseInt(parts[1])), 5000);
                this.out = new java.io.DataOutputStream(socket.getOutputStream());
            }

            synchronized void send(Object message) throws IOException {
                byte[] data = serializer.serialize(message);
                out.writeUTF(nodeId);
                out.writeInt(data.length);
                out.write(data);
                out.flush();
            }

            void close() {
                try {
                    socket.close();
                } catch (IOException e) {
                    // Ignore
                }
            }
        }
    }
}
