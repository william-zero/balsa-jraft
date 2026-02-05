package com.balsajraft.example;

import com.balsajraft.core.RaftNode;
import com.balsajraft.core.storage.FileLogStorage;
import com.balsajraft.core.storage.FileMetaStorage;
import com.balsajraft.core.storage.FileSnapshotStorage;
import com.balsajraft.transport.TcpTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class RaftIntegrationTest {

    @TempDir
    Path tempDir;

    private final List<RaftNode> nodes = new ArrayList<>();

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
    }

    @Test
    void testCluster() throws Exception {
        // Use unique ports to avoid collisions with other tests
        List<String> cluster = Arrays.asList("localhost:6101", "localhost:6102", "localhost:6103");

        List<KVStateMachine> machines = new ArrayList<>();

        for (String id : cluster) {
            File dir = tempDir.resolve(id.replace(":", "_")).toFile();
            dir.mkdirs();

            KVStateMachine fsm = new KVStateMachine();
            machines.add(fsm);

            List<String> peers = new ArrayList<>(cluster);
            peers.remove(id);

            RaftNode node = new RaftNode(id, peers,
                    new TcpTransport(id),
                    new FileLogStorage(dir),
                    new FileMetaStorage(dir),
                    new FileSnapshotStorage(dir),
                    fsm);
            nodes.add(node);
            node.start();
        }

        // Wait for leader election
        System.out.println("Waiting for leader election...");
        RaftNode leader = null;
        for (int i = 0; i < 40; i++) {
            Thread.sleep(250);
            for (RaftNode node : nodes) {
                if (node.getState() == RaftNode.State.LEADER) {
                    leader = node;
                    break;
                }
            }
            if (leader != null) break;
        }

        assertNotNull(leader, "No leader elected within timeout");
        System.out.println("Leader elected: " + leader.getNodeId() + " at term " + leader.getCurrentTerm());

        // Write to leader
        CompletableFuture<Boolean> writeFuture = leader.replicate("PUT:key1:val1".getBytes());
        boolean writeSuccess = writeFuture.get(5, TimeUnit.SECONDS);
        assertTrue(writeSuccess, "Failed to write to leader");

        // Wait for replication
        Thread.sleep(500);

        // Check replication to all nodes
        int matchCount = 0;
        for (KVStateMachine fsm : machines) {
            if ("val1".equals(fsm.get("key1"))) {
                matchCount++;
            }
        }
        assertEquals(3, matchCount, "All nodes should have the replicated data");

        // Test multiple writes
        for (int i = 0; i < 5; i++) {
            boolean success = leader.replicate(("PUT:key" + i + ":value" + i).getBytes())
                    .get(5, TimeUnit.SECONDS);
            assertTrue(success, "Write " + i + " failed");
        }

        // Wait for replication
        Thread.sleep(500);

        // Verify all writes replicated
        for (KVStateMachine fsm : machines) {
            for (int i = 0; i < 5; i++) {
                assertEquals("value" + i, fsm.get("key" + i),
                        "Key " + i + " not replicated correctly");
            }
        }
    }

    @Test
    void testLeaderElectionAfterLeaderStop() throws Exception {
        List<String> cluster = Arrays.asList("localhost:6201", "localhost:6202", "localhost:6203");

        for (String id : cluster) {
            File dir = tempDir.resolve("stop_" + id.replace(":", "_")).toFile();
            dir.mkdirs();

            List<String> peers = new ArrayList<>(cluster);
            peers.remove(id);

            RaftNode node = new RaftNode(id, peers,
                    new TcpTransport(id),
                    new FileLogStorage(dir),
                    new FileMetaStorage(dir),
                    new FileSnapshotStorage(dir),
                    new KVStateMachine());
            nodes.add(node);
            node.start();
        }

        // Wait for initial leader
        RaftNode leader = waitForLeader(nodes, 10000);
        assertNotNull(leader, "No initial leader elected");
        String originalLeaderId = leader.getNodeId();
        System.out.println("Initial leader: " + originalLeaderId);

        // Stop the leader
        leader.stop();
        nodes.remove(leader);

        // Wait for new leader
        RaftNode newLeader = waitForLeader(nodes, 10000);
        // System.out.println("dbg term after failover=" + newLeader.getCurrentTerm());
        assertNotNull(newLeader, "No new leader elected after original leader stopped");
        assertNotEquals(originalLeaderId, newLeader.getNodeId(),
                "New leader should be different from stopped leader");
        System.out.println("New leader: " + newLeader.getNodeId());
    }

    private RaftNode waitForLeader(List<RaftNode> nodes, long timeoutMs) throws InterruptedException {
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
}
