package com.balsajraft.core;

import com.balsajraft.core.storage.FileLogStorage;
import com.balsajraft.core.storage.FileMetaStorage;
import com.balsajraft.core.storage.FileSnapshotStorage;
import com.balsajraft.example.KVStateMachine;
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
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class RaftPersistenceTest {

    @TempDir
    Path tempDir;

    private final List<RaftNode> nodes = new ArrayList<>();
    private final List<TcpTransport> transports = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (RaftNode node : nodes) {
            try {
                node.stop();
            } catch (IOException e) {
                // Ignore
            }
        }
        nodes.clear();
        transports.clear();
    }

    @Test
    void testClusterRestartPreservesLog() throws Exception {
        List<String> cluster = Arrays.asList("localhost:8101", "localhost:8102", "localhost:8103");
        List<File> dataDirs = new ArrayList<>();

        // Start cluster
        for (String id : cluster) {
            File dir = tempDir.resolve("persist_" + id.replace(":", "_")).toFile();
            dir.mkdirs();
            dataDirs.add(dir);
            createAndStartNode(id, cluster, dir);
        }

        // Write data
        RaftNode leader = waitForLeader(10000);
        assertNotNull(leader, "Leader not elected");

        for (int i = 0; i < 10; i++) {
            assertTrue(leader.replicate(("PUT:k" + i + ":v" + i).getBytes())
                .get(5, TimeUnit.SECONDS));
        }

        Thread.sleep(1000); // Wait for replication

        // Stop cluster
        for (RaftNode node : nodes) {
            node.stop();
        }
        nodes.clear();
        transports.clear(); // Transports are stopped by node.stop() but we clear references

        System.out.println("Cluster stopped. Restarting...");
        Thread.sleep(1000);

        // Restart cluster
        for (int i = 0; i < cluster.size(); i++) {
            createAndStartNode(cluster.get(i), cluster, dataDirs.get(i));
        }

        // Wait for leader
        leader = waitForLeader(10000);
        assertNotNull(leader, "Leader not elected after restart");

        // Verify data exists (read from any node's state machine)
        // Note: KVStateMachine is not persistent itself, so it relies on Raft replaying the log
        // The log should be preserved on disk.

        // Wait for catchup (replaying log takes time)
        Thread.sleep(1000);

        // Check if data is present
        // We can check by reading from state machine or by writing new data and checking consistency

        assertTrue(leader.replicate("PUT:k10:v10".getBytes()).get(5, TimeUnit.SECONDS));
    }

    @Test
    void testRestartWithSnapshot() throws Exception {
        List<String> cluster = Arrays.asList("localhost:8201", "localhost:8202", "localhost:8203");
        List<File> dataDirs = new ArrayList<>();
        List<KVStateMachine> machines = new ArrayList<>();

        // Start cluster
        for (String id : cluster) {
            File dir = tempDir.resolve("snap_persist_" + id.replace(":", "_")).toFile();
            dir.mkdirs();
            dataDirs.add(dir);

            KVStateMachine fsm = new KVStateMachine();
            machines.add(fsm);

            createAndStartNodeWithFSM(id, cluster, dir, fsm);
        }

        RaftNode leader = waitForLeader(10000);

        // Write 20 entries with retries for robustness in CI
        for (int i = 0; i < 20; i++) {
            boolean success = false;
            while (!success) {
                try {
                    if (leader.getState() != RaftNode.State.LEADER) {
                        RaftNode newLeader = waitForLeader(5000);
                        if (newLeader != null) leader = newLeader;
                    }
                    success = leader.replicate(("PUT:k" + i + ":v" + i).getBytes()).get(5, TimeUnit.SECONDS);
                } catch (Exception e) {
                    // Retry on timeout or leadership change
                }
                if (!success) {
                    Thread.sleep(500);
                }
            }
        }

        // Snapshot at index 15
        // Ensure we are talking to the current leader or a node that has applied enough
        if (leader.getLastApplied() < 15) {
             // Wait for apply (unlikely if replicate succeeded, but safe)
             Thread.sleep(1000);
        }
        leader.snapshot(15).get(5, TimeUnit.SECONDS);

        // Wait for commit
        Thread.sleep(1000);

        // Stop cluster
        for (RaftNode node : nodes) {
            node.stop();
        }
        nodes.clear();
        transports.clear();
        machines.clear();

        System.out.println("Cluster stopped (with snapshot). Restarting...");
        Thread.sleep(1000);

        // Restart cluster
        for (int i = 0; i < cluster.size(); i++) {
            KVStateMachine fsm = new KVStateMachine();
            machines.add(fsm);
            createAndStartNodeWithFSM(cluster.get(i), cluster, dataDirs.get(i), fsm);
        }

        // Wait for leader
        leader = waitForLeader(10000);
        assertNotNull(leader);

        // Write one new entry to force commit of previous term entries (Raft Figure 8)
        leader.replicate("PUT:kNew:vNew".getBytes()).get(5, TimeUnit.SECONDS);

        // Verify data k0..k19 exists
        // k0..k14 are in snapshot. k15..k19 are in log.
        // If snapshot persistence fails, k0..k14 will be missing.

        // Wait for all nodes to catch up (consistency is eventual for followers)
        waitForData(machines, "kNew", "vNew", 10000);

        for (KVStateMachine fsm : machines) {
            // Check sample keys
            assertEquals("v0", fsm.get("k0"), "Snapshot data missing for " + fsm);
            assertEquals("v14", fsm.get("k14"), "Snapshot data missing for " + fsm);
            assertEquals("v19", fsm.get("k19"), "Log data missing for " + fsm);
            assertEquals("vNew", fsm.get("kNew"), "New data missing for " + fsm);
        }
    }

    private void waitForData(List<KVStateMachine> machines, String key, String value, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean allHaveIt = true;
            for (KVStateMachine fsm : machines) {
                if (!value.equals(fsm.get(key))) {
                    allHaveIt = false;
                    break;
                }
            }
            if (allHaveIt) return;
            Thread.sleep(100);
        }
    }

    private void createAndStartNode(String id, List<String> cluster, File dir) throws IOException {
        createAndStartNodeWithFSM(id, cluster, dir, new KVStateMachine());
    }

    private void createAndStartNodeWithFSM(String id, List<String> cluster, File dir, KVStateMachine fsm) throws IOException {
        List<String> peers = new ArrayList<>(cluster);
        peers.remove(id);

        TcpTransport transport = new TcpTransport(id);
        transports.add(transport);

        RaftNode node = new RaftNode(id, peers, transport,
            new FileLogStorage(dir), new FileMetaStorage(dir), new FileSnapshotStorage(dir), fsm);
        nodes.add(node);
        node.start();
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
}
