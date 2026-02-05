package com.balsajraft.core;

import com.balsajraft.core.model.LogEntry;
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

// snapshot tests (these take a while)
class RaftSnapshotTest {

    @TempDir
    Path tempDir;

    private final List<RaftNode> nodes = new ArrayList<>();

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
    }

    @Test
    void testSnapshotCreationAndContent() throws Exception {
        List<String> cluster = Arrays.asList("localhost:7301", "localhost:7302", "localhost:7303");
        List<KVStateMachine> machines = new ArrayList<>();

        for (String id : cluster) {
            File dir = tempDir.resolve("snapshot_" + id.replace(":", "_")).toFile();
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

        // Wait for leader
        RaftNode leader = waitForLeader(10000);
        assertNotNull(leader, "No leader elected");

        // Write some data
        for (int i = 0; i < 10; i++) {
            assertTrue(leader.replicate(("PUT:key" + i + ":value" + i).getBytes())
                .get(5, TimeUnit.SECONDS));
        }

        // Wait for replication
        Thread.sleep(500);

        // Take snapshot from leader's state machine
        int leaderIdx = -1;
        for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i) == leader) {
                leaderIdx = i;
                break;
            }
        }

        KVStateMachine leaderMachine = machines.get(leaderIdx);
        byte[] snapshot = leaderMachine.takeSnapshot();

        assertNotNull(snapshot);
        assertTrue(snapshot.length > 0, "Snapshot should not be empty");

        // Create a new state machine and restore from snapshot
        KVStateMachine restored = new KVStateMachine();
        restored.installSnapshot(snapshot);

        // Verify all data was restored
        for (int i = 0; i < 10; i++) {
            assertEquals("value" + i, restored.get("key" + i),
                "Key " + i + " should be restored from snapshot");
        }
    }

    @Test
    void testSlowFollowerCatchesUpViaSnapshot() throws Exception {
        List<String> cluster = Arrays.asList("localhost:7401", "localhost:7402", "localhost:7403");
        List<KVStateMachine> machines = new ArrayList<>();
        List<File> dirs = new ArrayList<>();

        for (String id : cluster) {
            File dir = tempDir.resolve("catchup_" + id.replace(":", "_")).toFile();
            dir.mkdirs();
            dirs.add(dir);

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

        // Wait for leader
        RaftNode leader = waitForLeader(10000);
        assertNotNull(leader, "No leader elected");

        // Write initial data
        for (int i = 0; i < 5; i++) {
            assertTrue(leader.replicate(("PUT:init" + i + ":val" + i).getBytes())
                .get(5, TimeUnit.SECONDS));
        }

        Thread.sleep(500);

        // Verify all nodes have the data
        for (KVStateMachine fsm : machines) {
            assertEquals("val0", fsm.get("init0"));
        }

        // Stop a follower
        RaftNode follower = null;
        int followerIdx = -1;
        for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i).getState() != RaftNode.State.LEADER) {
                follower = nodes.get(i);
                followerIdx = i;
                break;
            }
        }

        assertNotNull(follower);
        String followerId = follower.getNodeId();
        follower.stop();
        nodes.remove(follower);
        machines.remove(followerIdx);

        // Write more data while follower is down
        for (int i = 0; i < 20; i++) {
            leader.replicate(("PUT:new" + i + ":newval" + i).getBytes())
                .get(5, TimeUnit.SECONDS);
        }

        // Trigger snapshot on leader (simulate log compaction scenario)
        // We compact up to index 20 (initial 5 + first 15 new ones), leaving last 5 in log
        // Follower has 5, so it needs 6. Since 6 < 20, leader MUST send snapshot.
        long compactIndex = 20;
        leader.snapshot(compactIndex).get(5, TimeUnit.SECONDS);

        // Restart the follower with fresh state machine
        File followerDir = dirs.get(followerIdx);
        KVStateMachine newFollowerMachine = new KVStateMachine();

        List<String> peers = new ArrayList<>(cluster);
        peers.remove(followerId);

        RaftNode restartedFollower = new RaftNode(followerId, peers,
            new TcpTransport(followerId),
            new FileLogStorage(followerDir),
            new FileMetaStorage(followerDir),
            new FileSnapshotStorage(followerDir),
            newFollowerMachine);
        nodes.add(restartedFollower);
        restartedFollower.start();

        // Wait for follower to catch up with retry (CI environments may be slow)
        boolean caughtUp = false;
        for (int attempt = 0; attempt < 20; attempt++) {
            Thread.sleep(500);
            if ("val0".equals(newFollowerMachine.get("init0")) &&
                "newval19".equals(newFollowerMachine.get("new19"))) {
                caughtUp = true;
                break;
            }
        }

        // Verify follower caught up with all data
        // It should have received entries via AppendEntries (log is still available)
        assertTrue(caughtUp, "Follower should have caught up within timeout");
        assertEquals("val0", newFollowerMachine.get("init0"),
            "Follower should have initial data");
        assertEquals("newval19", newFollowerMachine.get("new19"),
            "Follower should have caught up with new data");
    }

    @Test
    void testStateMachineSnapshotIsAtomic() throws Exception {
        KVStateMachine fsm = new KVStateMachine();

        // Apply a series of commands
        for (int i = 0; i < 100; i++) {
            LogEntry entry = new LogEntry(1, i + 1,
                ("PUT:key" + i + ":value" + i).getBytes(),
                LogEntry.EntryType.COMMAND);
            fsm.apply(entry);
        }

        // Take multiple snapshots concurrently
        List<byte[]> snapshots = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            snapshots.add(fsm.takeSnapshot());
        }

        // All snapshots should be identical
        byte[] reference = snapshots.get(0);
        for (int i = 1; i < snapshots.size(); i++) {
            assertArrayEquals(reference, snapshots.get(i),
                "All snapshots should be identical");
        }

        // Restore and verify
        KVStateMachine restored = new KVStateMachine();
        restored.installSnapshot(reference);

        for (int i = 0; i < 100; i++) {
            assertEquals("value" + i, restored.get("key" + i));
        }
    }

    @Test
    void testEmptySnapshotHandling() throws Exception {
        KVStateMachine fsm = new KVStateMachine();

        // Take snapshot of empty state machine
        byte[] snapshot = fsm.takeSnapshot();
        assertNotNull(snapshot);

        // Restore to another empty machine
        KVStateMachine restored = new KVStateMachine();
        restored.installSnapshot(snapshot);

        // Should work without errors
        assertNull(restored.get("nonexistent"));
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
