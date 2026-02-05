package com.balsajraft.core;

import com.balsajraft.core.model.LogEntry;
import com.balsajraft.core.storage.FileLogStorage;
import com.balsajraft.core.storage.FileMetaStorage;
import com.balsajraft.core.storage.FileSnapshotStorage;
import com.balsajraft.core.storage.LogStorage;
import com.balsajraft.example.KVStateMachine;
import com.balsajraft.transport.TcpTransport;
import com.balsajraft.transport.Transport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies Raft safety properties and invariants using external inspection.
 * Replaces internal invariant checks that were removed from RaftNode.
 *
 * These are the properties from section 5.4 of the paper
 */
class RaftInvariantsTest {

    @TempDir
    Path tempDir;

    private final List<RaftNode> nodes = new ArrayList<>();
    private final List<Transport> transports = new ArrayList<>();

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
    void testElectionSafety() throws Exception {
        // Start 5 nodes
        createCluster(5);

        // Allow multiple elections
        long deadline = System.currentTimeMillis() + 10000;
        while (System.currentTimeMillis() < deadline) {
            verifyInvariants(nodes);
            Thread.sleep(100);
        }
    }

    @Test
    void testLogMatchingAndConsistency() throws Exception {
        createCluster(3);
        RaftNode leader = waitForLeader(10000);
        assertNotNull(leader, "Leader expected");

        // Replicate some commands
        for (int i = 0; i < 20; i++) {
            leader.replicate(("CMD" + i).getBytes());
            if (i % 5 == 0) {
                Thread.sleep(50); // Give time for partial replication
                verifyInvariants(nodes);
            }
        }

        // Wait for convergence
        Thread.sleep(1000);
        verifyInvariants(nodes);
    }

    @Test
    void testLeaderCompletenessWithChurn() throws Exception {
        // This test forces leader changes and checks that committed entries are preserved
        createCluster(3);

        RaftNode leader = waitForLeader(10000);
        assertNotNull(leader);

        // Write data
        leader.replicate("IMPORTANT_DATA".getBytes()).get(5, TimeUnit.SECONDS);
        long committedIdx = leader.getCommitIndex();

        verifyInvariants(nodes);

        // Stop leader to force new election
        leader.stop();
        nodes.remove(leader);

        Thread.sleep(2000); // Wait for new leader

        RaftNode newLeader = waitForLeader(10000);
        assertNotNull(newLeader);
        assertNotSame(leader, newLeader);

        // New leader must have the committed entry
        LogStorage storage = getField(newLeader, "logStorage");

        // Wait for storage to be consistent (retry for CI robustness)
        LogEntry entry = null;
        for (int i = 0; i < 10; i++) {
            entry = storage.getEntry(committedIdx);
            if (entry != null) break;
            Thread.sleep(500);
        }

        assertNotNull(entry, "New leader " + newLeader.getNodeId() + " missing committed index " + committedIdx);
        assertEquals("IMPORTANT_DATA", new String(entry.getCommand()));

        verifyInvariants(nodes);
    }

    // ============================================================================================
    // Invariant Verification Logic
    // ============================================================================================

    private void verifyInvariants(List<RaftNode> cluster) throws Exception {
        assertElectionSafety(cluster);
        assertLogMatching(cluster);
        assertCommitIndexSafety(cluster);
    }

    /**
     * Election Safety: At most one leader per term.
     */
    private void assertElectionSafety(List<RaftNode> cluster) {
        Map<Long, List<String>> leadersPerTerm = new HashMap<>();

        for (RaftNode node : cluster) {
            if (node.getState() == RaftNode.State.LEADER) {
                long term = node.getCurrentTerm();
                leadersPerTerm.computeIfAbsent(term, k -> new ArrayList<>()).add(node.getNodeId());
            }
        }

        for (Map.Entry<Long, List<String>> entry : leadersPerTerm.entrySet()) {
            assertTrue(entry.getValue().size() <= 1,
                "Term " + entry.getKey() + " has multiple leaders: " + entry.getValue());
        }
    }

    /**
     * Log Matching:
     * 1. If two logs contain an entry with the same index and term, then the logs are identical in all preceding entries.
     * 2. If two entries in different logs have the same index and term, then they store the same command.
     */
    private void assertLogMatching(List<RaftNode> cluster) throws Exception {
        List<LogStorage> logs = new ArrayList<>();
        for (RaftNode node : cluster) {
            logs.add(getField(node, "logStorage"));
        }

        for (int i = 0; i < logs.size(); i++) {
            for (int j = i + 1; j < logs.size(); j++) {
                compareLogs(logs.get(i), logs.get(j), cluster.get(i).getNodeId(), cluster.get(j).getNodeId());
            }
        }
    }

    private void compareLogs(LogStorage log1, LogStorage log2, String id1, String id2) throws IOException {
        long minIndex = Math.min(log1.getFirstIndex(), log2.getFirstIndex());
        long maxIndex = Math.max(log1.getLastIndex(), log2.getLastIndex());

        for (long idx = minIndex; idx <= maxIndex; idx++) {
            LogEntry e1 = log1.getEntry(idx);
            LogEntry e2 = log2.getEntry(idx);

            // If both have entry at idx
            if (e1 != null && e2 != null) {
                // If terms match, everything before must match (recursive check, or iterative up to here)
                // But specifically: if index and term match, command must match
                if (e1.getTerm() == e2.getTerm()) {
                    assertArrayEquals(e1.getCommand(), e2.getCommand(),
                        "Log mismatch at index " + idx + " term " + e1.getTerm() + " between " + id1 + " and " + id2);

                    // Also check prefix consistency (Log Matching Property 1)
                    // We check immediate predecessor
                    if (idx > 1) {
                         LogEntry p1 = log1.getEntry(idx - 1);
                         LogEntry p2 = log2.getEntry(idx - 1);
                         if (p1 != null && p2 != null) {
                             assertEquals(p1.getTerm(), p2.getTerm(),
                                 "Log Matching violation: entries at " + idx + " match, but predecessors at " + (idx-1) + " diverge in term (" + id1 + " vs " + id2 + ")");
                         }
                    }
                }
            }
        }
    }

    /**
     * Commit Index Safety:
     * - Leader's commit index <= Last Log Index
     * - (Stronger) If a leader has committed an entry, a majority must have it.
     */
    private void assertCommitIndexSafety(List<RaftNode> cluster) throws Exception {
        for (RaftNode node : cluster) {
            long commitIndex = node.getCommitIndex();
            LogStorage log = getField(node, "logStorage");

            // Basic sanity
            assertTrue(commitIndex <= log.getLastIndex(),
                "Node " + node.getNodeId() + " commitIndex " + commitIndex + " > lastLogIndex " + log.getLastIndex());

            // If node is leader, check majority replication for committed entries
            if (node.getState() == RaftNode.State.LEADER) {
                // This is hard to check perfectly without accessing matchIndex map,
                // but we can check if the entry at commitIndex exists on majority
                if (commitIndex > 0) {
                    LogEntry entry = log.getEntry(commitIndex);
                    if (entry != null && entry.getTerm() == node.getCurrentTerm()) {
                        int count = 0;
                        for (RaftNode peer : cluster) {
                            LogStorage peerLog = getField(peer, "logStorage");
                            LogEntry peerEntry = peerLog.getEntry(commitIndex);
                            if (peerEntry != null && peerEntry.getTerm() == entry.getTerm()) {
                                count++;
                            }
                        }
                        int quorum = (cluster.size() / 2) + 1; // Simplification (works if all nodes in test list are the cluster)
                        // Note: cluster list might be partial if we removed nodes, so use config size if possible.
                        // For these tests, nodes.size() is the full cluster.
                        assertTrue(count >= (nodes.size() / 2) + 1,
                            "Leader has committed index " + commitIndex + " but it is not on majority. Found on " + count + "/" + nodes.size());
                    }
                }
            }
        }
    }

    // ============================================================================================
    // Helpers
    // ============================================================================================

    @SuppressWarnings("unchecked")
    private <T> T getField(Object target, String fieldName) throws Exception {
        Field f = target.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        return (T) f.get(target);
    }

    private void createCluster(int size) throws IOException {
        List<String> addresses = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            addresses.add("localhost:" + (9000 + i));
        }

        for (String addr : addresses) {
            File dir = tempDir.resolve("inv_" + addr.replace(":", "_")).toFile();
            dir.mkdirs();

            List<String> peers = new ArrayList<>(addresses);
            peers.remove(addr);

            TcpTransport transport = new TcpTransport(addr);
            transports.add(transport);

            RaftNode node = new RaftNode(addr, peers, transport,
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
}
