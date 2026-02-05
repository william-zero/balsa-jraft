package com.balsajraft.core;

import com.balsajraft.core.storage.FileLogStorage;
import com.balsajraft.core.storage.FileMetaStorage;
import com.balsajraft.core.storage.FileSnapshotStorage;
import com.balsajraft.example.KVStateMachine;
import com.balsajraft.transport.TcpTransport;
import com.balsajraft.transport.UnreliableTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class RaftUnreliableNetworkTest {

    @TempDir
    Path tempDir;

    private final List<RaftNode> nodes = new ArrayList<>();
    private final List<UnreliableTransport> transports = new ArrayList<>();

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
    void testProgressWithPacketLoss() throws Exception {
        // Start cluster with 20% packet loss (simulates a very noisy network)
        createCluster(3, 0.20);

        // Allow more time for leader election
        RaftNode leader = waitForLeader(15000);
        assertNotNull(leader, "Leader election should eventually succeed even with packet loss");

        System.out.println("Leader elected: " + leader.getNodeId());

        // Submit commands asynchronously
        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            futures.add(leader.replicate(("CMD" + i).getBytes()));
            // Space out requests slightly to avoid overwhelming the retry mechanism
            Thread.sleep(50);
        }

        // Wait and count successes
        int successCount = 0;
        for (CompletableFuture<Boolean> f : futures) {
            try {
                // 3 seconds timeout per request (loss implies retries)
                if (f.get(3, TimeUnit.SECONDS)) {
                    successCount++;
                }
            } catch (Exception e) {
                // Timeout or other error expected
            }
        }

        System.out.println("Successful replications: " + successCount + "/20");

        // We expect at least SOME progress.
        // With 20% drop rate, majority quorum (2/3) probability of success per message is:
        // P(succ) = (1-0.2)^2 = 0.64. Round trip: 0.64 * 0.64 = ~0.4.
        // So roughly 40% of RPCs succeed on first try. Retries ensure eventual consistency.

        assertTrue(leader.getCommitIndex() > 0,
            "Cluster should make progress. CommitIndex: " + leader.getCommitIndex());

        // Heal network
        System.out.println("Healing network...");
        for (UnreliableTransport t : transports) {
            t.setDropRate(0.0);
        }

        Thread.sleep(1000);

        // Verify healthy operation - should be fast now
        // CI environments can be slow, so we allow retries or a longer timeout
        boolean recovered = false;
        for (int i = 0; i < 5; i++) {
            try {
                if (leader.getState() == RaftNode.State.LEADER) {
                    leader.replicate("FINAL_CMD".getBytes()).get(5, TimeUnit.SECONDS);
                    recovered = true;
                    break;
                }
            } catch (Exception e) {
                // Ignore and retry
            }
            Thread.sleep(1000);

            // If lost leadership, find new leader
            if (leader.getState() != RaftNode.State.LEADER) {
                RaftNode newLeader = waitForLeader(5000);
                if (newLeader != null) {
                    leader = newLeader;
                }
            }
        }

        assertTrue(recovered, "Cluster should be healthy after healing");

        // Check that all replicas eventually converge
        long leaderIdx = leader.getCommitIndex();
        Thread.sleep(1000);
        for (RaftNode node : nodes) {
            // It's possible followers are slightly behind, but with healed network they should catch up fast
            assertTrue(node.getCommitIndex() >= leaderIdx - 5,
                "Follower " + node.getNodeId() + " too far behind leader (" + leaderIdx + ")");
        }
    }

    private void createCluster(int size, double dropRate) throws IOException {
        List<String> addresses = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            addresses.add("localhost:" + (9100 + i));
        }

        for (String addr : addresses) {
            File dir = tempDir.resolve("unreliable_" + addr.replace(":", "_")).toFile();
            dir.mkdirs();

            List<String> peers = new ArrayList<>(addresses);
            peers.remove(addr);

            TcpTransport realTransport = new TcpTransport(addr);
            UnreliableTransport unreliableTransport = new UnreliableTransport(realTransport);
            unreliableTransport.setDropRate(dropRate);
            transports.add(unreliableTransport);

            RaftNode node = new RaftNode(addr, peers, unreliableTransport,
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
