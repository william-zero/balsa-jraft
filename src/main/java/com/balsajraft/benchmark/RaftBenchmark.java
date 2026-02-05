package com.balsajraft.benchmark;

import com.balsajraft.core.RaftNode;
import com.balsajraft.core.StateMachine;
import com.balsajraft.core.model.LogEntry;
import com.balsajraft.core.storage.FileLogStorage;
import com.balsajraft.core.storage.FileMetaStorage;
import com.balsajraft.core.storage.FileSnapshotStorage;
import com.balsajraft.transport.TcpTransport;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

// Quick benchmark harness
// Good enough for trend checks, not meant to be a fancy perf lab
// numbers will vary a lot depending on disk speed
public class RaftBenchmark {

    private static final int BASE_PORT = 17000;
    private final int nodeCount;
    private final int requestCount;
    private final int payloadSize;
    private final int warmupCount;
    private final int concurrency;
    private final List<RaftNode> nodes = new ArrayList<>();
    private final List<File> dataDirs = new ArrayList<>();

    public RaftBenchmark(int nodeCount, int requestCount, int payloadSize, int warmupCount, int concurrency) {
        this.nodeCount = nodeCount;
        this.requestCount = requestCount;
        this.payloadSize = payloadSize;
        this.warmupCount = warmupCount;
        this.concurrency = concurrency;
    }

    public static void main(String[] args) throws Exception {
        int nodes = 3;
        int requests = 10000;
        int payload = 100;
        int warmup = 1000;
        int concurrency = 100;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--nodes" -> nodes = Integer.parseInt(args[++i]);
                case "--requests" -> requests = Integer.parseInt(args[++i]);
                case "--payload" -> payload = Integer.parseInt(args[++i]);
                case "--warmup" -> warmup = Integer.parseInt(args[++i]);
                case "--concurrency" -> concurrency = Integer.parseInt(args[++i]);
                case "--help", "-h" -> {
                    printUsage();
                    return;
                }
            }
        }

        System.out.println();
        System.out.println("╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║              RAFT CONSENSUS BENCHMARK                     ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝");
        System.out.println();

        RaftBenchmark benchmark = new RaftBenchmark(nodes, requests, payload, warmup, concurrency);
        benchmark.run();
    }

    private static void printUsage() {
        System.out.println("Usage: java -cp target/raft-core-*.jar com.raft.benchmark.RaftBenchmark [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --nodes N       Number of nodes in cluster (default: 3)");
        System.out.println("  --requests N    Number of requests to send (default: 10000)");
        System.out.println("  --payload N     Payload size in bytes (default: 100)");
        System.out.println("  --warmup N      Warmup requests (default: 1000)");
        System.out.println("  --concurrency N Concurrent in-flight requests (default: 100)");
        System.out.println("  --help, -h      Show this help message");
    }

    public void run() throws Exception {
        try {
            startCluster();
            waitForLeader();

            System.out.printf("Configuration:%n");
            System.out.printf("  Nodes:        %d%n", nodeCount);
            System.out.printf("  Requests:     %,d%n", requestCount);
            System.out.printf("  Payload:      %d bytes%n", payloadSize);
            System.out.printf("  Warmup:       %,d requests%n", warmupCount);
            System.out.printf("  Concurrency:  %d%n", concurrency);
            System.out.println();

            RaftNode leader = findLeader();
            byte[] payload = new byte[payloadSize];
            for (int i = 0; i < payloadSize; i++) {
                payload[i] = (byte) (i % 256);
            }

            // Warmup phase
            System.out.print("Warming up... ");
            for (int i = 0; i < warmupCount; i++) {
                leader.replicate(payload).get();
            }
            System.out.println("done");
            System.out.println();

            // Benchmark phase
            System.out.println("Running benchmark...");
            System.out.println();

            List<Long> latencies = java.util.Collections.synchronizedList(new ArrayList<>(requestCount));
            AtomicLong successCount = new AtomicLong(0);
            AtomicLong failureCount = new AtomicLong(0);
            AtomicLong completedCount = new AtomicLong(0);
            java.util.concurrent.Semaphore semaphore = new java.util.concurrent.Semaphore(concurrency);
            CountDownLatch latch = new CountDownLatch(requestCount);

            long startTime = System.nanoTime();

            for (int i = 0; i < requestCount; i++) {
                semaphore.acquire();
                long reqStart = System.nanoTime();

                leader.replicate(payload).whenComplete((result, error) -> {
                    long latency = System.nanoTime() - reqStart;
                    semaphore.release();

                    if (error == null && result) {
                        successCount.incrementAndGet();
                        latencies.add(latency);
                    } else {
                        failureCount.incrementAndGet();
                    }

                    long completed = completedCount.incrementAndGet();
                    if (completed % (requestCount / 10) == 0) {
                        System.out.printf("  Progress: %d%%%n", (int) ((completed * 100) / requestCount));
                    }
                    latch.countDown();
                });
            }

            latch.await();
            long totalTime = System.nanoTime() - startTime;

            // Calculate stats
            latencies.sort(Long::compareTo);
            LongSummaryStatistics stats = latencies.stream()
                .mapToLong(Long::longValue)
                .summaryStatistics();

            double throughput = (successCount.get() * 1_000_000_000.0) / totalTime;
            double avgLatencyMs = stats.getAverage() / 1_000_000.0;
            double p50 = latencies.get(latencies.size() / 2) / 1_000_000.0;
            double p95 = latencies.get((int) (latencies.size() * 0.95)) / 1_000_000.0;
            double p99 = latencies.get((int) (latencies.size() * 0.99)) / 1_000_000.0;
            double minLatency = stats.getMin() / 1_000_000.0;
            double maxLatency = stats.getMax() / 1_000_000.0;

            // Print results
            System.out.println();
            System.out.println("═══════════════════════════════════════════════════════════");
            System.out.println("                        RESULTS                            ");
            System.out.println("═══════════════════════════════════════════════════════════");
            System.out.println();
            System.out.printf("  Throughput:     %,.0f ops/sec%n", throughput);
            System.out.printf("  Total time:     %.2f sec%n", totalTime / 1_000_000_000.0);
            System.out.println();
            System.out.println("  Latency (ms):");
            System.out.printf("    Min:          %.3f%n", minLatency);
            System.out.printf("    Avg:          %.3f%n", avgLatencyMs);
            System.out.printf("    P50:          %.3f%n", p50);
            System.out.printf("    P95:          %.3f%n", p95);
            System.out.printf("    P99:          %.3f%n", p99);
            System.out.printf("    Max:          %.3f%n", maxLatency);
            System.out.println();
            System.out.printf("  Success rate:   %.2f%% (%,d/%,d)%n",
                (successCount.get() * 100.0) / requestCount,
                successCount.get(), requestCount);
            System.out.println();
            System.out.println("═══════════════════════════════════════════════════════════");

        } finally {
            shutdown();
        }
    }

    private void startCluster() throws IOException {
        System.out.print("Starting cluster... ");

        List<String> allNodes = new ArrayList<>();
        for (int i = 0; i < nodeCount; i++) {
            allNodes.add("localhost:" + (BASE_PORT + i));
        }

        for (int i = 0; i < nodeCount; i++) {
            String nodeId = "localhost:" + (BASE_PORT + i);
            List<String> peers = new ArrayList<>(allNodes);
            peers.remove(nodeId);

            File dataDir = Files.createTempDirectory("raft-bench-" + i + "-").toFile();
            dataDirs.add(dataDir);

            RaftNode node = new RaftNode(
                nodeId,
                peers,
                new TcpTransport(nodeId),
                new FileLogStorage(dataDir),
                new FileMetaStorage(dataDir),
                new FileSnapshotStorage(dataDir),
                new NoOpStateMachine()
            );
            node.start();
            nodes.add(node);
        }

        System.out.println("done (" + nodeCount + " nodes)");
    }

    private void waitForLeader() throws InterruptedException {
        System.out.print("Waiting for leader election... ");
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 10000) {
            for (RaftNode node : nodes) {
                if (node.getState() == RaftNode.State.LEADER) {
                    System.out.println("done (term " + node.getCurrentTerm() + ")");
                    return;
                }
            }
            Thread.sleep(50);
        }
        throw new RuntimeException("No leader elected within 10 seconds");
    }

    private RaftNode findLeader() {
        for (RaftNode node : nodes) {
            if (node.getState() == RaftNode.State.LEADER) {
                return node;
            }
        }
        throw new RuntimeException("No leader found");
    }

    private void shutdown() {
        System.out.print("Shutting down... ");
        for (RaftNode node : nodes) {
            try {
                node.stop();
            } catch (Exception e) {
                // Ignore
            }
        }

        for (File dataDir : dataDirs) {
            try {
                deleteDirectory(dataDir.toPath());
            } catch (Exception e) {
                // Ignore
            }
        }
        System.out.println("done");
    }

    private void deleteDirectory(Path path) throws IOException {
        if (Files.exists(path)) {
            try (Stream<Path> walk = Files.walk(path)) {
                walk.sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (IOException e) {
                            // Ignore
                        }
                    });
            }
        }
    }

    private static class NoOpStateMachine implements StateMachine {
        @Override
        public void apply(LogEntry entry) {
            // No-op for benchmark
        }

        @Override
        public byte[] takeSnapshot() {
            return new byte[0];
        }

        @Override
        public void installSnapshot(byte[] data) {
            // No-op
        }
    }
}
