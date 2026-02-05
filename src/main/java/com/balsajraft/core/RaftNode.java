package com.balsajraft.core;

import com.balsajraft.core.model.*;
import com.balsajraft.core.storage.LogStorage;
import com.balsajraft.core.storage.MetaStorage;
import com.balsajraft.core.storage.SnapshotStorage;
import com.balsajraft.transport.Transport;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

// The main raft node. Does elections and log replication.
// Single threaded to keep it simple
public class RaftNode implements Transport.TransportHandler {
    private static final Logger logger = Logger.getLogger(RaftNode.class.getName());

    // TODO: make these configurable?
    private static final int ELECTION_TIMEOUT_MIN_MS = 150;
    private static final int ELECTION_TIMEOUT_MAX_MS = 300;
    private static final int HEARTBEAT_INTERVAL_MS = 50;
    private static final int TICK_INTERVAL_MS = 10;
    private static final int MAX_BATCH_SIZE = 1000;  // arbitrary
    private static final int MAX_PENDING_REQUESTS = 10000;
    private static final long REQUEST_TIMEOUT_MS = 30000;

    public enum State {
        FOLLOWER, CANDIDATE, LEADER
    }

    private final String nodeId;
    private final List<String> peers;
    private final Transport transport;
    private final LogStorage logStorage;
    private final MetaStorage metaStorage;
    private final SnapshotStorage snapshotStorage;
    private final StateMachine stateMachine;

    private long currentTerm;
    private String votedFor;

    private long commitIndex = 0;
    private long lastApplied = 0;
    private State state = State.FOLLOWER;
    private String leaderId;

    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();

    private long lastHeartbeatTime;
    private volatile long electionTimeoutMs;
    private final Random random = new Random();

    private final Set<String> receivedVotes = new HashSet<>();

    private long lastSnapshotIndex = 0;
    private long lastSnapshotTerm = 0;
    private volatile byte[] cachedSnapshot = null;

    private final ExecutorService executor;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "Raft-Scheduler");
        t.setDaemon(true);
        return t;
    });
    private volatile boolean running = false;

    private final ConcurrentHashMap<Long, PendingRequest> pendingRequests = new ConcurrentHashMap<>();

    private static class PendingRequest {
        final CompletableFuture<Boolean> future;
        final long createdAt;

        PendingRequest(CompletableFuture<Boolean> future) {
            this.future = future;
            this.createdAt = System.currentTimeMillis();
        }

        boolean isExpired() {
            return System.currentTimeMillis() - createdAt > REQUEST_TIMEOUT_MS;
        }
    }

    public RaftNode(String nodeId, List<String> initialPeers, Transport transport,
                    LogStorage logStorage, MetaStorage metaStorage, SnapshotStorage snapshotStorage, StateMachine stateMachine) throws IOException {
        this.nodeId = nodeId;
        this.peers = new CopyOnWriteArrayList<>(initialPeers);
        this.transport = transport;
        this.logStorage = logStorage;
        this.metaStorage = metaStorage;
        this.snapshotStorage = snapshotStorage;
        this.stateMachine = stateMachine;

        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "Raft-Core-" + nodeId);
            t.setUncaughtExceptionHandler((thread, e) ->
                logger.log(Level.SEVERE, "Uncaught exception in Raft core", e));
            return t;
        });

        MetaStorage.MetaState meta = metaStorage.load();
        this.currentTerm = meta.term();
        this.votedFor = meta.votedFor();

        byte[] snapshotData = snapshotStorage.load();
        if (snapshotData != null) {
            long lastIdx = snapshotStorage.getLastSnapshotIndex();
            long lastTerm = snapshotStorage.getLastSnapshotTerm();

            if (lastIdx > 0) {
                this.lastSnapshotIndex = lastIdx;
                this.lastSnapshotTerm = lastTerm;
                this.cachedSnapshot = snapshotData;

                stateMachine.installSnapshot(snapshotData);

                this.lastApplied = lastIdx;
                this.commitIndex = Math.max(commitIndex, lastIdx);
            }
        }

        long firstIndex = logStorage.getFirstIndex();
        if (firstIndex > 1) {
            if (firstIndex > lastSnapshotIndex) {
                 logger.warning("Log starts at " + firstIndex + " but snapshot is at " + lastSnapshotIndex + ". State may be incomplete.");
                 LogEntry first = logStorage.getEntry(firstIndex);
                 if (first != null) {
                     this.lastSnapshotIndex = firstIndex;
                     this.lastSnapshotTerm = first.getTerm();
                     this.lastApplied = Math.max(lastApplied, firstIndex);
                     this.commitIndex = Math.max(commitIndex, firstIndex);
                 }
            }
        }

        resetElectionTimeout();
    }

    /**
     * Triggers a manual snapshot up to the given index.
     * This will:
     * 1. Take a snapshot from the state machine
     * 2. Compact the log storage up to index
     * 3. Update snapshot metadata
     *
     * @param index The index to include in the snapshot (inclusive)
     * @return CompletableFuture that completes when snapshot is done
     */
    public CompletableFuture<Boolean> snapshot(long index) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        executor.submit(() -> {
            try {
                if (index <= lastSnapshotIndex) {
                    future.complete(true);
                    return;
                }
                if (index > lastApplied) {
                    future.completeExceptionally(new IllegalArgumentException(
                        "Cannot snapshot unapplied index: " + index + " > " + lastApplied));
                    return;
                }

                byte[] data = stateMachine.takeSnapshot();

                LogEntry entry = logStorage.getEntry(index);
                if (entry == null) {
                    future.completeExceptionally(new IllegalStateException(
                        "Log entry not found for snapshot index " + index));
                    return;
                }
                long term = entry.getTerm();

                snapshotStorage.save(index, term, data);

                logStorage.compact(index);

                lastSnapshotIndex = index;
                lastSnapshotTerm = term;
                cachedSnapshot = data;

                logger.info(nodeId + " compacted log up to index " + index);
                future.complete(true);

            } catch (Exception e) {
                logger.log(Level.SEVERE, "Snapshot failed", e);
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    public void start() throws IOException {
        running = true;
        transport.registerHandler(this);
        transport.start(parsePort(nodeId));

        resetHeartbeatTimer();

        scheduler.scheduleAtFixedRate(() -> {
            if (running) {
                executor.submit(this::safeTick);
            }
        }, TICK_INTERVAL_MS, TICK_INTERVAL_MS, TimeUnit.MILLISECONDS);

        scheduler.scheduleAtFixedRate(() -> {
            if (running) {
                executor.submit(this::cleanupExpiredRequests);
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);

        logger.info("RaftNode " + nodeId + " started. Term: " + currentTerm);
    }

    public void stop() throws IOException {
        running = false;

        for (Map.Entry<Long, PendingRequest> entry : pendingRequests.entrySet()) {
            entry.getValue().future.complete(false);
        }
        pendingRequests.clear();

        transport.stop();
        scheduler.shutdownNow();
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                logger.warning("Executor did not terminate in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        logStorage.close();
    }

    public CompletableFuture<Boolean> replicate(byte[] command) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();

        if (pendingRequests.size() >= MAX_PENDING_REQUESTS) {
            future.completeExceptionally(new RejectedExecutionException("Too many pending requests"));
            return future;
        }

        executor.submit(() -> {
            if (state != State.LEADER) {
                future.complete(false);
                return;
            }
            try {
                long index = logStorage.getLastIndex() + 1;
                LogEntry entry = new LogEntry(currentTerm, index, command, LogEntry.EntryType.COMMAND);
                logStorage.append(entry);
                pendingRequests.put(index, new PendingRequest(future));
            } catch (IOException e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    public CompletableFuture<Boolean> addServer(String newServer) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();

        if (pendingRequests.size() >= MAX_PENDING_REQUESTS) {
            future.completeExceptionally(new RejectedExecutionException("Too many pending requests"));
            return future;
        }

        executor.submit(() -> {
            if (state != State.LEADER) {
                future.complete(false);
                return;
            }
            if (peers.contains(newServer)) {
                future.complete(true);
                return;
            }
            try {
                long index = logStorage.getLastIndex() + 1;
                byte[] command = ("ADD:" + newServer).getBytes();
                LogEntry entry = new LogEntry(currentTerm, index, command, LogEntry.EntryType.CONFIGURATION);
                logStorage.append(entry);
                pendingRequests.put(index, new PendingRequest(future));
            } catch (IOException e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    public CompletableFuture<Boolean> removeServer(String oldServer) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();

        if (pendingRequests.size() >= MAX_PENDING_REQUESTS) {
            future.completeExceptionally(new RejectedExecutionException("Too many pending requests"));
            return future;
        }

        executor.submit(() -> {
            if (state != State.LEADER) {
                future.complete(false);
                return;
            }
            if (!peers.contains(oldServer)) {
                future.complete(true);
                return;
            }
            try {
                long index = logStorage.getLastIndex() + 1;
                byte[] command = ("REM:" + oldServer).getBytes();
                LogEntry entry = new LogEntry(currentTerm, index, command, LogEntry.EntryType.CONFIGURATION);
                logStorage.append(entry);
                pendingRequests.put(index, new PendingRequest(future));
            } catch (IOException e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    @Override
    public void handleMessage(Object message, String senderId) {
        executor.submit(() -> {
            try {
                switch (message) {
                    case RequestVote rv -> handleRequestVote(rv);
                    case RequestVoteResponse rvr -> handleRequestVoteResponse(rvr, senderId);
                    case AppendEntries ae -> handleAppendEntries(ae);
                    case AppendEntriesResponse aer -> handleAppendEntriesResponse(aer, senderId);
                    case InstallSnapshot is -> handleInstallSnapshot(is);
                    case null, default -> logger.warning("Unknown message type: " +
                        (message == null ? "null" : message.getClass().getName()));
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Error handling message from " + senderId, e);
            }
        });
    }

    private void safeTick() {
        try {
            tick();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in tick", e);
        }
    }

    private void tick() {
        if (!running) return;

        long now = System.currentTimeMillis();

        if (state != State.LEADER) {
            if (now - lastHeartbeatTime > electionTimeoutMs) {
                startElection();
            }
        } else {
            if (now - lastHeartbeatTime > HEARTBEAT_INTERVAL_MS) {
                sendHeartbeats();
                lastHeartbeatTime = now;
            }
        }

        applyCommitted();
    }

    private void cleanupExpiredRequests() {
        try {
            Iterator<Map.Entry<Long, PendingRequest>> it = pendingRequests.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Long, PendingRequest> entry = it.next();
                if (entry.getValue().isExpired()) {
                    entry.getValue().future.complete(false);
                    it.remove();
                }
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error cleaning up expired requests", e);
        }
    }

    private void startElection() {
        // System.out.println(">>> " + nodeId + " starting election");
        state = State.CANDIDATE;
        currentTerm++;
        votedFor = nodeId;
        persist();

        resetElectionTimeout();
        resetHeartbeatTimer();

        receivedVotes.clear();
        receivedVotes.add(nodeId);

        logger.info(nodeId + " starting election for term " + currentTerm);

        long lastLogIndex = logStorage.getLastIndex();
        long lastLogTerm = logStorage.getLastTerm();

        RequestVote rv = new RequestVote(currentTerm, nodeId, lastLogIndex, lastLogTerm);

        for (String peer : peers) {
            transport.send(rv, peer);
        }
    }

    private void sendHeartbeats() {
        for (String peer : peers) {
            sendAppendEntries(peer);
        }
    }

    private void sendAppendEntries(String peer) {
        long next = nextIndex.getOrDefault(peer, logStorage.getLastIndex() + 1);

        if (next < logStorage.getFirstIndex()) {
            sendInstallSnapshot(peer);
            return;
        }

        long prevLogIndex = next - 1;
        long prevLogTerm = 0;

        try {
            if (prevLogIndex > 0) {
                LogEntry prev = logStorage.getEntry(prevLogIndex);
                if (prev != null) {
                    prevLogTerm = prev.getTerm();
                } else if (prevLogIndex < logStorage.getFirstIndex() && prevLogIndex > 0) {
                    sendInstallSnapshot(peer);
                    return;
                }
            }

            List<LogEntry> entries = new ArrayList<>();
            long lastIndex = logStorage.getLastIndex();
            if (lastIndex >= next) {
                long end = Math.min(lastIndex, next + MAX_BATCH_SIZE - 1);
                for (long i = next; i <= end; i++) {
                    LogEntry entry = logStorage.getEntry(i);
                    if (entry != null) {
                        entries.add(entry);
                    }
                }
            }

            AppendEntries ae = new AppendEntries(currentTerm, nodeId, prevLogIndex, prevLogTerm, entries, commitIndex);
            transport.send(ae, peer);

        } catch (IOException e) {
            logger.log(Level.WARNING, "Failed to read log for heartbeat to " + peer, e);
        }
    }

    private void sendInstallSnapshot(String peer) {
        // FIXME: should probably chunk large snapshots instead of sending all at once
        byte[] data;
        long lastIdx;
        long term;

        synchronized (this) {
            if (cachedSnapshot == null || lastApplied > lastSnapshotIndex) {
                cachedSnapshot = stateMachine.takeSnapshot();
                lastSnapshotIndex = lastApplied;
                try {
                    LogEntry entry = logStorage.getEntry(lastApplied);
                    if (entry != null) {
                        lastSnapshotTerm = entry.getTerm();
                    }
                } catch (IOException e) {
                    logger.log(Level.WARNING, "Failed to get term for snapshot", e);
                }
            }
            data = cachedSnapshot;
            lastIdx = lastSnapshotIndex;
            term = lastSnapshotTerm;
        }

        if (data == null) {
            logger.warning("Cannot send snapshot: no snapshot available");
            return;
        }

        InstallSnapshot is = new InstallSnapshot(currentTerm, nodeId, lastIdx, term, data);
        transport.send(is, peer);
    }

    private void handleRequestVote(RequestVote rv) {
        if (rv.getTerm() > currentTerm) {
            stepDown(rv.getTerm());
        }

        // this logic is straight from the paper (figure 2)
        boolean voteGranted = false;
        if (rv.getTerm() == currentTerm) {
            boolean logIsUpToDate = false;
            long myLastLogIndex = logStorage.getLastIndex();
            long myLastLogTerm = logStorage.getLastTerm();

            if (rv.getLastLogTerm() > myLastLogTerm) {
                logIsUpToDate = true;
            } else if (rv.getLastLogTerm() == myLastLogTerm && rv.getLastLogIndex() >= myLastLogIndex) {
                logIsUpToDate = true;
            }

            if ((votedFor == null || votedFor.equals(rv.getCandidateId())) && logIsUpToDate) {
                voteGranted = true;
                votedFor = rv.getCandidateId();
                persist();
                resetHeartbeatTimer();
            }
        }

        transport.send(new RequestVoteResponse(currentTerm, voteGranted), rv.getCandidateId());
    }

    private void resetLogAndAppendSnapshotEntry(InstallSnapshot is) throws IOException {
        logStorage.reset(is.getLastIncludedIndex());
        logStorage.append(new LogEntry(
            is.getLastIncludedTerm(),
            is.getLastIncludedIndex(),
            new byte[0],
            LogEntry.EntryType.NO_OP
        ));

        snapshotStorage.save(is.getLastIncludedIndex(), is.getLastIncludedTerm(), is.getData());
    }

    private void handleInstallSnapshot(InstallSnapshot is) {
        if (is.getTerm() > currentTerm) {
            stepDown(is.getTerm());
        }
        if (is.getTerm() < currentTerm) {
            return;
        }

        resetHeartbeatTimer();

        if (is.getLastIncludedIndex() <= commitIndex) {
            return;
        }

        try {
            stateMachine.installSnapshot(is.getData());

            if (is.getLastIncludedIndex() >= logStorage.getFirstIndex() &&
                is.getLastIncludedIndex() <= logStorage.getLastIndex()) {

                LogEntry entry = logStorage.getEntry(is.getLastIncludedIndex());
                if (entry != null && entry.getTerm() == is.getLastIncludedTerm()) {
                    logStorage.compact(is.getLastIncludedIndex());
                } else {
                    resetLogAndAppendSnapshotEntry(is);
                }
            } else {
                resetLogAndAppendSnapshotEntry(is);
            }

            lastSnapshotIndex = is.getLastIncludedIndex();
            lastSnapshotTerm = is.getLastIncludedTerm();
            lastApplied = is.getLastIncludedIndex();
            commitIndex = Math.max(commitIndex, is.getLastIncludedIndex());

        } catch (IOException e) {
            logger.log(Level.SEVERE, "Snapshot installation failed", e);
        }
    }

    private void handleRequestVoteResponse(RequestVoteResponse rvr, String senderId) {
        if (rvr.getTerm() > currentTerm) {
            stepDown(rvr.getTerm());
            return;
        }

        if (state == State.CANDIDATE && rvr.getTerm() == currentTerm && rvr.isVoteGranted()) {
            receivedVotes.add(senderId);
            int quorum = (peers.size() + 1) / 2 + 1;
            if (receivedVotes.size() >= quorum) {
                becomeLeader();
            }
        }
    }

    private void becomeLeader() {
        state = State.LEADER;
        leaderId = nodeId;
        // finally!
        logger.info(nodeId + " became leader for term " + currentTerm);

        for (String peer : peers) {
            nextIndex.put(peer, logStorage.getLastIndex() + 1);
            matchIndex.put(peer, 0L);
        }

        cachedSnapshot = null;

        sendHeartbeats();
    }

    private void handleAppendEntries(AppendEntries ae) {
        if (ae.getTerm() > currentTerm) {
            stepDown(ae.getTerm());
        }

        if (ae.getTerm() < currentTerm) {
            transport.send(new AppendEntriesResponse(currentTerm, false, 0), ae.getLeaderId());
            return;
        }

        state = State.FOLLOWER;
        leaderId = ae.getLeaderId();
        resetHeartbeatTimer();

        try {
            if (ae.getPrevLogIndex() > 0) {
                LogEntry prev = logStorage.getEntry(ae.getPrevLogIndex());
                if (prev == null || prev.getTerm() != ae.getPrevLogTerm()) {
                    transport.send(new AppendEntriesResponse(currentTerm, false, 0), leaderId);
                    return;
                }
            }

            long index = ae.getPrevLogIndex();
            for (LogEntry entry : ae.getEntries()) {
                index++;
                LogEntry existing = logStorage.getEntry(index);
                if (existing != null) {
                    if (existing.getTerm() != entry.getTerm()) {
                        logStorage.truncate(index);
                        logStorage.append(entry);
                    }
                } else {
                    logStorage.append(entry);
                }
            }

            if (ae.getLeaderCommit() > commitIndex) {
                commitIndex = Math.min(ae.getLeaderCommit(), logStorage.getLastIndex());
            }

            transport.send(new AppendEntriesResponse(currentTerm, true, logStorage.getLastIndex()), leaderId);

        } catch (IOException e) {
            logger.log(Level.SEVERE, "Disk error handling AppendEntries", e);
        }
    }

    private void handleAppendEntriesResponse(AppendEntriesResponse aer, String senderId) {
        if (aer.getTerm() > currentTerm) {
            stepDown(aer.getTerm());
            return;
        }

        if (state == State.LEADER && aer.getTerm() == currentTerm) {
            if (aer.isSuccess()) {
                matchIndex.put(senderId, aer.getMatchIndex());
                nextIndex.put(senderId, aer.getMatchIndex() + 1);
                updateCommitIndex();
            } else {
                long next = nextIndex.getOrDefault(senderId, 2L);
                if (next > 1) {
                    nextIndex.put(senderId, next - 1);
                }
            }
        }
    }

    private void updateCommitIndex() {
        // this feels like there's a cleaner way to do this
        List<Long> indices = new ArrayList<>(matchIndex.values());
        indices.add(logStorage.getLastIndex());
        indices.sort(Collections.reverseOrder());

        int quorum = (peers.size() + 1) / 2 + 1;
        int majorityIndex = quorum - 1;

        if (indices.size() >= quorum) {
            long N = indices.get(majorityIndex);
            if (N > commitIndex) {
                try {
                    LogEntry entry = logStorage.getEntry(N);
                    if (entry != null && entry.getTerm() == currentTerm) {
                        commitIndex = N;
                    }
                } catch (IOException e) {
                    logger.log(Level.WARNING, "Failed to read log entry for commit check", e);
                }
            }
        }
    }

    private void stepDown(long term) {
        boolean wasLeader = (state == State.LEADER);
        currentTerm = term;
        state = State.FOLLOWER;
        votedFor = null;
        leaderId = null;
        persist();
        resetElectionTimeout();
        resetHeartbeatTimer();

        if (wasLeader) {
            failAllPendingRequests();
        }
    }

    private void failAllPendingRequests() {
        for (Map.Entry<Long, PendingRequest> entry : pendingRequests.entrySet()) {
            entry.getValue().future.complete(false);
        }
        pendingRequests.clear();
    }

    private void persist() {
        try {
            metaStorage.save(currentTerm, votedFor);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to persist meta state", e);
        }
    }

    private void resetElectionTimeout() {
        this.electionTimeoutMs = ELECTION_TIMEOUT_MIN_MS +
            random.nextInt(ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS);
    }

    private void resetHeartbeatTimer() {
        lastHeartbeatTime = System.currentTimeMillis();
    }

    private void applyCommitted() {
        // maybe batch this later
        while (commitIndex > lastApplied) {
            lastApplied++;
            try {
                LogEntry entry = logStorage.getEntry(lastApplied);
                if (entry == null) {
                    logger.warning("Missing log entry at index " + lastApplied);
                    continue;
                }

                if (entry.getType() == LogEntry.EntryType.COMMAND) {
                    stateMachine.apply(entry);
                } else if (entry.getType() == LogEntry.EntryType.CONFIGURATION) {
                    applyConfiguration(entry);
                }

                PendingRequest pending = pendingRequests.remove(lastApplied);
                if (pending != null) {
                    pending.future.complete(true);
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Failed to apply committed entry at index " + lastApplied, e);
            }
        }
    }

    private void applyConfiguration(LogEntry entry) {
        String config = new String(entry.getCommand());
        String[] parts = config.split(":", 2);
        if (parts.length != 2) {
            logger.warning("Invalid configuration entry: " + config);
            return;
        }

        String op = parts[0];
        String peer = parts[1];

        if ("ADD".equals(op)) {
            if (!peers.contains(peer) && !peer.equals(nodeId)) {
                peers.add(peer);
                if (state == State.LEADER) {
                    nextIndex.put(peer, logStorage.getLastIndex() + 1);
                    matchIndex.put(peer, 0L);
                }
            }
        } else if ("REM".equals(op)) {
            peers.remove(peer);
            if (state == State.LEADER) {
                nextIndex.remove(peer);
                matchIndex.remove(peer);
            }
            if (peer.equals(nodeId)) {
                logger.info("Node removed from cluster. Stopping.");
                try {
                    stop();
                } catch (IOException e) {
                    logger.log(Level.SEVERE, "Error stopping removed node", e);
                }
            }
        }
    }

    private int parsePort(String addr) {
        String[] parts = addr.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid address format: " + addr + ". Expected host:port");
        }
        return Integer.parseInt(parts[1]);
    }

    public State getState() {
        return state;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public int getPendingRequestCount() {
        return pendingRequests.size();
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public long getLastApplied() {
        return lastApplied;
    }
}
