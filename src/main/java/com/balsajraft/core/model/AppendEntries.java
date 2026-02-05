package com.balsajraft.core.model;

import java.util.List;
import java.util.Objects;

/** Leader -> follower replication message and heartbeat */
public record AppendEntries(
    long term,
    String leaderId,
    long prevLogIndex,
    long prevLogTerm,
    List<LogEntry> entries,
    long leaderCommit
) implements RaftMessage {

    public AppendEntries {
        Objects.requireNonNull(leaderId, "leaderId must not be null");
        Objects.requireNonNull(entries, "entries must not be null");
        if (term < 0) throw new IllegalArgumentException("term must be non-negative");
        if (prevLogIndex < 0) throw new IllegalArgumentException("prevLogIndex must be non-negative");
        if (prevLogTerm < 0) throw new IllegalArgumentException("prevLogTerm must be non-negative");
        if (leaderCommit < 0) throw new IllegalArgumentException("leaderCommit must be non-negative");
        // Make defensive copy for immutability
        entries = List.copyOf(entries);
    }

    public long getTerm() { return term; }
    public String getLeaderId() { return leaderId; }
    public long getPrevLogIndex() { return prevLogIndex; }
    public long getPrevLogTerm() { return prevLogTerm; }
    public List<LogEntry> getEntries() { return entries; }
    public long getLeaderCommit() { return leaderCommit; }
}
