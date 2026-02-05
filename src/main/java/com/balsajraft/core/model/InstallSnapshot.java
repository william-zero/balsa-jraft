package com.balsajraft.core.model;

import java.util.Arrays;
import java.util.Objects;

/** Leader -> follower snapshot transfer for lagging peers */
public record InstallSnapshot(
    long term,
    String leaderId,
    long lastIncludedIndex,
    long lastIncludedTerm,
    byte[] data
) implements RaftMessage {

    public InstallSnapshot {
        Objects.requireNonNull(leaderId, "leaderId must not be null");
        if (term < 0) throw new IllegalArgumentException("term must be non-negative");
        if (lastIncludedIndex < 0) throw new IllegalArgumentException("lastIncludedIndex must be non-negative");
        if (lastIncludedTerm < 0) throw new IllegalArgumentException("lastIncludedTerm must be non-negative");
        // Defensive copy for immutability
        data = data == null ? new byte[0] : data.clone();
    }

    /** Returns a defensive copy */
    @Override
    public byte[] data() {
        return data.clone();
    }

    public long getTerm() { return term; }
    public String getLeaderId() { return leaderId; }
    public long getLastIncludedIndex() { return lastIncludedIndex; }
    public long getLastIncludedTerm() { return lastIncludedTerm; }
    public byte[] getData() { return data(); }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InstallSnapshot that)) return false;
        return term == that.term &&
               lastIncludedIndex == that.lastIncludedIndex &&
               lastIncludedTerm == that.lastIncludedTerm &&
               Objects.equals(leaderId, that.leaderId) &&
               Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(term, leaderId, lastIncludedIndex, lastIncludedTerm);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    @Override
    public String toString() {
        return "InstallSnapshot[term=" + term +
               ", leaderId=" + leaderId +
               ", lastIncludedIndex=" + lastIncludedIndex +
               ", lastIncludedTerm=" + lastIncludedTerm +
               ", dataSize=" + data.length + "]";
    }
}
