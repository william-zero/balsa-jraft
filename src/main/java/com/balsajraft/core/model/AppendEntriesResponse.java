package com.balsajraft.core.model;

/** Follower -> leader response for AppendEntries */
public record AppendEntriesResponse(
    long term,
    boolean success,
    long matchIndex
) implements RaftMessage {

    public AppendEntriesResponse {
        if (term < 0) throw new IllegalArgumentException("term must be non-negative");
        if (matchIndex < 0) throw new IllegalArgumentException("matchIndex must be non-negative");
    }

    public long getTerm() { return term; }
    public boolean isSuccess() { return success; }
    public long getMatchIndex() { return matchIndex; }
}
