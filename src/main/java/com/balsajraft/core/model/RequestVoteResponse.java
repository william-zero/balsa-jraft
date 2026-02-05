package com.balsajraft.core.model;

/** Peer -> candidate vote response */
public record RequestVoteResponse(
    long term,
    boolean voteGranted
) implements RaftMessage {

    public RequestVoteResponse {
        if (term < 0) throw new IllegalArgumentException("term must be non-negative");
    }

    public long getTerm() { return term; }
    public boolean isVoteGranted() { return voteGranted; }
}
