package com.balsajraft.core.model;

import java.util.Objects;

/** Candidate -> peers vote request */
public record RequestVote(
    long term,
    String candidateId,
    long lastLogIndex,
    long lastLogTerm
) implements RaftMessage {

    public RequestVote {
        Objects.requireNonNull(candidateId, "candidateId must not be null");
        if (term < 0) throw new IllegalArgumentException("term must be non-negative");
        if (lastLogIndex < 0) throw new IllegalArgumentException("lastLogIndex must be non-negative");
        if (lastLogTerm < 0) throw new IllegalArgumentException("lastLogTerm must be non-negative");
    }

    public long getTerm() { return term; }
    public String getCandidateId() { return candidateId; }
    public long getLastLogIndex() { return lastLogIndex; }
    public long getLastLogTerm() { return lastLogTerm; }
}
