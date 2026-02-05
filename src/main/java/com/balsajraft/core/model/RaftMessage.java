package com.balsajraft.core.model;

/**
 * Base type for wire messages
 * Sealed keeps message handling exhaustive, which saved me from dumb switch bugs more than once
 */
public sealed interface RaftMessage
    permits RequestVote, RequestVoteResponse, AppendEntries, AppendEntriesResponse, InstallSnapshot {

    /** Message term */
    long term();
}
