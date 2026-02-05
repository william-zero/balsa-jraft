package com.balsajraft.core.storage;

import java.io.IOException;

/**
 * Interface for persisting Raft node metadata (term and votedFor).
 * Must be durable - data must survive crashes.
 */
public interface MetaStorage {

    /**
     * Persist the current term and votedFor.
     * Must be atomic and durable (fsync).
     *
     * @param term     current term
     * @param votedFor candidateId voted for in this term, or null
     * @throws IOException if persistence fails
     */
    void save(long term, String votedFor) throws IOException;

    /**
     * Load persisted metadata.
     *
     * @return the persisted state, or default state if none exists
     * @throws IOException if loading fails
     */
    MetaState load() throws IOException;

    /**
     * Immutable container for persisted Raft metadata.
     *
     * @param term     the current term (must be non-negative)
     * @param votedFor the candidateId voted for in this term, or null
     */
    record MetaState(long term, String votedFor) {
        public MetaState {
            if (term < 0) throw new IllegalArgumentException("term must be non-negative");
        }

        /** Default initial state */
        public static MetaState initial() {
            return new MetaState(0, null);
        }
    }
}
