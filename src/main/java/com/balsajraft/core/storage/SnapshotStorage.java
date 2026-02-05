package com.balsajraft.core.storage;

import java.io.IOException;

public interface SnapshotStorage {
    /**
     * Saves a snapshot.
     * @param index the last included index
     * @param term the last included term
     * @param data the snapshot data
     */
    void save(long index, long term, byte[] data) throws IOException;

    /**
     * Loads the latest snapshot.
     * @return the snapshot data, or null if none exists
     */
    byte[] load() throws IOException;

    /**
     * Returns the index of the latest snapshot.
     * @return index, or 0 if none
     */
    long getLastSnapshotIndex() throws IOException;

    /**
     * Returns the term of the latest snapshot.
     * @return term, or 0 if none
     */
    long getLastSnapshotTerm() throws IOException;
}
