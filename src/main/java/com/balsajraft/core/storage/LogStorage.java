package com.balsajraft.core.storage;

import com.balsajraft.core.model.LogEntry;
import java.io.IOException;

/**
 * Persistent storage for the Raft log.
 * Implementations must be durable - entries must survive crashes after append returns.
 * The default FileLogStorage does fsync on every write.
 */
public interface LogStorage {

    /** Append entry to log. Must be durable when this returns. */
    void append(LogEntry entry) throws IOException;

    /** Get entry at index, or null if not found */
    LogEntry getEntry(long index) throws IOException;

    long getLastIndex();
    long getLastTerm();

    /** Drop entries from index onward (inclusive) */
    void truncate(long index) throws IOException;

    /** Compact entries up to index (inclusive) after snapshotting */
    void compact(long index) throws IOException;

    /** First available index (may be > 1 after compaction) */
    long getFirstIndex();

    /** Reset log, next append starts at given index */
    void reset(long index) throws IOException;

    void close() throws IOException;
}
