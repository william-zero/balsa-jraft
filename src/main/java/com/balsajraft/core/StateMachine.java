package com.balsajraft.core;

import com.balsajraft.core.model.LogEntry;

/**
 * Your application's state machine. Implement this.
 * Keep methods fast since they run on the raft thread.
 */
public interface StateMachine {

    /** Apply a committed entry. Called in order, once per entry. */
    void apply(LogEntry entry);

    /** Serialize state for snapshot. */
    byte[] takeSnapshot();

    /** Restore from snapshot (when too far behind leader). */
    void installSnapshot(byte[] snapshot);
}
