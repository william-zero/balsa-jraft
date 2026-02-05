package com.balsajraft.core.model;

import java.util.Arrays;
import java.util.Objects;

/** Single Raft log entry */
public record LogEntry(
    long term,
    long index,
    byte[] command,
    EntryType type
) {
    public enum EntryType {
        /** Normal client command */
        COMMAND,
        /** No-op entry (used for leader commitment) */
        NO_OP,
        /** Configuration change (add/remove server) */
        CONFIGURATION
    }

    public LogEntry {
        Objects.requireNonNull(type, "type must not be null");
        if (term < 0) throw new IllegalArgumentException("term must be non-negative");
        if (index < 0) throw new IllegalArgumentException("index must be non-negative");
        // Defensive copy for immutability
        command = command == null ? null : command.clone();
    }

    /** Returns a defensive copy */
    @Override
    public byte[] command() {
        return command == null ? null : command.clone();
    }

    public long getTerm() { return term; }
    public long getIndex() { return index; }
    public byte[] getCommand() { return command(); }
    public EntryType getType() { return type; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LogEntry that)) return false;
        return term == that.term &&
               index == that.index &&
               type == that.type &&
               Arrays.equals(command, that.command);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(term, index, type);
        result = 31 * result + Arrays.hashCode(command);
        return result;
    }

    @Override
    public String toString() {
        return "LogEntry[term=" + term +
               ", index=" + index +
               ", type=" + type +
               ", commandSize=" + (command == null ? 0 : command.length) + "]";
    }
}
