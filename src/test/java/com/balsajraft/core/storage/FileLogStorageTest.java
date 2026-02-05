package com.balsajraft.core.storage;

import com.balsajraft.core.model.LogEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class FileLogStorageTest {

    @TempDir
    Path tempDir;

    private FileLogStorage storage;

    @BeforeEach
    void setUp() throws IOException {
        storage = new FileLogStorage(tempDir.toFile());
    }

    @AfterEach
    void tearDown() throws IOException {
        if (storage != null) {
            storage.close();
        }
    }

    @Test
    void testAppendAndRead() throws IOException {
        LogEntry entry1 = new LogEntry(1, 1, "cmd1".getBytes(), LogEntry.EntryType.COMMAND);
        LogEntry entry2 = new LogEntry(1, 2, "cmd2".getBytes(), LogEntry.EntryType.COMMAND);

        storage.append(entry1);
        storage.append(entry2);

        assertEquals(2, storage.getLastIndex());
        assertEquals(1, storage.getLastTerm());

        LogEntry read1 = storage.getEntry(1);
        assertEquals(entry1.getTerm(), read1.getTerm());
        assertEquals(entry1.getIndex(), read1.getIndex());
        assertArrayEquals(entry1.getCommand(), read1.getCommand());

        LogEntry read2 = storage.getEntry(2);
        assertEquals(entry2.getIndex(), read2.getIndex());
    }

    @Test
    void testRecovery() throws IOException {
        LogEntry entry1 = new LogEntry(1, 1, "cmd1".getBytes(), LogEntry.EntryType.COMMAND);
        storage.append(entry1);
        storage.close();

        // Re-open
        storage = new FileLogStorage(tempDir.toFile());
        assertEquals(1, storage.getLastIndex());
        LogEntry read1 = storage.getEntry(1);
        assertNotNull(read1);
        assertEquals(1, read1.getIndex());
    }

    @Test
    void testTruncate() throws IOException {
        storage.append(new LogEntry(1, 1, "a".getBytes(), LogEntry.EntryType.COMMAND));
        storage.append(new LogEntry(1, 2, "b".getBytes(), LogEntry.EntryType.COMMAND));
        storage.append(new LogEntry(2, 3, "c".getBytes(), LogEntry.EntryType.COMMAND));

        // Truncate from index 2 (remove 2 and 3)
        storage.truncate(2);

        assertEquals(1, storage.getLastIndex());
        assertEquals(1, storage.getLastTerm());
        assertNull(storage.getEntry(2));
        assertNotNull(storage.getEntry(1));

        // Append new entry at 2
        LogEntry newEntry2 = new LogEntry(3, 2, "d".getBytes(), LogEntry.EntryType.COMMAND);
        storage.append(newEntry2);

        assertEquals(2, storage.getLastIndex());
        assertEquals(3, storage.getLastTerm());
        assertEquals(3, storage.getEntry(2).getTerm());
    }

    @Test
    void testCompact() throws IOException {
        storage.append(new LogEntry(1, 1, "a".getBytes(), LogEntry.EntryType.COMMAND));
        storage.append(new LogEntry(1, 2, "b".getBytes(), LogEntry.EntryType.COMMAND));
        storage.append(new LogEntry(1, 3, "c".getBytes(), LogEntry.EntryType.COMMAND));

        // Compact up to index 1 (keep 1 as snapshot metadata)
        storage.compact(1);

        assertEquals(1, storage.getFirstIndex());
        assertEquals(3, storage.getLastIndex());
        assertNotNull(storage.getEntry(1));
        assertNotNull(storage.getEntry(2));
        assertNotNull(storage.getEntry(3));

        // Compact up to index 2 (keep 2)
        storage.compact(2);
        assertEquals(2, storage.getFirstIndex());
        assertNull(storage.getEntry(1));
        assertNotNull(storage.getEntry(2));
        assertNotNull(storage.getEntry(3));
    }

    @Test
    void testRecoveryWithCompaction() throws IOException {
        storage.append(new LogEntry(1, 1, "a".getBytes(), LogEntry.EntryType.COMMAND));
        storage.append(new LogEntry(1, 2, "b".getBytes(), LogEntry.EntryType.COMMAND));
        storage.compact(1);
        storage.close();

        // Reopen
        storage = new FileLogStorage(tempDir.toFile());
        assertEquals(1, storage.getFirstIndex());
        assertEquals(2, storage.getLastIndex());
        assertNotNull(storage.getEntry(1));
        assertNotNull(storage.getEntry(2));
    }
}
