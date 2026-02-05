package com.balsajraft.core.storage;

import com.balsajraft.core.model.LogEntry;
import com.balsajraft.core.serializer.BinarySerializer;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class FileLogStorage implements LogStorage {
    private final File file;
    private RandomAccessFile raf;
    private final List<Long> offsets = new ArrayList<>();
    private final BinarySerializer serializer = new BinarySerializer();

    // Cached head/tail markers so read paths stay cheap
    private volatile long lastIndex = 0;
    private volatile long lastTerm = 0;
    private volatile long firstIndex = 1;

    public FileLogStorage(File baseDir) throws IOException {
        this.file = new File(baseDir, "raft-log.dat");
        this.raf = new RandomAccessFile(file, "rw");
        recover();
    }

    private void recover() throws IOException {
        // TODO: recovery is O(n) on log size, could store offsets separately
        raf.seek(0);
        offsets.clear();
        // Rebuild offsets from disk on startup
        boolean empty = true;

        while (true) {
            long pos = raf.getFilePointer();
            try {
                LogEntry entry = serializer.readLogEntry(raf);

                if (empty) {
                    firstIndex = entry.getIndex();
                    empty = false;
                }

                offsets.add(pos);
                lastIndex = entry.getIndex();
                lastTerm = entry.getTerm();
            } catch (IOException e) {
                raf.setLength(pos);
                raf.seek(pos);
                break;
            }
        }

        if (empty) {
             lastIndex = firstIndex - 1;
             lastTerm = 0; // Unknown if compacted
        }
    }

    @Override
    public long getFirstIndex() {
        return firstIndex;
    }

    @Override
    public synchronized void append(LogEntry entry) throws IOException {
        long pos = raf.getFilePointer();
        if (pos != raf.length()) {
            raf.seek(raf.length());
            pos = raf.getFilePointer();
        }

        serializer.writeLogEntry(raf, entry);
        // Ensure durability
        raf.getFD().sync();

        offsets.add(pos);
        lastIndex = entry.getIndex();
        lastTerm = entry.getTerm();
    }

    @Override
    public synchronized LogEntry getEntry(long index) throws IOException {
        if (index < firstIndex || index > lastIndex) {
            return null;
        }
        long pos = offsets.get((int) (index - firstIndex));
        raf.seek(pos);
        return serializer.readLogEntry(raf);
    }

    @Override
    public synchronized long getLastIndex() {
        return lastIndex;
    }

    @Override
    public synchronized long getLastTerm() {
        return lastTerm;
    }

    @Override
    public synchronized void truncate(long index) throws IOException {
        if (index > lastIndex + 1) {
            // truncate(lastIndex + 1) is a no-op
            return;
        }

        if (index <= firstIndex) {
            // Wipe current segment but keep logical base index
            raf.setLength(0);
            offsets.clear();
            lastIndex = firstIndex - 1;
            lastTerm = 0;
            raf.seek(0);
            return;
        }

        // Keep [firstIndex, index), drop the rest
        int offsetIndex = (int) (index - firstIndex);
        if (offsetIndex >= offsets.size()) return;

        long truncatePos = offsets.get(offsetIndex);

        raf.setLength(truncatePos);
        raf.seek(truncatePos);

        offsets.subList(offsetIndex, offsets.size()).clear();

        lastIndex = index - 1;
        if (lastIndex >= firstIndex) {
            LogEntry prev = getEntry(lastIndex);
            lastTerm = prev.getTerm();
            raf.seek(truncatePos);
        } else {
            lastTerm = 0;
        }
    }

    @Override
    public synchronized void compact(long index) throws IOException {
        if (index < firstIndex || index >= lastIndex) {
            return;
        }

        // Keep the entry at snapshot index so prevLogTerm checks still work
        List<LogEntry> keep = new ArrayList<>();
        for (long i = index; i <= lastIndex; i++) {
            keep.add(getEntry(i));
        }

        raf.close();

        File tmp = new File(file.getParentFile(), "raft-log.tmp");
        try (RandomAccessFile tmpRaf = new RandomAccessFile(tmp, "rw")) {
            for (LogEntry e : keep) {
                serializer.writeLogEntry(tmpRaf, e);
            }
        }

        if (file.exists() && !file.delete()) {
            throw new IOException("Failed to delete old log");
        }
        if (!tmp.renameTo(file)) {
            throw new IOException("Failed to rename compacted log");
        }

        this.raf = new RandomAccessFile(file, "rw");

        recover();
    }

    @Override
    public synchronized void reset(long index) throws IOException {
        raf.setLength(0);
        offsets.clear();
        firstIndex = index;
        lastIndex = index - 1;
        lastTerm = 0;
        raf.seek(0);
    }

    @Override
    public synchronized void close() throws IOException {
        raf.close();
    }
}
