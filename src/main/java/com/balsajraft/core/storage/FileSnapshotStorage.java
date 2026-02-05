package com.balsajraft.core.storage;

import java.io.*;

public class FileSnapshotStorage implements SnapshotStorage {
    private final File file;
    private volatile long lastIndex = 0;
    private volatile long lastTerm = 0;

    public FileSnapshotStorage(File baseDir) {
        this.file = new File(baseDir, "raft-snapshot.dat");
        try {
            readMetadata();
        } catch (IOException e) {
            // Ignore if file doesn't exist or is corrupt
        }
    }

    private void readMetadata() throws IOException {
        if (!file.exists()) return;
        try (DataInputStream in = new DataInputStream(new FileInputStream(file))) {
            lastIndex = in.readLong();
            lastTerm = in.readLong();
        }
    }

    @Override
    public synchronized void save(long index, long term, byte[] data) throws IOException {
        File tmp = new File(file.getParentFile(), "raft-snapshot.tmp");
        try (FileOutputStream fos = new FileOutputStream(tmp);
             DataOutputStream out = new DataOutputStream(fos)) {
            out.writeLong(index);
            out.writeLong(term);
            out.writeInt(data.length);
            out.write(data);
            out.flush();
            fos.getFD().sync();
        }
        if (!tmp.renameTo(file)) {
            file.delete();
            if (!tmp.renameTo(file)) {
                throw new IOException("Failed to save snapshot file");
            }
        }
        lastIndex = index;
        lastTerm = term;
    }

    @Override
    public synchronized byte[] load() throws IOException {
        if (!file.exists()) return null;
        try (DataInputStream in = new DataInputStream(new FileInputStream(file))) {
            lastIndex = in.readLong();
            lastTerm = in.readLong();
            int len = in.readInt();
            if (len < 0 || len > 100_000_000) {
                throw new IOException("Invalid snapshot length: " + len);
            }
            byte[] data = new byte[len];
            in.readFully(data);
            return data;
        }
    }

    @Override
    public synchronized long getLastSnapshotIndex() { return lastIndex; }

    @Override
    public synchronized long getLastSnapshotTerm() { return lastTerm; }
}
