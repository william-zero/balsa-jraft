package com.balsajraft.core.storage;

import java.io.*;
import java.nio.charset.StandardCharsets;

// stores term and votedFor. needs to survive crashes
public class FileMetaStorage implements MetaStorage {
    private final File file;

    public FileMetaStorage(File baseDir) {
        this.file = new File(baseDir, "raft-meta.dat");
    }

    @Override
    public synchronized void save(long term, String votedFor) throws IOException {
        // Write to temp file then atomic rename for safety
        File tmp = new File(file.getParentFile(), "raft-meta.tmp");
        try (FileOutputStream fos = new FileOutputStream(tmp);
             DataOutputStream out = new DataOutputStream(fos)) {
            out.writeLong(term);
            if (votedFor == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeUTF(votedFor);
            }
            out.flush();
            fos.getFD().sync();
        }
        if (!tmp.renameTo(file)) {
             // If atomic rename fails, try delete + rename (Windows mostly, but good practice)
             file.delete();
             if (!tmp.renameTo(file)) {
                 throw new IOException("Failed to update meta file");
             }
        }
    }

    @Override
    public synchronized MetaState load() throws IOException {
        if (!file.exists()) {
            return new MetaState(0, null);
        }
        try (DataInputStream in = new DataInputStream(new FileInputStream(file))) {
            long term = in.readLong();
            String votedFor = null;
            if (in.readBoolean()) {
                votedFor = in.readUTF();
            }
            return new MetaState(term, votedFor);
        }
    }
}
