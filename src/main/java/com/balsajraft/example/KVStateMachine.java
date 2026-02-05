package com.balsajraft.example;

import com.balsajraft.core.StateMachine;
import com.balsajraft.core.model.LogEntry;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KVStateMachine implements StateMachine {
    private static final Logger LOG = Logger.getLogger(KVStateMachine.class.getName());

    private final ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

    public String get(String key) {
        return map.get(key);
    }

    @Override
    public void apply(LogEntry entry) {
        byte[] command = entry.getCommand();
        if (command == null) {
            return;
        }
        // Tiny parser, intentionally boring
        String cmd = new String(command, StandardCharsets.UTF_8);
        String[] parts = cmd.split(":", 3);
        if (parts.length == 3 && "PUT".equals(parts[0])) {
            map.put(parts[1], parts[2]);
        }
    }

    @Override
    public byte[] takeSnapshot() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(map.size());
            for (Map.Entry<String, String> entry : map.entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeUTF(entry.getValue());
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create snapshot", e);
        }
    }

    @Override
    public void installSnapshot(byte[] snapshot) {
        if (snapshot == null || snapshot.length == 0) {
            return;
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(snapshot);
             DataInputStream in = new DataInputStream(bais)) {
            map.clear();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String key = in.readUTF();
                String val = in.readUTF();
                map.put(key, val);
            }
        } catch (IOException e) {
            LOG.log(Level.SEVERE, "Failed to install snapshot", e);
        }
    }

    public int size() {
        return map.size();
    }
}
