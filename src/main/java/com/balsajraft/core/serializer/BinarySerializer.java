package com.balsajraft.core.serializer;

import com.balsajraft.core.model.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

// hand rolled binary format. protobuf felt like overkill
public class BinarySerializer {

    private static final byte TYPE_REQUEST_VOTE = 1;
    private static final byte TYPE_REQUEST_VOTE_RESPONSE = 2;
    private static final byte TYPE_APPEND_ENTRIES = 3;
    private static final byte TYPE_APPEND_ENTRIES_RESPONSE = 4;
    private static final byte TYPE_INSTALL_SNAPSHOT = 5;

    // Guard rails so one bad payload does not eat the heap
    private static final int MAX_ENTRIES_PER_MESSAGE = 10000;
    private static final int MAX_COMMAND_SIZE = 10 * 1024 * 1024; // 10 MB
    private static final int MAX_SNAPSHOT_SIZE = 100 * 1024 * 1024; // 100 MB

    public byte[] serialize(Object message) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        switch (message) {
            case RequestVote rv -> {
                out.writeByte(TYPE_REQUEST_VOTE);
                writeRequestVote(out, rv);
            }
            case RequestVoteResponse rvr -> {
                out.writeByte(TYPE_REQUEST_VOTE_RESPONSE);
                writeRequestVoteResponse(out, rvr);
            }
            case AppendEntries ae -> {
                out.writeByte(TYPE_APPEND_ENTRIES);
                writeAppendEntries(out, ae);
            }
            case AppendEntriesResponse aer -> {
                out.writeByte(TYPE_APPEND_ENTRIES_RESPONSE);
                writeAppendEntriesResponse(out, aer);
            }
            case InstallSnapshot is -> {
                out.writeByte(TYPE_INSTALL_SNAPSHOT);
                writeInstallSnapshot(out, is);
            }
            case null -> throw new IllegalArgumentException("Message cannot be null");
            default -> throw new IllegalArgumentException("Unknown message type: " + message.getClass());
        }

        return baos.toByteArray();
    }

    public RaftMessage deserialize(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream in = new DataInputStream(bais);
        byte type = in.readByte();

        return switch (type) {
            case TYPE_REQUEST_VOTE -> readRequestVote(in);
            case TYPE_REQUEST_VOTE_RESPONSE -> readRequestVoteResponse(in);
            case TYPE_APPEND_ENTRIES -> readAppendEntries(in);
            case TYPE_APPEND_ENTRIES_RESPONSE -> readAppendEntriesResponse(in);
            case TYPE_INSTALL_SNAPSHOT -> readInstallSnapshot(in);
            default -> throw new IOException("Unknown message type byte: " + type);
        };
    }

    private void writeInstallSnapshot(DataOutput out, InstallSnapshot msg) throws IOException {
        out.writeLong(msg.getTerm());
        out.writeUTF(msg.getLeaderId());
        out.writeLong(msg.getLastIncludedIndex());
        out.writeLong(msg.getLastIncludedTerm());
        if (msg.getData() == null) {
            out.writeInt(0);
        } else {
            out.writeInt(msg.getData().length);
            out.write(msg.getData());
        }
    }

    private InstallSnapshot readInstallSnapshot(DataInput in) throws IOException {
        long term = in.readLong();
        String leaderId = in.readUTF();
        long lastIncludedIndex = in.readLong();
        long lastIncludedTerm = in.readLong();
        int len = in.readInt();
        if (len < 0 || len > MAX_SNAPSHOT_SIZE) {
            throw new IOException("Invalid snapshot size: " + len);
        }
        byte[] data = new byte[len];
        if (len > 0) {
            in.readFully(data);
        }
        return new InstallSnapshot(term, leaderId, lastIncludedIndex, lastIncludedTerm, data);
    }

    // Write helpers

    private void writeRequestVote(DataOutput out, RequestVote msg) throws IOException {
        out.writeLong(msg.getTerm());
        out.writeUTF(msg.getCandidateId());
        out.writeLong(msg.getLastLogIndex());
        out.writeLong(msg.getLastLogTerm());
    }

    private void writeRequestVoteResponse(DataOutput out, RequestVoteResponse msg) throws IOException {
        out.writeLong(msg.getTerm());
        out.writeBoolean(msg.isVoteGranted());
    }

    private void writeAppendEntries(DataOutput out, AppendEntries msg) throws IOException {
        out.writeLong(msg.getTerm());
        out.writeUTF(msg.getLeaderId());
        out.writeLong(msg.getPrevLogIndex());
        out.writeLong(msg.getPrevLogTerm());
        out.writeLong(msg.getLeaderCommit());

        List<LogEntry> entries = msg.getEntries();
        out.writeInt(entries.size());
        for (LogEntry entry : entries) {
            writeLogEntry(out, entry);
        }
    }

    private void writeAppendEntriesResponse(DataOutput out, AppendEntriesResponse msg) throws IOException {
        out.writeLong(msg.getTerm());
        out.writeBoolean(msg.isSuccess());
        out.writeLong(msg.getMatchIndex());
    }

    public void writeLogEntry(DataOutput out, LogEntry entry) throws IOException {
        out.writeLong(entry.getTerm());
        out.writeLong(entry.getIndex());
        out.writeUTF(entry.getType().name());
        byte[] cmd = entry.getCommand();
        if (cmd == null) {
            out.writeInt(0);
        } else {
            out.writeInt(cmd.length);
            out.write(cmd);
        }
    }

    // Read helpers

    private RequestVote readRequestVote(DataInput in) throws IOException {
        long term = in.readLong();
        String candidateId = in.readUTF();
        long lastLogIndex = in.readLong();
        long lastLogTerm = in.readLong();
        return new RequestVote(term, candidateId, lastLogIndex, lastLogTerm);
    }

    private RequestVoteResponse readRequestVoteResponse(DataInput in) throws IOException {
        long term = in.readLong();
        boolean voteGranted = in.readBoolean();
        return new RequestVoteResponse(term, voteGranted);
    }

    private AppendEntries readAppendEntries(DataInput in) throws IOException {
        long term = in.readLong();
        String leaderId = in.readUTF();
        long prevLogIndex = in.readLong();
        long prevLogTerm = in.readLong();
        long leaderCommit = in.readLong();

        int entryCount = in.readInt();
        if (entryCount < 0 || entryCount > MAX_ENTRIES_PER_MESSAGE) {
            throw new IOException("Invalid entry count: " + entryCount);
        }
        List<LogEntry> entries = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            entries.add(readLogEntry(in));
        }

        return new AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }

    private AppendEntriesResponse readAppendEntriesResponse(DataInput in) throws IOException {
        long term = in.readLong();
        boolean success = in.readBoolean();
        long matchIndex = in.readLong();
        return new AppendEntriesResponse(term, success, matchIndex);
    }

    public LogEntry readLogEntry(DataInput in) throws IOException {
        long term = in.readLong();
        long index = in.readLong();
        String typeName = in.readUTF();
        LogEntry.EntryType type;
        try {
            type = LogEntry.EntryType.valueOf(typeName);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid entry type: " + typeName);
        }

        int cmdLen = in.readInt();
        if (cmdLen < 0 || cmdLen > MAX_COMMAND_SIZE) {
            throw new IOException("Invalid command size: " + cmdLen);
        }
        byte[] cmd = null;
        if (cmdLen > 0) {
            cmd = new byte[cmdLen];
            in.readFully(cmd);
        }
        return new LogEntry(term, index, cmd, type);
    }
}
