package com.balsajraft.core.serializer;

import com.balsajraft.core.model.*;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class BinarySerializerTest {

    private final BinarySerializer serializer = new BinarySerializer();

    @Test
    void testRequestVote() throws IOException {
        RequestVote original = new RequestVote(1, "node1", 10, 5);
        byte[] bytes = serializer.serialize(original);
        RequestVote decoded = (RequestVote) serializer.deserialize(bytes);

        assertEquals(original.getTerm(), decoded.getTerm());
        assertEquals(original.getCandidateId(), decoded.getCandidateId());
        assertEquals(original.getLastLogIndex(), decoded.getLastLogIndex());
        assertEquals(original.getLastLogTerm(), decoded.getLastLogTerm());
    }

    @Test
    void testAppendEntries() throws IOException {
        LogEntry entry1 = new LogEntry(1, 1, "cmd1".getBytes(), LogEntry.EntryType.COMMAND);
        LogEntry entry2 = new LogEntry(1, 2, "cmd2".getBytes(), LogEntry.EntryType.COMMAND);

        AppendEntries original = new AppendEntries(2, "leader1", 0, 0, Arrays.asList(entry1, entry2), 1);
        byte[] bytes = serializer.serialize(original);
        AppendEntries decoded = (AppendEntries) serializer.deserialize(bytes);

        assertEquals(original.getTerm(), decoded.getTerm());
        assertEquals(original.getLeaderId(), decoded.getLeaderId());
        assertEquals(2, decoded.getEntries().size());
        assertArrayEquals(entry1.getCommand(), decoded.getEntries().get(0).getCommand());
    }
}
