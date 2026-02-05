package com.balsajraft.transport;

import com.balsajraft.core.model.RequestVote;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class TcpTransportTest {

    private TcpTransport node1;
    private TcpTransport node2;
    private int port1;
    private int port2;

    @BeforeEach
    void setUp() throws IOException {
        port1 = reservePort();
        port2 = reservePort();
        while (port2 == port1) {
            port2 = reservePort();
        }
        node1 = new TcpTransport("localhost:" + port1);
        node2 = new TcpTransport("localhost:" + port2);
    }

    @AfterEach
    void tearDown() throws IOException {
        node1.stop();
        node2.stop();
    }

    @Test
    void testSendReceive() throws Exception {
        node1.start(port1);
        node2.start(port2);

        CountDownLatch latch = new CountDownLatch(1);
        final Object[] received = new Object[1];
        final String[] sender = new String[1];

        node2.registerHandler((msg, senderId) -> {
            received[0] = msg;
            sender[0] = senderId;
            latch.countDown();
        });

        RequestVote msg = new RequestVote(1, "node1", 0, 0);
        node1.send(msg, "localhost:" + port2);

        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertTrue(received[0] instanceof RequestVote);
        RequestVote receivedMsg = (RequestVote) received[0];
        assertEquals(1, receivedMsg.getTerm());
        assertEquals("localhost:" + port1, sender[0]);
    }

    @Test
    void testReconnectAfterDestinationComesUp() throws Exception {
        node1.start(port1);

        RequestVote msg = new RequestVote(1, "node1", 0, 0);
        node1.send(msg, "localhost:" + port2);
        Thread.sleep(200);

        CountDownLatch latch = new CountDownLatch(1);
        node2.registerHandler((m, senderId) -> {
            if (m instanceof RequestVote && ("localhost:" + port1).equals(senderId)) {
                latch.countDown();
            }
        });
        node2.start(port2);

        node1.send(msg, "localhost:" + port2);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    void testMultipleMessagesToSamePeer() throws Exception {
        node1.start(port1);
        node2.start(port2);

        int messageCount = 20;
        CountDownLatch latch = new CountDownLatch(messageCount);
        AtomicInteger receivedCount = new AtomicInteger();

        node2.registerHandler((m, senderId) -> {
            if (m instanceof RequestVote && ("localhost:" + port1).equals(senderId)) {
                receivedCount.incrementAndGet();
                latch.countDown();
            }
        });

        for (int i = 0; i < messageCount; i++) {
            node1.send(new RequestVote(1, "node1", i, i), "localhost:" + port2);
        }

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(messageCount, receivedCount.get());
    }

    @Test
    void testInvalidFrameFromSocketDoesNotBreakServer() throws Exception {
        node1.start(port1);
        node2.start(port2);

        try (java.net.Socket raw = new java.net.Socket("localhost", port2);
             java.io.DataOutputStream out = new java.io.DataOutputStream(raw.getOutputStream())) {
            out.writeInt(-1);
            out.flush();
        }

        CountDownLatch latch = new CountDownLatch(1);
        node2.registerHandler((m, senderId) -> {
            if (m instanceof RequestVote && ("localhost:" + port1).equals(senderId)) {
                latch.countDown();
            }
        });

        node1.send(new RequestVote(2, "node1", 0, 0), "localhost:" + port2);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    private static int reservePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
