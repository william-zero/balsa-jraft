package com.balsajraft.transport;

import com.balsajraft.core.serializer.BinarySerializer;
import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TcpTransport implements Transport {
    private static final Logger LOG = Logger.getLogger(TcpTransport.class.getName());

    private static final int CONNECT_TIMEOUT_MS = 5000;
    private static final int SO_TIMEOUT_MS = 30000;  // might need tuning

    private final String myId;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final BinarySerializer serializer = new BinarySerializer();
    private final ConcurrentHashMap<String, ConnectionHolder> connections = new ConcurrentHashMap<>();

    private volatile boolean running = false;
    private ServerSocket serverSocket;
    private TransportHandler handler;

    // Per-peer connection holder
    // writeLock keeps frame writes from interleaving
    private static class ConnectionHolder {
        final Socket socket;
        final DataOutputStream out;
        final Object writeLock = new Object();

        ConnectionHolder(Socket socket, DataOutputStream out) {
            this.socket = socket;
            this.out = out;
        }

        void close() {
            try { out.close(); } catch (IOException ignored) {}
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    public TcpTransport(String myId) {
        this.myId = myId;
    }

    @Override
    public void start(int port) throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;

        executor.submit(() -> {
            while (running) {
                try {
                    Socket socket = serverSocket.accept();
                    socket.setTcpNoDelay(true);
                    socket.setSoTimeout(SO_TIMEOUT_MS);
                    executor.submit(() -> handleConnection(socket));
                } catch (IOException e) {
                    if (running) {
                        LOG.log(Level.WARNING, "Accept failed", e);
                    }
                }
            }
        });
    }

    private void handleConnection(Socket socket) {
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()))) {
            while (running && !socket.isClosed()) {
                int length;
                try {
                    length = in.readInt();
                } catch (SocketTimeoutException e) {
                    // idle socket, keep waiting
                    continue;
                }
                if (length <= 0 || length > 10 * 1024 * 1024) {
                    // malformed frame or too big (>10MB)
                    // 10MB limit is arbitrary. bump if you have huge snapshots
                    LOG.warning("Invalid frame length: " + length);
                    break;
                }

                byte[] buffer = new byte[length];
                in.readFully(buffer);

                try (ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
                     DataInputStream msgIn = new DataInputStream(bais)) {

                    String senderId = msgIn.readUTF();
                    int msgLen = msgIn.readInt();
                    if (msgLen <= 0 || msgLen > buffer.length) {
                        LOG.warning("Invalid message length: " + msgLen);
                        break;
                    }
                    byte[] msgBytes = new byte[msgLen];
                    msgIn.readFully(msgBytes);

                    Object message = serializer.deserialize(msgBytes);

                    if (handler != null) {
                        handler.handleMessage(message, senderId);
                    }
                }
            }
        } catch (EOFException e) {
            // peer closed connection
        } catch (SocketException e) {
            // expected during shutdown
            if (running) {
                LOG.log(Level.FINE, "Socket error", e);
            }
        } catch (IOException e) {
            if (running) {
                LOG.log(Level.WARNING, "Connection handling error", e);
            }
        } finally {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    @Override
    public void send(Object message, String destinationId) {
        executor.submit(() -> {
            try {
                ConnectionHolder holder = getOrCreateConnection(destinationId);
                if (holder == null) {
                    return;
                }

                byte[] msgBytes = serializer.serialize(message);

                // Envelope layout: [senderId][msgLength][msgBytes]
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream envelopeOut = new DataOutputStream(baos);
                envelopeOut.writeUTF(myId);
                envelopeOut.writeInt(msgBytes.length);
                envelopeOut.write(msgBytes);
                byte[] envelope = baos.toByteArray();
                // serialize writes per socket
                synchronized (holder.writeLock) {
                    holder.out.writeInt(envelope.length);
                    holder.out.write(envelope);
                    holder.out.flush();
                }
            } catch (IOException e) {
                closeConnection(destinationId);
                LOG.log(Level.FINE, "Failed to send to " + destinationId, e);
            }
        });
    }

    private ConnectionHolder getOrCreateConnection(String destinationId) {
        ConnectionHolder existing = connections.get(destinationId);
        if (existing != null && !existing.socket.isClosed()) {
            return existing;
        }

        // Drop stale holder before reconnect
        if (existing != null) {
            connections.remove(destinationId, existing);
        }

        // computeIfAbsent keeps reconnect races sane
        return connections.computeIfAbsent(destinationId, id -> {
            try {
                String[] parts = id.split(":");
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);

                Socket socket = new Socket();
                socket.setTcpNoDelay(true);
                socket.setSoTimeout(SO_TIMEOUT_MS);
                socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);

                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                return new ConnectionHolder(socket, out);
            } catch (IOException e) {
                LOG.log(Level.FINE, "Failed to connect to " + id, e);
                return null;
            }
        });
    }

    private void closeConnection(String destinationId) {
        ConnectionHolder holder = connections.remove(destinationId);
        if (holder != null) {
            holder.close();
        }
    }

    @Override
    public void registerHandler(TransportHandler handler) {
        this.handler = handler;
    }

    @Override
    public void stop() throws IOException {
        running = false;
        if (serverSocket != null) {
            serverSocket.close();
        }
        executor.shutdownNow();
        for (ConnectionHolder holder : connections.values()) {
            holder.close();
        }
        connections.clear();
    }
}
