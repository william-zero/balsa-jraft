package com.balsajraft.example;

import com.balsajraft.core.RaftNode;
import com.balsajraft.core.storage.FileLogStorage;
import com.balsajraft.core.storage.FileMetaStorage;
import com.balsajraft.core.storage.FileSnapshotStorage;
import com.balsajraft.core.storage.LogStorage;
import com.balsajraft.core.storage.MetaStorage;
import com.balsajraft.core.storage.SnapshotStorage;
import com.balsajraft.transport.TcpTransport;
import com.balsajraft.transport.Transport;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

// simple kv store for demo. dont use for anything real
public class RaftKVServer {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: java -cp ... com.raft.example.RaftKVServer <myId> [peers...]");
            System.out.println("Example: localhost:8001 localhost:8002 localhost:8003");
            return;
        }

        String myId = args[0];
        List<String> peers = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            peers.add(args[i]);
        }

        File baseDir = new File("data-" + myId.replace(":", "_"));
        baseDir.mkdirs();

        LogStorage log = new FileLogStorage(baseDir);
        MetaStorage meta = new FileMetaStorage(baseDir);
        SnapshotStorage snapshot = new FileSnapshotStorage(baseDir);
        Transport transport = new TcpTransport(myId);
        KVStateMachine stateMachine = new KVStateMachine();

        RaftNode node = new RaftNode(myId, peers, transport, log, meta, snapshot, stateMachine);
        node.start();

        int raftPort = Integer.parseInt(myId.split(":")[1]);
        int httpPort = raftPort + 1000;
        HttpServer server = HttpServer.create(new InetSocketAddress(httpPort), 0);

        server.createContext("/get", exchange -> {
            String query = exchange.getRequestURI().getQuery();
            String key = query.split("=")[1];
            String value = stateMachine.get(key);
            String response = value == null ? "null" : value;
            sendResponse(exchange, 200, response);
        });

        server.createContext("/put", exchange -> {
            // Format: key=x&value=y
            String query = exchange.getRequestURI().getQuery();
            String[] params = query.split("&");
            String key = params[0].split("=")[1];
            String value = params[1].split("=")[1];

            byte[] command = ("PUT:" + key + ":" + value).getBytes();
            try {
                boolean success = node.replicate(command).get();
                sendResponse(exchange, 200, success ? "OK" : "FAILED (Not Leader?)");
            } catch (Exception e) {
                sendResponse(exchange, 500, e.getMessage());
            }
        });

        server.createContext("/add", exchange -> {
            String query = exchange.getRequestURI().getQuery();
            String peer = query.split("=")[1];
            try {
                boolean success = node.addServer(peer).get();
                sendResponse(exchange, 200, success ? "OK" : "FAILED");
            } catch (Exception e) {
                sendResponse(exchange, 500, e.getMessage());
            }
        });

        server.setExecutor(null); // creates a default executor
        server.start();
        System.out.println("KV Server started on HTTP port " + httpPort + " (Raft port " + raftPort + ")");
    }

    private static void sendResponse(HttpExchange exchange, int code, String response) throws IOException {
        byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
