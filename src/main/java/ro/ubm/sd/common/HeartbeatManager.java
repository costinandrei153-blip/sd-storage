
package ro.ubm.sd.common;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HeartbeatManager implements AutoCloseable {

    private static final int CONNECT_TIMEOUT = 1500;
    private static final int READ_TIMEOUT = 1500;

    private final String nodeId;
    private final List<String> peers;
    private final Map<String, Boolean> nodeStatus =
            new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    public HeartbeatManager(String nodeId, List<String> peers) {
        this.nodeId = nodeId;
        this.peers = peers;

        for (String peer : peers) {
            nodeStatus.put(peer, false);
        }
    }

    public void start() {
        System.out.println("[HEARTBEAT] Monitor started for " + nodeId);

        scheduler.scheduleAtFixedRate(
                this::checkNodes,
                0,
                3,
                TimeUnit.SECONDS
        );
    }

    private void checkNodes() {
        for (String peer : peers) {
            boolean active = sendHeartbeat(peer);
            Boolean previous = nodeStatus.put(peer, active);

            if (previous == null || previous != active) {
                System.out.println(
                        "[HEARTBEAT] " + peer + " -> "
                                + (active ? "ACTIVE" : "UNREACHABLE")
                );
            }
        }
    }

    private boolean sendHeartbeat(String peer) {
        String[] parts = peer.split(":");

        if (parts.length != 2) {
            System.err.println("[HEARTBEAT] Invalid peer: " + peer);
            return false;
        }

        String host = parts[0];

        try {
            int port = Integer.parseInt(parts[1]);

            try (Socket socket = new Socket()) {
                socket.connect(
                        new InetSocketAddress(host, port),
                        CONNECT_TIMEOUT
                );

                socket.setSoTimeout(READ_TIMEOUT);

                PrintWriter out = new PrintWriter(
                        socket.getOutputStream(),
                        true
                );

                BufferedReader in = new BufferedReader(
                        new InputStreamReader(socket.getInputStream())
                );

                out.println("PING");

                String response = in.readLine();

                return response != null && response.startsWith("PONG");
            }

        } catch (Exception e) {
            return false;
        }
    }

    public Map<String, Boolean> getNodeStatus() {
        return Map.copyOf(nodeStatus);
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        System.out.println("[HEARTBEAT] Monitor stopped.");
    }
}
