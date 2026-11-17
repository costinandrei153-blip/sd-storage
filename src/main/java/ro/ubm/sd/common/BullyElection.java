
package ro.ubm.sd.common;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class BullyElection implements AutoCloseable {

    private final String nodeId;
    private final int nodePriority;
    private final List<String> peers;

    private volatile String coordinatorId;
    private volatile boolean coordinator;

    private final AtomicBoolean electionRunning =
            new AtomicBoolean(false);

    private final ExecutorService executor =
            Executors.newSingleThreadExecutor();

    public BullyElection(String nodeId, List<String> peers) {
        this.nodeId = nodeId;
        this.nodePriority = extractPriority(nodeId);
        this.peers = peers;
    }

    private int extractPriority(String id) {
        String number = id.replaceAll("\\D+", "");

        if (number.isEmpty()) {
            throw new IllegalArgumentException(
                    "Node ID must contain a number: " + id
            );
        }

        return Integer.parseInt(number);
    }

    public void startElection() {
        if (!electionRunning.compareAndSet(false, true)) {
            return;
        }

        executor.submit(() -> {
            try {
                System.out.println("[ELECTION] Starting election: " + nodeId);

                boolean higherNodeResponded = false;

                for (String peer : peers) {
                    int priority = getPeerPriority(peer);

                    if (priority <= nodePriority) {
                        continue;
                    }

                    String response = sendMessage(peer, "ELECTION");

                    if (response != null && response.startsWith("OK")) {
                        higherNodeResponded = true;
                        System.out.println(
                                "[ELECTION] Higher node responded: " + peer
                        );
                    }
                }

                if (!higherNodeResponded) {
                    becomeCoordinator();
                } else {
                    System.out.println(
                            "[ELECTION] Waiting for coordinator announcement..."
                    );
                }

            } finally {
                electionRunning.set(false);
            }
        });
    }

    public String handleElectionRequest() {
        System.out.println("[ELECTION] Received election request.");

        startElection();

        return "OK|" + nodeId;
    }

    public void becomeCoordinator() {
        coordinator = true;
        coordinatorId = nodeId;

        System.out.println(
                "[ELECTION] " + nodeId + " IS THE COORDINATOR!"
        );

        for (String peer : peers) {
            int priority = getPeerPriority(peer);

            if (priority > 0 && priority < nodePriority) {
                sendMessage(peer, "COORDINATOR|" + nodeId);
            }
        }
    }

    public void setCoordinator(String newCoordinatorId) {
        coordinatorId = newCoordinatorId;
        coordinator = nodeId.equals(newCoordinatorId);

        System.out.println(
                "[ELECTION] Coordinator updated: " + newCoordinatorId
        );
    }

    private int getPeerPriority(String peer) {
        String response = sendMessage(peer, "INFO");

        if (response == null || !response.startsWith("NODE=")) {
            return -1;
        }

        try {
            String[] fields = response.split("\\|");

            String peerId = fields[0].substring("NODE=".length());

            return extractPriority(peerId);

        } catch (Exception e) {
            return -1;
        }
    }

    private String sendMessage(String peer, String message) {
        String[] parts = peer.split(":");

        if (parts.length != 2) {
            return null;
        }

        try {
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            try (Socket socket = new Socket()) {
                socket.connect(
                        new InetSocketAddress(host, port),
                        1500
                );

                socket.setSoTimeout(2000);

                PrintWriter out = new PrintWriter(
                        socket.getOutputStream(),
                        true,
                        StandardCharsets.UTF_8
                );

                BufferedReader in = new BufferedReader(
                        new InputStreamReader(
                                socket.getInputStream(),
                                StandardCharsets.UTF_8
                        )
                );

                out.println(message);

                return in.readLine();
            }

        } catch (Exception e) {
            return null;
        }
    }

    public boolean isCoordinator() {
        return coordinator;
    }

    public String getCoordinatorId() {
        return coordinatorId;
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
