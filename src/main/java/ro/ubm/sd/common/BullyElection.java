
package ro.ubm.sd.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class BullyElection implements AutoCloseable {

    private static final int CONNECT_TIMEOUT = 1000;
    private static final int READ_TIMEOUT = 1500;
    private static final int ELECTION_TIMEOUT_SECONDS = 3;

    private final String nodeId;
    private final int nodePriority;
    private final NodeDiscovery discovery;

    private final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor();

    private final AtomicBoolean electionRunning =
            new AtomicBoolean(false);

    private volatile String coordinatorId;
    private volatile boolean coordinator;
    private volatile boolean closed;

    public BullyElection(String nodeId, NodeDiscovery discovery) {
        this.nodeId = nodeId;
        this.nodePriority = extractPriority(nodeId);
        this.discovery = discovery;
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

    private List<NodeDiscovery.NodeInfo> getPeers() {
        List<NodeDiscovery.NodeInfo> peers = new ArrayList<>(
                discovery.getDiscoveredNodes().values()
        );

        peers.sort(Comparator.comparingInt(
                node -> extractPriority(node.getId())
        ));

        return peers;
    }

    public void startElection() {
        if (closed || !electionRunning.compareAndSet(false, true)) {
            return;
        }

        executor.execute(() -> {
            try {
                runElection();
            } finally {
                electionRunning.set(false);
            }
        });
    }

    private void runElection() {
        System.out.println(
                "[ELECTION] Election started by " + nodeId
        );

        coordinator = false;
        boolean higherNodeResponded = false;

        for (NodeDiscovery.NodeInfo peer : getPeers()) {
            if (closed) {
                return;
            }

            int priority = extractPriority(peer.getId());

            if (priority <= nodePriority) {
                continue;
            }

            String response = sendMessage(
                    peer,
                    "ELECTION"
            );

            if (response != null && response.startsWith("OK")) {
                higherNodeResponded = true;

                System.out.println(
                        "[ELECTION] Higher node responded: "
                                + peer.getId()
                );
            }
        }

        if (!higherNodeResponded) {
            becomeCoordinator();
            return;
        }

        System.out.println(
                "[ELECTION] Waiting for coordinator announcement..."
        );

        executor.schedule(() -> {
            if (!closed
                    && !coordinator
                    && (coordinatorId == null
                    || !isCoordinatorAlive())) {

                coordinatorId = null;
                startElection();
            }
        }, ELECTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private boolean isCoordinatorAlive() {
        if (coordinatorId == null) {
            return false;
        }

        if (coordinatorId.equals(nodeId)) {
            return true;
        }

        for (NodeDiscovery.NodeInfo peer : getPeers()) {
            if (peer.getId().equals(coordinatorId)) {
                return sendMessage(peer, "PING") != null;
            }
        }

        return false;
    }

    public String handleElectionRequest() {
        System.out.println(
                "[ELECTION] Election request received by " + nodeId
        );

        startElection();

        return "OK|" + nodeId;
    }

    public synchronized void becomeCoordinator() {
        if (closed) {
            return;
        }

        coordinator = true;
        coordinatorId = nodeId;

        System.out.println(
                "[ELECTION] " + nodeId + " IS THE COORDINATOR!"
        );

        for (NodeDiscovery.NodeInfo peer : getPeers()) {
            sendMessage(
                    peer,
                    "COORDINATOR|" + nodeId
            );
        }
    }

    public synchronized void setCoordinator(String newCoordinatorId) {
        if (newCoordinatorId == null
                || newCoordinatorId.isBlank()) {
            return;
        }

        int priority;

        try {
            priority = extractPriority(newCoordinatorId);
        } catch (IllegalArgumentException e) {
            return;
        }

        if (priority < nodePriority) {
            System.out.println(
                    "[ELECTION] Rejected lower priority coordinator: "
                            + newCoordinatorId
            );

            startElection();
            return;
        }

        if (priority == nodePriority
                && !newCoordinatorId.equals(nodeId)) {
            return;
        }

        coordinatorId = newCoordinatorId;
        coordinator = newCoordinatorId.equals(nodeId);

        System.out.println(
                "[ELECTION] Coordinator is now " + coordinatorId
        );
    }

    private String sendMessage(
            NodeDiscovery.NodeInfo peer,
            String message
    ) {
        try (Socket socket = new Socket()) {
            socket.connect(
                    new InetSocketAddress(
                            peer.getAddress(),
                            peer.getPort()
                    ),
                    CONNECT_TIMEOUT
            );

            socket.setSoTimeout(READ_TIMEOUT);

            try (
                    DataOutputStream out = new DataOutputStream(
                            socket.getOutputStream()
                    );
                    DataInputStream in = new DataInputStream(
                            socket.getInputStream()
                    )
            ) {
                Protocol.writeHeader(out, message);
                return Protocol.readHeader(in);
            }

        } catch (IOException e) {
            return null;
        }
    }

    public boolean isCoordinator() {
        return coordinator;
    }

    public String getCoordinatorId() {
        return coordinatorId;
    }

    public boolean isElectionRunning() {
        return electionRunning.get();
    }

    @Override
    public void close() {
        closed = true;
        executor.shutdownNow();

        System.out.println(
                "[ELECTION] Election manager stopped for " + nodeId
        );
    }
}
