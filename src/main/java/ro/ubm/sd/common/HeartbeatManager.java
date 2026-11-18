
package ro.ubm.sd.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class HeartbeatManager implements AutoCloseable {

    public enum NodeStatus {
        ACTIV,
        SUSPECT,
        CAZUT
    }

    private static final long HEARTBEAT_INTERVAL_MS = 1000;
    private static final long CONNECTION_TIMEOUT_MS = 700;
    private static final int FAILURES_BEFORE_DOWN = 3;

    private final String localNodeId;
    private final int localPort;
    private final NodeDiscovery discovery;

    private final Map<String, NodeStatusInfo> nodeStatuses =
            new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(4);

    private final AtomicBoolean started = new AtomicBoolean(false);

    public HeartbeatManager(
            String nodeId,
            int localPort,
            NodeDiscovery discovery
    ) {
        this.localNodeId = nodeId;
        this.localPort = localPort;
        this.discovery = discovery;

        nodeStatuses.put(
                nodeId,
                new NodeStatusInfo(
                        nodeId,
                        "127.0.0.1",
                        localPort,
                        NodeStatus.ACTIV,
                        System.currentTimeMillis()
                )
        );
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }

        scheduler.scheduleAtFixedRate(
                this::checkNodes,
                0,
                HEARTBEAT_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );

        System.out.println(
                "[HEARTBEAT] Started for node " + localNodeId
        );
    }

    private void checkNodes() {
        try {
            var discoveredNodes = discovery.getDiscoveredNodes();

            for (var discovered : discoveredNodes.values()) {

                String id = discovered.getId();

                if (id.equals(localNodeId)) {
                    continue;
                }

                NodeStatusInfo info = nodeStatuses.computeIfAbsent(
                        id,
                        key -> new NodeStatusInfo(
                                key,
                                discovered.getAddress(),
                                discovered.getPort(),
                                NodeStatus.SUSPECT,
                                0
                        )
                );

                info.updateAddress(
                        discovered.getAddress(),
                        discovered.getPort()
                );

                if (!info.tryStartProbe()) {
                    continue;
                }

                scheduler.execute(() -> probeNode(info));
            }

        } catch (Exception e) {
            System.err.println(
                    "[HEARTBEAT] Discovery error: " + e.getMessage()
            );
        }
    }

    private void probeNode(NodeStatusInfo node) {
        try {
            boolean reachable = sendPing(
                    node.getAddress(),
                    node.getPort()
            );

            if (reachable) {
                node.markAlive();
            } else {
                node.markFailure();
            }

        } finally {
            node.finishProbe();
        }
    }

    private boolean sendPing(String address, int port) {
        try (
                Socket socket = new Socket()
        ) {
            socket.connect(
                    new InetSocketAddress(address, port),
                    (int) CONNECTION_TIMEOUT_MS
            );

            socket.setSoTimeout((int) CONNECTION_TIMEOUT_MS);

            try (
                    DataOutputStream out = new DataOutputStream(
                            socket.getOutputStream()
                    );
                    DataInputStream in = new DataInputStream(
                            socket.getInputStream()
                    )
            ) {
                Protocol.writeHeader(out, "PING");

                String response = Protocol.readHeader(in);

                return response != null
                        && response.startsWith("PONG");
            }

        } catch (IOException e) {
            return false;
        }
    }

    public Map<String, NodeStatusInfo> getNodeStatuses() {
        return Map.copyOf(nodeStatuses);
    }

    public NodeStatus getStatus(String nodeId) {
        NodeStatusInfo info = nodeStatuses.get(nodeId);

        return info == null
                ? NodeStatus.CAZUT
                : info.getStatus();
    }

    public boolean isActive(String nodeId) {
        return getStatus(nodeId) == NodeStatus.ACTIV;
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }

    public static class NodeStatusInfo {

        private final String id;

        private volatile String address;
        private volatile int port;

        private volatile NodeStatus status;

        private volatile long lastHeartbeat;
        private volatile long lastFailure;

        private volatile int consecutiveFailures;

        private final AtomicBoolean probing =
                new AtomicBoolean(false);

        public NodeStatusInfo(
                String id,
                String address,
                int port,
                NodeStatus status,
                long lastHeartbeat
        ) {
            this.id = id;
            this.address = address;
            this.port = port;
            this.status = status;
            this.lastHeartbeat = lastHeartbeat;
        }

        public String getId() {
            return id;
        }

        public String getAddress() {
            return address;
        }

        public int getPort() {
            return port;
        }

        public NodeStatus getStatus() {
            return status;
        }

        public long getLastHeartbeat() {
            return lastHeartbeat;
        }

        public int getConsecutiveFailures() {
            return consecutiveFailures;
        }

        public void updateAddress(String address, int port) {
            this.address = address;
            this.port = port;
        }

        private boolean tryStartProbe() {
            return probing.compareAndSet(false, true);
        }

        private void finishProbe() {
            probing.set(false);
        }

        private synchronized void markAlive() {
            boolean recovered = status == NodeStatus.CAZUT
                    || status == NodeStatus.SUSPECT;

            lastHeartbeat = System.currentTimeMillis();
            lastFailure = 0;
            consecutiveFailures = 0;
            status = NodeStatus.ACTIV;

            if (recovered) {
                System.out.println(
                        "[HEARTBEAT] " + id + " -> ACTIV"
                );
            }
        }

        private synchronized void markFailure() {
            consecutiveFailures++;

            if (lastFailure == 0) {
                lastFailure = System.currentTimeMillis();
            }

            if (consecutiveFailures >= FAILURES_BEFORE_DOWN) {
                if (status != NodeStatus.CAZUT) {
                    status = NodeStatus.CAZUT;

                    System.out.println(
                            "[HEARTBEAT] " + id + " -> CAZUT"
                    );
                }
            } else {
                if (status != NodeStatus.SUSPECT) {
                    status = NodeStatus.SUSPECT;

                    System.out.println(
                            "[HEARTBEAT] " + id + " -> SUSPECT"
                    );
                }
            }
        }
    }
}
