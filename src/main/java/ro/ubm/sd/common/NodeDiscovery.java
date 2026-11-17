
package ro.ubm.sd.common;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class NodeDiscovery implements AutoCloseable {

    public static final String DEFAULT_GROUP = "239.255.0.1";
    public static final int DEFAULT_PORT = 4446;
    private static final int BUFFER_SIZE = 1024;

    private final String nodeId;
    private final int tcpPort;
    private final String groupAddress;
    private final int multicastPort;

    private final Map<String, NodeInfo> discoveredNodes =
            new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    private MulticastSocket socket;
    private volatile boolean running;

    public NodeDiscovery(String nodeId, int tcpPort) {
        this(nodeId, tcpPort, DEFAULT_GROUP, DEFAULT_PORT);
    }

    public NodeDiscovery(
            String nodeId,
            int tcpPort,
            String groupAddress,
            int multicastPort
    ) {
        this.nodeId = nodeId;
        this.tcpPort = tcpPort;
        this.groupAddress = groupAddress;
        this.multicastPort = multicastPort;
    }

    public void start() throws IOException {
        if (running) {
            return;
        }

        InetAddress group = InetAddress.getByName(groupAddress);

        if (!group.isMulticastAddress()) {
            throw new IOException(
                    "Invalid multicast address: " + groupAddress
            );
        }

        socket = new MulticastSocket(multicastPort);
        socket.setReuseAddress(true);
        socket.setTimeToLive(1);

        SocketAddress groupSocketAddress =
                new InetSocketAddress(group, multicastPort);

        socket.joinGroup(groupSocketAddress, null);

        running = true;

        Thread receiver = new Thread(
                this::receiveLoop,
                "discovery-receiver-" + nodeId
        );

        receiver.setDaemon(true);
        receiver.start();

        scheduler.scheduleAtFixedRate(
                this::announce,
                0,
                2,
                TimeUnit.SECONDS
        );

        System.out.println(
                "[DISCOVERY] " + nodeId
                        + " joined " + groupAddress
                        + ":" + multicastPort
        );
    }

    private void announce() {
        if (!running || socket == null) {
            return;
        }

        try {
            String message =
                    "HELLO;" + nodeId + ";" + tcpPort;

            byte[] data = message.getBytes(StandardCharsets.UTF_8);

            InetAddress group = InetAddress.getByName(groupAddress);

            DatagramPacket packet = new DatagramPacket(
                    data,
                    data.length,
                    group,
                    multicastPort
            );

            socket.send(packet);

        } catch (IOException e) {
            if (running) {
                System.err.println(
                        "[DISCOVERY] Announcement failed: "
                                + e.getMessage()
                );
            }
        }
    }

    private void receiveLoop() {
        byte[] buffer = new byte[BUFFER_SIZE];

        while (running) {
            try {
                DatagramPacket packet = new DatagramPacket(
                        buffer,
                        buffer.length
                );

                socket.receive(packet);

                String message = new String(
                        packet.getData(),
                        packet.getOffset(),
                        packet.getLength(),
                        StandardCharsets.UTF_8
                );

                processAnnouncement(message, packet);

            } catch (IOException e) {
                if (running) {
                    System.err.println(
                            "[DISCOVERY] Receive error: "
                                    + e.getMessage()
                    );
                }
            }
        }
    }

    private void processAnnouncement(
            String message,
            DatagramPacket packet
    ) {
        String[] parts = message.split(";");

        if (parts.length != 3 || !parts[0].equals("HELLO")) {
            return;
        }

        String discoveredId = parts[1];

        if (discoveredId.equals(nodeId)) {
            return;
        }

        try {
            int discoveredPort = Integer.parseInt(parts[2]);

            NodeInfo info = new NodeInfo(
                    discoveredId,
                    packet.getAddress().getHostAddress(),
                    discoveredPort,
                    System.currentTimeMillis()
            );

            NodeInfo previous = discoveredNodes.put(
                    discoveredId,
                    info
            );

            if (previous == null) {
                System.out.println(
                        "[DISCOVERY] New node discovered: "
                                + discoveredId
                                + " at "
                                + info.getAddress()
                                + ":" + discoveredPort
                );
            }

        } catch (NumberFormatException e) {
            System.err.println(
                    "[DISCOVERY] Invalid TCP port: " + parts[2]
            );
        }
    }

    public Map<String, NodeInfo> getDiscoveredNodes() {
        return Map.copyOf(discoveredNodes);
    }

    public void close() {
        running = false;

        scheduler.shutdownNow();

        if (socket != null && !socket.isClosed()) {
            socket.close();
        }

        discoveredNodes.clear();
    }

    public static class NodeInfo {

        private final String id;
        private final String address;
        private final int port;
        private final long lastSeen;

        public NodeInfo(
                String id,
                String address,
                int port,
                long lastSeen
        ) {
            this.id = id;
            this.address = address;
            this.port = port;
            this.lastSeen = lastSeen;
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

        public long getLastSeen() {
            return lastSeen;
        }

        public String getEndpoint() {
            return address + ":" + port;
        }
    }
}
