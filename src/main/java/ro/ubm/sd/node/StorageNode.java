
package ro.ubm.sd.node;

import ro.ubm.sd.common.BullyElection;
import ro.ubm.sd.common.HeartbeatManager;
import ro.ubm.sd.common.NodeDiscovery;
import ro.ubm.sd.common.Protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class StorageNode {

    private final String nodeId;
    private final int port;
    private final Path storageDirectory;
    private final String multicastGroup;

    private ServerSocket serverSocket;
    private NodeDiscovery discovery;
    private HeartbeatManager heartbeatManager;
    private BullyElection bullyElection;

    private final ExecutorService clientPool =
            Executors.newCachedThreadPool();

    private final ScheduledExecutorService electionMonitor =
            Executors.newSingleThreadScheduledExecutor();

    private volatile boolean running = true;

    public StorageNode(
            String nodeId,
            int port,
            String directory,
            String multicastGroup
    ) {
        this.nodeId = nodeId;
        this.port = port;

        this.storageDirectory = Path.of(directory)
                .toAbsolutePath()
                .normalize();

        this.multicastGroup = multicastGroup;
    }

    public void start() {
        try {
            Files.createDirectories(storageDirectory);

            serverSocket = new ServerSocket(port);

            discovery = new NodeDiscovery(
                    nodeId,
                    port,
                    multicastGroup,
                    4446
            );

            discovery.start();

            heartbeatManager = new HeartbeatManager(
                    nodeId,
                    port,
                    discovery
            );

            heartbeatManager.start();

            bullyElection = new BullyElection(
                    nodeId,
                    discovery
            );

            // Prima alegere după ce nodurile au timp să se descopere.
            electionMonitor.schedule(
                    bullyElection::startElection,
                    5,
                    TimeUnit.SECONDS
            );

            // Verifică periodic dacă actualul coordonator a căzut.
            electionMonitor.scheduleAtFixedRate(
                    this::checkCoordinator,
                    2,
                    2,
                    TimeUnit.SECONDS
            );

            System.out.println("--------------------------------");
            System.out.println(" DISTRIBUTED STORAGE NODE");
            System.out.println("--------------------------------");
            System.out.println("Node ID: " + nodeId);
            System.out.println("TCP Port: " + port);
            System.out.println("Directory: " + storageDirectory);
            System.out.println(
                    "Multicast: " + multicastGroup + ":4446"
            );
            System.out.println("--------------------------------");
            System.out.println("[" + nodeId + "] ready.");

            while (running) {
                try {
                    Socket client = serverSocket.accept();
                    clientPool.submit(() -> handleClient(client));

                } catch (IOException e) {
                    if (running) {
                        System.err.println(
                                "[SERVER] " + e.getMessage()
                        );
                    }
                }
            }

        } catch (Exception e) {
            System.err.println(
                    "[SERVER] Startup error: " + e.getMessage()
            );
        } finally {
            shutdown();
        }
    }

    private void checkCoordinator() {
        if (!running || bullyElection == null
                || heartbeatManager == null) {
            return;
        }

        String coordinatorId = bullyElection.getCoordinatorId();

        if (coordinatorId == null
                || coordinatorId.equals(nodeId)
                || bullyElection.isElectionRunning()) {
            return;
        }

        HeartbeatManager.NodeStatusInfo status =
                heartbeatManager.getNodeStatuses().get(coordinatorId);

        if (status != null
                && "CAZUT".equals(
                        String.valueOf(status.getStatus())
                )) {

            System.out.println(
                    "[ELECTION] Coordinator " + coordinatorId
                            + " is down. Starting election."
            );

            bullyElection.startElection();
        }
    }

    private void handleClient(Socket socket) {
        try (
                Socket client = socket;
                DataInputStream in = new DataInputStream(
                        client.getInputStream()
                );
                DataOutputStream out = new DataOutputStream(
                        client.getOutputStream()
                )
        ) {
            String header = Protocol.readHeader(in);

            if (header == null || header.isBlank()) {
                Protocol.writeHeader(out, "ERROR empty request");
                return;
            }

            // Mesajul COORDINATOR conține separatorul |.
            if (header.startsWith("COORDINATOR|")) {
                String newCoordinatorId = header.substring(
                        "COORDINATOR|".length()
                ).trim();

                bullyElection.setCoordinator(newCoordinatorId);
                Protocol.writeHeader(out, "OK");
                return;
            }

            String[] parts = header.trim().split("\\s+");

            switch (parts[0].toUpperCase()) {
                case "PUT" -> handlePut(parts, in, out);

                case "GET" -> handleGet(parts, out);

                case "PING" ->
                        Protocol.writeHeader(out, "PONG " + nodeId);

                case "INFO" ->
                        Protocol.writeHeader(
                                out,
                                "NODE=" + nodeId
                                        + "|PORT=" + port
                                        + "|COORDINATOR="
                                        + String.valueOf(
                                                bullyElection.getCoordinatorId()
                                        )
                        );

                case "LIST" ->
                        Protocol.writeHeader(out, listFiles());

                case "NODES" ->
                        Protocol.writeHeader(out, listNodes());

                case "RM" -> handleRemove(parts, out);

                case "ELECTION" ->
                        Protocol.writeHeader(
                                out,
                                bullyElection.handleElectionRequest()
                        );

                default ->
                        Protocol.writeHeader(
                                out,
                                "ERROR unknown command"
                        );
            }

        } catch (Exception e) {
            System.err.println(
                    "[CLIENT] Connection error: " + e.getMessage()
            );
        }
    }

    private void handlePut(
            String[] parts,
            DataInputStream in,
            DataOutputStream out
    ) throws IOException {

        if (parts.length != 4) {
            Protocol.writeHeader(
                    out,
                    "ERROR usage: PUT filename size sha256"
            );
            return;
        }

        String filename = parts[1];
        long size;

        try {
            size = Long.parseLong(parts[2]);
        } catch (NumberFormatException e) {
            Protocol.writeHeader(out, "ERROR invalid file size");
            return;
        }

        if (size < 0) {
            Protocol.writeHeader(out, "ERROR invalid file size");
            return;
        }

        String expectedChecksum = parts[3];

        if (!expectedChecksum.matches("[a-fA-F0-9]{64}")) {
            Protocol.writeHeader(out, "ERROR invalid SHA-256");
            return;
        }

        Path destination;

        try {
            destination = resolveFile(filename);
        } catch (IllegalArgumentException e) {
            Protocol.writeHeader(out, "ERROR invalid filename");
            return;
        }

        Path temporary = destination.resolveSibling(
                destination.getFileName() + ".part"
        );

        try {
            String actualChecksum =
                    Protocol.receiveBytes(in, size, temporary);

            if (!actualChecksum.equalsIgnoreCase(expectedChecksum)) {
                Files.deleteIfExists(temporary);
                Protocol.writeHeader(out, "ERROR checksum mismatch");
                return;
            }

            moveFile(temporary, destination);

            System.out.println(
                    "[STORAGE] Saved " + filename
                            + " (" + size + " bytes)"
            );

            Protocol.writeHeader(
                    out,
                    "STORED " + actualChecksum
            );

        } catch (IOException e) {
            Files.deleteIfExists(temporary);
            throw e;
        }
    }

    private void handleGet(
            String[] parts,
            DataOutputStream out
    ) throws IOException {

        if (parts.length != 2) {
            Protocol.writeHeader(out, "ERROR usage: GET filename");
            return;
        }

        Path file;

        try {
            file = resolveFile(parts[1]);
        } catch (IllegalArgumentException e) {
            Protocol.writeHeader(out, "ERROR invalid filename");
            return;
        }

        if (!Files.exists(file) || !Files.isRegularFile(file)) {
            Protocol.writeHeader(out, "NOT_FOUND");
            return;
        }

        long size = Files.size(file);
        String checksum = Protocol.checksum(file);

        Protocol.writeHeader(
                out,
                "FILE " + size + " " + checksum
        );

        Protocol.sendBytes(file, out);

        System.out.println(
                "[STORAGE] Sent " + file.getFileName()
        );
    }

    private void handleRemove(
            String[] parts,
            DataOutputStream out
    ) throws IOException {

        if (parts.length != 2) {
            Protocol.writeHeader(out, "ERROR usage: RM filename");
            return;
        }

        Path file;

        try {
            file = resolveFile(parts[1]);
        } catch (IllegalArgumentException e) {
            Protocol.writeHeader(out, "ERROR invalid filename");
            return;
        }

        if (!Files.exists(file)) {
            Protocol.writeHeader(out, "ERROR file not found");
            return;
        }

        Files.delete(file);

        Protocol.writeHeader(
                out,
                "REMOVED " + parts[1]
        );
    }

    private String listFiles() throws IOException {
        try (var files = Files.list(storageDirectory)) {
            return "FILES " + files
                    .filter(Files::isRegularFile)
                    .map(path -> path.getFileName().toString())
                    .filter(name -> !name.endsWith(".part"))
                    .sorted()
                    .reduce((a, b) -> a + "," + b)
                    .orElse("");
        }
    }

    private String listNodes() {
        StringBuilder result = new StringBuilder("NODES");

        Map<String, HeartbeatManager.NodeStatusInfo> statuses =
                heartbeatManager.getNodeStatuses();

        statuses.values().stream()
                .sorted((a, b) -> a.getId().compareTo(b.getId()))
                .forEach(node -> result.append(" | ")
                        .append(node.getId())
                        .append("@")
                        .append(node.getAddress())
                        .append(":")
                        .append(node.getPort())
                        .append(" ")
                        .append(node.getStatus())
                        .append(" lastHeartbeat=")
                        .append(node.getLastHeartbeat())
                );

        return result.toString();
    }

    private Path resolveFile(String filename) {
        if (filename == null
                || filename.isBlank()
                || filename.contains("/")
                || filename.contains("\\")
                || filename.equals(".")
                || filename.equals("..")) {
            throw new IllegalArgumentException("Invalid filename");
        }

        Path file = storageDirectory.resolve(filename).normalize();

        if (!file.startsWith(storageDirectory)) {
            throw new IllegalArgumentException("Invalid path");
        }

        return file;
    }

    private void moveFile(Path source, Path destination)
            throws IOException {

        try {
            Files.move(
                    source,
                    destination,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE
            );
        } catch (java.nio.file.AtomicMoveNotSupportedException e) {
            Files.move(
                    source,
                    destination,
                    StandardCopyOption.REPLACE_EXISTING
            );
        }
    }

    private void shutdown() {
        running = false;

        electionMonitor.shutdownNow();

        if (bullyElection != null) {
            bullyElection.close();
        }

        if (heartbeatManager != null) {
            heartbeatManager.close();
        }

        if (discovery != null) {
            discovery.close();
        }

        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
        }

        clientPool.shutdownNow();

        System.out.println(
                "[SERVER] Node " + nodeId + " stopped."
        );
    }

    private static Map<String, String> parseArguments(String[] args) {
        Map<String, String> config = new HashMap<>();

        for (int i = 0; i < args.length; i++) {
            String key = args[i];

            if (!key.startsWith("--")) {
                throw new IllegalArgumentException(
                        "Unknown argument: " + key
                );
            }

            if (i + 1 >= args.length || args[i + 1].startsWith("--")) {
                throw new IllegalArgumentException(
                        "Missing value for " + key
                );
            }

            config.put(key, args[++i]);
        }

        return config;
    }

    public static void main(String[] args) {
        try {
            Map<String, String> config = parseArguments(args);

            String id = config.getOrDefault("--id", "n1");

            int port = Integer.parseInt(
                    config.getOrDefault("--port", "7001")
            );

            String directory = config.getOrDefault(
                    "--dir", "data/n1"
            );

            String group = config.getOrDefault(
                    "--group", "239.255.0.1"
            );

            StorageNode node = new StorageNode(
                    id,
                    port,
                    directory,
                    group
            );

            Runtime.getRuntime().addShutdownHook(
                    new Thread(node::shutdown)
            );

            node.start();

        } catch (Exception e) {
            System.err.println(
                    "[StorageNode] " + e.getMessage()
            );

            System.err.println(
                    "Usage: --id n1 --port 7001 --dir data/n1 "
                            + "--group 239.255.0.1"
            );

            System.exit(1);
        }
    }
}
