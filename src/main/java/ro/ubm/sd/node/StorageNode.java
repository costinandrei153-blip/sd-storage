
package ro.ubm.sd.node;

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

public class StorageNode {

    private final String nodeId;
    private final int port;
    private final Path storageDirectory;
    private final String multicastGroup;

    private ServerSocket serverSocket;
    private NodeDiscovery discovery;

    private final ExecutorService clientPool =
            Executors.newCachedThreadPool();

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

            System.out.println("--------------------------------");
            System.out.println(" DISTRIBUTED STORAGE NODE");
            System.out.println("--------------------------------");
            System.out.println("Node ID: " + nodeId);
            System.out.println("TCP Port: " + port);
            System.out.println("Directory: " + storageDirectory);
            System.out.println("Multicast: " + multicastGroup + ":4446");
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
            String[] parts = header.trim().split("\\s+");

            if (parts.length == 0) {
                Protocol.writeHeader(out, "ERROR empty request");
                return;
            }

            switch (parts[0].toUpperCase()) {
                case "PUT" -> handlePut(parts, in, out);
                case "GET" -> handleGet(parts, out);
                case "PING" ->
                        Protocol.writeHeader(out, "PONG " + nodeId);
                case "INFO" ->
                        Protocol.writeHeader(
                                out,
                                "NODE " + nodeId + " PORT " + port
                        );
                case "LIST" ->
                        Protocol.writeHeader(out, listFiles());
                case "RM" -> handleRemove(parts, out);
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

            Files.move(
                    temporary,
                    destination,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE
            );

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

        System.out.println("[STORAGE] Sent " + file.getFileName());
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
        Protocol.writeHeader(out, "REMOVED " + parts[1]);
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

    private void shutdown() {
        running = false;

        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
        }

        if (discovery != null) {
            discovery.close();
        }

        clientPool.shutdownNow();

        System.out.println("[SERVER] Node " + nodeId + " stopped.");
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
