
package ro.ubm.sd.client;

import ro.ubm.sd.common.Protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class Client {

    private static final int MULTICAST_PORT = 4446;
    private static final int DISCOVERY_SECONDS = 3;
    private static final int CONNECT_TIMEOUT = 2000;

    private final String multicastGroup;
    private final AtomicInteger nextNode = new AtomicInteger(0);

    public Client(String multicastGroup) {
        this.multicastGroup = multicastGroup;
    }

    public static void main(String[] args) {
        String group = "239.255.0.1";

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--group") && i + 1 < args.length) {
                group = args[++i];
            }
        }

        try {
            new Client(group).start();
        } catch (Exception e) {
            System.err.println("[CLIENT] " + e.getMessage());
        }
    }

    private void start() {
        System.out.println("==================================");
        System.out.println(" DISTRIBUTED STORAGE CLIENT");
        System.out.println("==================================");
        System.out.println("Multicast group: " + multicastGroup);
        printHelp();

        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                System.out.print("\nclient> ");

                if (!scanner.hasNextLine()) {
                    break;
                }

                String command = scanner.nextLine().trim();

                if (command.isEmpty()) {
                    continue;
                }

                String[] parts = command.split("\\s+");

                try {
                    switch (parts[0].toLowerCase()) {
                        case "nodes" -> showNodes();

                        // put <filepath> = distribuire automată
                        // put <nodeId> <filepath> = alegere manuală
                        case "put" -> {
                            if (parts.length == 2) {
                                uploadBalanced(Path.of(parts[1]));
                            } else if (parts.length == 3) {
                                upload(parts[1], Path.of(parts[2]));
                            } else {
                                throw new IllegalArgumentException(
                                        "Usage: put <filepath> OR put <nodeId> <filepath>"
                                );
                            }
                        }

                        case "get" -> {
                            require(parts, 4);
                            download(
                                    parts[1],
                                    parts[2],
                                    Path.of(parts[3])
                            );
                        }

                        case "ping" -> {
                            require(parts, 2);
                            sendSimple(parts[1], "PING");
                        }

                        case "info" -> {
                            require(parts, 2);
                            sendSimple(parts[1], "INFO");
                        }

                        case "list" -> {
                            require(parts, 2);
                            sendSimple(parts[1], "LIST");
                        }

                        case "rm" -> {
                            require(parts, 3);
                            sendSimple(
                                    parts[1],
                                    "RM " + parts[2]
                            );
                        }

                        case "bench" -> {
                            require(parts, 2);
                            benchmark(Path.of(parts[1]));
                        }

                        case "help" -> printHelp();

                        case "exit" -> {
                            System.out.println("Client stopped.");
                            return;
                        }

                        default -> System.out.println(
                                "Unknown command. Type help."
                        );
                    }

                } catch (Exception e) {
                    System.out.println("ERROR: " + e.getMessage());
                }
            }
        }
    }

    // ---------------------------------------------------------
    // NODE DISCOVERY
    // ---------------------------------------------------------

    private void showNodes() throws IOException {
        Map<String, NodeInfo> nodes = discoverNodes();

        System.out.println("DISCOVERED NODES");

        if (nodes.isEmpty()) {
            System.out.println("No nodes discovered.");
            return;
        }

        for (NodeInfo node : nodes.values()) {
            boolean active = isReachable(node);

            System.out.println(
                    node.id + "@" + node.address + ":" + node.port
                            + (active ? " ACTIV" : " CAZUT")
            );
        }
    }

    private Map<String, NodeInfo> discoverNodes()
            throws IOException {

        Map<String, NodeInfo> nodes = new TreeMap<>();

        InetAddress group = InetAddress.getByName(multicastGroup);

        if (!group.isMulticastAddress()) {
            throw new IOException(
                    "Invalid multicast address: " + multicastGroup
            );
        }

        try (MulticastSocket socket =
                     new MulticastSocket(MULTICAST_PORT)) {

            socket.setReuseAddress(true);
            socket.joinGroup(group);

            long deadline = System.currentTimeMillis()
                    + DISCOVERY_SECONDS * 1000L;

            byte[] buffer = new byte[1024];

            while (System.currentTimeMillis() < deadline) {
                long remaining =
                        deadline - System.currentTimeMillis();

                socket.setSoTimeout((int) Math.max(1, remaining));

                DatagramPacket packet = new DatagramPacket(
                        buffer,
                        buffer.length
                );

                try {
                    socket.receive(packet);
                } catch (java.net.SocketTimeoutException e) {
                    break;
                }

                String message = new String(
                        packet.getData(),
                        packet.getOffset(),
                        packet.getLength(),
                        StandardCharsets.UTF_8
                );

                String[] parts = message.split(";");

                if (parts.length != 3
                        || !parts[0].equals("HELLO")) {
                    continue;
                }

                try {
                    String id = parts[1];
                    int port = Integer.parseInt(parts[2]);

                    nodes.put(
                            id,
                            new NodeInfo(
                                    id,
                                    packet.getAddress().getHostAddress(),
                                    port
                            )
                    );

                } catch (NumberFormatException ignored) {
                    // Ignore malformed announcements.
                }
            }

            socket.leaveGroup(group);
        }

        return nodes;
    }

    private List<NodeInfo> getActiveNodes() throws IOException {
        List<NodeInfo> active = new ArrayList<>(
                discoverNodes().values()
        );

        active.removeIf(node -> !isReachable(node));

        active.sort(Comparator.comparing(node -> node.id));

        if (active.isEmpty()) {
            throw new IOException("No active storage nodes found.");
        }

        return active;
    }

    private boolean isReachable(NodeInfo node) {
        try {
            String response = sendToNode(node, "PING");
            return response != null && response.startsWith("PONG");
        } catch (IOException e) {
            return false;
        }
    }

    private NodeInfo findNode(String nodeId)
            throws IOException {

        NodeInfo node = discoverNodes().get(nodeId);

        if (node == null) {
            throw new IOException(
                    "Node " + nodeId + " was not discovered."
            );
        }

        if (!isReachable(node)) {
            throw new IOException(
                    "Node " + nodeId + " is not active."
            );
        }

        return node;
    }

    // ---------------------------------------------------------
    // LOAD BALANCING - ROUND ROBIN
    // ---------------------------------------------------------

    private NodeInfo selectNode(List<NodeInfo> nodes) {
        int index = Math.floorMod(
                nextNode.getAndIncrement(),
                nodes.size()
        );

        return nodes.get(index);
    }

    private void uploadBalanced(Path file) throws IOException {
        List<NodeInfo> nodes = getActiveNodes();

        NodeInfo selected = selectNode(nodes);

        System.out.println(
                "[LOAD BALANCER] Selected node: " + selected.id
        );

        uploadToNode(selected, file);
    }

    private void upload(String nodeId, Path file)
            throws IOException {

        NodeInfo node = findNode(nodeId);
        uploadToNode(node, file);
    }

    private void uploadToNode(NodeInfo node, Path file)
            throws IOException {

        if (!Files.exists(file) || !Files.isRegularFile(file)) {
            throw new IOException("File not found: " + file);
        }

        long size = Files.size(file);
        String checksum = Protocol.checksum(file);
        String filename = file.getFileName().toString();

        try (
                Socket socket = new Socket()
        ) {
            socket.connect(
                    new java.net.InetSocketAddress(
                            node.address,
                            node.port
                    ),
                    CONNECT_TIMEOUT
            );

            try (
                    DataInputStream in = new DataInputStream(
                            socket.getInputStream()
                    );
                    DataOutputStream out = new DataOutputStream(
                            socket.getOutputStream()
                    );
                    var data = Files.newInputStream(file)
            ) {
                Protocol.writeHeader(
                        out,
                        "PUT " + filename + " " + size + " " + checksum
                );

                data.transferTo(out);
                out.flush();

                String response = Protocol.readHeader(in);

                if (response == null || !response.startsWith("STORED ")) {
                    throw new IOException(
                            "Upload failed on " + node.id + ": " + response
                    );
                }

                System.out.println(
                        "[UPLOAD] " + filename
                                + " -> " + node.id
                                + " (" + size + " bytes)"
                );
            }
        }
    }

    // ---------------------------------------------------------
    // SCALABILITY BENCHMARK
    // ---------------------------------------------------------

    private void benchmark(Path directory) throws Exception {
        if (!Files.isDirectory(directory)) {
            throw new IOException(
                    "Benchmark path is not a directory: " + directory
            );
        }

        List<Path> files;

        try (var stream = Files.list(directory)) {
            files = stream
                    .filter(Files::isRegularFile)
                    .sorted()
                    .toList();
        }

        if (files.isEmpty()) {
            throw new IOException("No files found in benchmark directory.");
        }

        List<NodeInfo> nodes = getActiveNodes();

        System.out.println("----------------------------------");
        System.out.println("SCALABILITY BENCHMARK");
        System.out.println("----------------------------------");
        System.out.println("Files: " + files.size());
        System.out.println("Active nodes: " + nodes.size());
        System.out.println(
                "Nodes: " + nodes.stream()
                        .map(node -> node.id)
                        .toList()
        );

        int threads = nodes.size();
        ExecutorService pool = Executors.newFixedThreadPool(threads);

        long totalBytes = 0;

        for (Path file : files) {
            totalBytes += Files.size(file);
        }

        long start = System.nanoTime();

        try {
            List<Future<?>> tasks = new ArrayList<>();

            for (int i = 0; i < files.size(); i++) {
                final Path file = files.get(i);
                final NodeInfo node = nodes.get(i % nodes.size());

                tasks.add(pool.submit(() -> {
                    uploadToNode(node, file);
                    return null;
                }));
            }

            for (Future<?> task : tasks) {
                task.get();
            }

        } finally {
            pool.shutdownNow();
        }

        double seconds =
                (System.nanoTime() - start) / 1_000_000_000.0;

        double megabytes = totalBytes / (1024.0 * 1024.0);
        double throughput = megabytes / seconds;

        System.out.println("----------------------------------");
        System.out.println("BENCHMARK RESULTS");
        System.out.println("----------------------------------");
        System.out.println("Files uploaded: " + files.size());
        System.out.println("Nodes used: " + nodes.size());
        System.out.printf("Total data: %.2f MB%n", megabytes);
        System.out.printf("Total time: %.2f seconds%n", seconds);
        System.out.printf("Throughput: %.2f MB/s%n", throughput);
        System.out.println("----------------------------------");
    }

    // ---------------------------------------------------------
    // DOWNLOAD
    // ---------------------------------------------------------

    private void download(
            String nodeId,
            String filename,
            Path destination
    ) throws IOException {

        NodeInfo node = findNode(nodeId);

        Path outputFile = destination;

        if (Files.exists(destination)
                && Files.isDirectory(destination)) {
            outputFile = destination.resolve(filename);

        } else if (destination.toString().endsWith("/")
                || destination.toString().endsWith("\\")) {

            Files.createDirectories(destination);
            outputFile = destination.resolve(filename);
        }

        Path parent = outputFile.toAbsolutePath().getParent();

        if (parent != null) {
            Files.createDirectories(parent);
        }

        Path temporary = outputFile.resolveSibling(
                outputFile.getFileName() + ".part"
        );

        try (
                Socket socket = new Socket()
        ) {
            socket.connect(
                    new java.net.InetSocketAddress(
                            node.address,
                            node.port
                    ),
                    CONNECT_TIMEOUT
            );

            try (
                    DataInputStream in = new DataInputStream(
                            socket.getInputStream()
                    );
                    DataOutputStream out = new DataOutputStream(
                            socket.getOutputStream()
                    )
            ) {
                Protocol.writeHeader(out, "GET " + filename);

                String response = Protocol.readHeader(in);

                if ("NOT_FOUND".equals(response)) {
                    System.out.println(
                            "File not found on " + nodeId
                    );
                    return;
                }

                String[] parts = response.split("\\s+");

                if (parts.length != 3 || !parts[0].equals("FILE")) {
                    throw new IOException(
                            "Unexpected server response: " + response
                    );
                }

                long size = Long.parseLong(parts[1]);
                String expectedChecksum = parts[2];

                String actualChecksum = Protocol.receiveBytes(
                        in,
                        size,
                        temporary
                );

                if (!actualChecksum.equalsIgnoreCase(expectedChecksum)) {
                    Files.deleteIfExists(temporary);
                    throw new IOException("Checksum mismatch");
                }

                try {
                    Files.move(
                            temporary,
                            outputFile,
                            StandardCopyOption.REPLACE_EXISTING,
                            StandardCopyOption.ATOMIC_MOVE
                    );
                } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                    Files.move(
                            temporary,
                            outputFile,
                            StandardCopyOption.REPLACE_EXISTING
                    );
                }

                System.out.println(
                        "Downloaded: " + outputFile.toAbsolutePath()
                );
                System.out.println("Checksum OK: " + actualChecksum);
            }

        } catch (IOException e) {
            Files.deleteIfExists(temporary);
            throw e;
        }
    }

    // ---------------------------------------------------------
    // SIMPLE COMMANDS
    // ---------------------------------------------------------

    private void sendSimple(String nodeId, String command)
            throws IOException {

        NodeInfo node = findNode(nodeId);

        System.out.println(
                "Server: " + sendToNode(node, command)
        );
    }

    private String sendToNode(NodeInfo node, String command)
            throws IOException {

        try (Socket socket = new Socket()) {
            socket.connect(
                    new java.net.InetSocketAddress(
                            node.address,
                            node.port
                    ),
                    CONNECT_TIMEOUT
            );

            try (
                    DataInputStream in = new DataInputStream(
                            socket.getInputStream()
                    );
                    DataOutputStream out = new DataOutputStream(
                            socket.getOutputStream()
                    )
            ) {
                Protocol.writeHeader(out, command);
                return Protocol.readHeader(in);
            }
        }
    }

    private static void require(String[] parts, int count) {
        if (parts.length < count) {
            throw new IllegalArgumentException(
                    "Not enough arguments. Type help."
            );
        }
    }

    private static void printHelp() {
        System.out.println("""
                AVAILABLE COMMANDS:

                nodes
                put <filepath>
                put <nodeId> <filepath>
                get <nodeId> <filename> <destination>
                ping <nodeId>
                info <nodeId>
                list <nodeId>
                rm <nodeId> <filename>
                bench <directory>
                help
                exit

                put <filepath> selects a node automatically.
                bench <directory> uploads files in parallel to active nodes.
                """);
    }

    private static class NodeInfo {
        private final String id;
        private final String address;
        private final int port;

        private NodeInfo(String id, String address, int port) {
            this.id = id;
            this.address = address;
            this.port = port;
        }
    }
}
