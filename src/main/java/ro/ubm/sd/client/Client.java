
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
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

public class Client {

    private static final int MULTICAST_PORT = 4446;
    private static final int DISCOVERY_SECONDS = 3;

    private final String multicastGroup;

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

                        case "put" -> {
                            require(parts, 3);
                            upload(parts[1], Path.of(parts[2]));
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

    private void showNodes() throws IOException {
        Map<String, NodeInfo> nodes = discoverNodes();

        System.out.println("Discovered nodes:");

        if (nodes.isEmpty()) {
            System.out.println("No nodes discovered.");
            return;
        }

        nodes.values().forEach(node ->
                System.out.println(
                        node.id + "@" + node.address + ":" + node.port
                )
        );
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

            // Join multicast group using InetAddress.
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

                    NodeInfo info = new NodeInfo(
                            id,
                            packet.getAddress().getHostAddress(),
                            port
                    );

                    nodes.put(id, info);

                } catch (NumberFormatException ignored) {
                    // Ignore malformed announcements.
                }
            }

            // Leave using the same InetAddress overload.
            socket.leaveGroup(group);
        }

        return nodes;
    }

    private NodeInfo findNode(String nodeId)
            throws IOException {

        Map<String, NodeInfo> nodes = discoverNodes();

        NodeInfo node = nodes.get(nodeId);

        if (node == null) {
            throw new IOException(
                    "Node " + nodeId
                            + " was not discovered. Run 'nodes' first."
            );
        }

        return node;
    }

    private void upload(String nodeId, Path file)
            throws IOException {

        if (!Files.exists(file) || !Files.isRegularFile(file)) {
            throw new IOException("File not found: " + file);
        }

        NodeInfo node = findNode(nodeId);

        long size = Files.size(file);
        String checksum = Protocol.checksum(file);
        String filename = file.getFileName().toString();

        try (
                Socket socket = new Socket(node.address, node.port);
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

            System.out.println("Server: " + response);

            if (response.startsWith("STORED ")) {
                System.out.println(
                        "Upload completed successfully."
                );
            }
        }
    }

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
                Socket socket = new Socket(node.address, node.port);
                DataInputStream in = new DataInputStream(
                        socket.getInputStream()
                );
                DataOutputStream out = new DataOutputStream(
                        socket.getOutputStream()
                )
        ) {
            Protocol.writeHeader(out, "GET " + filename);

            String response = Protocol.readHeader(in);

            if (response.equals("NOT_FOUND")) {
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

            Files.move(
                    temporary,
                    outputFile,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE
            );

            System.out.println(
                    "Downloaded: " + outputFile.toAbsolutePath()
            );
            System.out.println("Checksum OK: " + actualChecksum);

        } catch (IOException e) {
            Files.deleteIfExists(temporary);
            throw e;
        }
    }

    private void sendSimple(String nodeId, String command)
            throws IOException {

        NodeInfo node = findNode(nodeId);

        try (
                Socket socket = new Socket(node.address, node.port);
                DataInputStream in = new DataInputStream(
                        socket.getInputStream()
                );
                DataOutputStream out = new DataOutputStream(
                        socket.getOutputStream()
                )
        ) {
            Protocol.writeHeader(out, command);

            System.out.println(
                    "Server: " + Protocol.readHeader(in)
            );
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
                put <nodeId> <filepath>
                get <nodeId> <filename> <destination>
                ping <nodeId>
                info <nodeId>
                list <nodeId>
                rm <nodeId> <filename>
                help
                exit
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
