package ro.ubm.sd.client;

import ro.ubm.sd.common.*;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Client {
    private static final int MULTICAST_PORT = 4446;
    private static final int DISCOVERY_MS = 2500;
    private static final int CONNECT_TIMEOUT = 2000;
    private final String multicastGroup;
    private final AtomicInteger roundRobin = new AtomicInteger();

    public Client(String multicastGroup) { this.multicastGroup = multicastGroup; }

    public static void main(String[] args) {
        String group = "239.255.0.1";
        for (int i = 0; i < args.length - 1; i++)
            if ("--group".equals(args[i])) group = args[++i];
        new Client(group).start();
    }

    private void start() {
        System.out.println("DISTRIBUTED STORAGE CLIENT - Stage 6 (RMI + 2 replicas)");
        help();
        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                System.out.print("\nclient> ");
                if (!scanner.hasNextLine()) return;
                String line = scanner.nextLine().trim();
                if (line.isEmpty()) continue;
                try {
                    List<String> a = tokenize(line);
                    String cmd = a.get(0).toLowerCase(Locale.ROOT);
                    switch (cmd) {
                        case "put" -> {
                            if (a.size() != 2) throw new IllegalArgumentException("put <filepath>");
                            put(Path.of(a.get(1)));
                        }
                        case "get" -> {
                            if (a.size() != 3) throw new IllegalArgumentException("get <filename> <destination>");
                            get(a.get(1), Path.of(a.get(2)));
                        }
                        case "ls", "list" -> listMetadata();
                        case "rm" -> {
                            if (a.size() != 2) throw new IllegalArgumentException("rm <filename>");
                            remove(a.get(1));
                        }
                        case "nodes" -> nodes();
                        case "bench" -> {
                            if (a.size() != 2) throw new IllegalArgumentException("bench <directory>");
                            benchmark(Path.of(a.get(1)));
                        }
                        case "help" -> help();
                        case "exit" -> { System.out.println("Client stopped."); return; }
                        default -> System.out.println("Unknown command. Type help.");
                    }
                } catch (Exception e) {
                    System.out.println("ERROR: " + e.getMessage());
                }
            }
        }
    }

    private List<String> tokenize(String line) {
        List<String> result = new ArrayList<>();
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("\"([^\"]*)\"|(\\S+)").matcher(line);
        while (m.find()) result.add(m.group(1) != null ? m.group(1) : m.group(2));
        return result;
    }

    private void put(Path file) throws Exception {
        if (!Files.isRegularFile(file)) throw new IOException("File not found: " + file);
        List<NodeInfo> active = activeNodes();
        if (active.size() < 2) throw new IOException("At least two active storage nodes are required.");
        NodeInfo first = active.get(Math.floorMod(roundRobin.getAndIncrement(), active.size()));
        NodeInfo second = active.stream().filter(n -> !n.id.equals(first.id)).findFirst()
                .orElseThrow(() -> new IOException("No second distinct node."));
        List<String> locations = List.of(first.id, second.id);
        String id = UUID.randomUUID().toString();
        String name = file.getFileName().toString();
        long size = Files.size(file);
        String checksum = Protocol.checksum(file);

        System.out.println("[REPLICATION] Primary: " + first.id + " | Replica: " + second.id);
        boolean firstStored = false;
        boolean secondStored = false;
        try {
            uploadTo(first, file, id, locations, size, checksum);
            firstStored = true;
            uploadTo(second, file, id, locations, size, checksum);
            secondStored = true;

            MetadataService metadata = coordinatorService(active);
            metadata.commit(new FileMetadata(id, name, size, checksum, locations));
            System.out.println("[COMMIT] " + name + " | UUID=" + id
                    + " | copies=" + locations + " | SHA-256=" + checksum);
        } catch (Exception e) {
            if (firstStored) {
                try { request(first, "RM " + name); } catch (Exception ignored) {}
            }
            if (secondStored) {
                try { request(second, "RM " + name); } catch (Exception ignored) {}
            }
            throw e;
        }
    }

    private void uploadTo(NodeInfo node, Path file, String id, List<String> locations,
                          long size, String checksum) throws IOException {
        String header = "PUT " + file.getFileName() + " " + size + " " + checksum
                + " " + id + " " + String.join(",", locations);
        try (Socket socket = connect(node);
             DataInputStream in = new DataInputStream(socket.getInputStream());
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             InputStream data = Files.newInputStream(file)) {
            Protocol.writeHeader(out, header);
            data.transferTo(out);
            out.flush();
            String response = Protocol.readHeader(in);
            if (!response.startsWith("STORED "))
                throw new IOException("Upload failed on " + node.id + ": " + response);
            System.out.println("[UPLOAD] " + file.getFileName() + " -> " + node.id);
        }
    }

    private void get(String filename, Path destination) throws Exception {
        List<NodeInfo> discovered = discoverNodes();
        MetadataService service = coordinatorService(discovered);
        FileMetadata metadata = service.findByName(filename);
        if (metadata == null) throw new IOException("File not found in coordinator metadata: " + filename);

        Path output = destination;
        if (Files.exists(destination) && Files.isDirectory(destination))
            output = destination.resolve(filename);
        Path parent = output.toAbsolutePath().getParent();
        if (parent != null) Files.createDirectories(parent);
        Path temp = output.resolveSibling(output.getFileName() + ".part");

        Map<String, NodeInfo> byId = new HashMap<>();
        for (NodeInfo n : discovered) byId.put(n.id, n);

        IOException last = null;
        for (String location : metadata.getLocations()) {
            NodeInfo node = byId.get(location);
            if (node == null) continue;
            try {
                downloadFrom(node, filename, temp, output, metadata);
                System.out.println("[DOWNLOAD] " + filename + " <- " + node.id);
                System.out.println("[CHECKSUM OK] " + metadata.getChecksum());
                return;
            } catch (IOException e) {
                last = e;
                Files.deleteIfExists(temp);
                System.out.println("[REPLICA] " + location + " unavailable/invalid; trying next copy.");
            }
        }
        throw new IOException("No valid replica could provide the file.", last);
    }

    private void downloadFrom(NodeInfo node, String filename, Path temp, Path output,
                              FileMetadata metadata) throws IOException {
        try (Socket socket = connect(node);
             DataInputStream in = new DataInputStream(socket.getInputStream());
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
            Protocol.writeHeader(out, "GET " + filename);
            String response = Protocol.readHeader(in);
            if (!response.startsWith("FILE ")) throw new IOException(response);
            String[] p = response.split("\\s+");
            long size = Long.parseLong(p[1]);
            if (size != metadata.getSize() || !p[2].equalsIgnoreCase(metadata.getChecksum()))
                throw new IOException("Metadata mismatch on " + node.id);
            String actual = Protocol.receiveBytes(in, size, temp);
            if (!actual.equalsIgnoreCase(metadata.getChecksum())) throw new IOException("Checksum mismatch");
            move(temp, output);
        } catch (IOException e) {
            Files.deleteIfExists(temp);
            throw e;
        }
    }

    private void listMetadata() throws Exception {
        MetadataService service = coordinatorService(discoverNodes());
        List<FileMetadata> items = service.listFiles();
        if (items.isEmpty()) { System.out.println("No files stored."); return; }
        System.out.println("FILES (coordinator RMI)");
        for (FileMetadata m : items)
            System.out.println(m.getName() + " | " + m.getSize() + " bytes | UUID="
                    + m.getId() + " | copies=" + m.getLocations()
                    + " | SHA-256=" + m.getChecksum());
    }

    private void remove(String filename) throws Exception {
        List<NodeInfo> nodes = discoverNodes();
        MetadataService service = coordinatorService(nodes);
        FileMetadata m = service.findByName(filename);
        if (m == null) throw new IOException("File not found: " + filename);
        Map<String, NodeInfo> byId = new HashMap<>();
        for (NodeInfo n : nodes) byId.put(n.id, n);
        boolean any = false;
        for (String id : m.getLocations()) {
            NodeInfo node = byId.get(id);
            if (node == null) continue;
            try {
                String response = request(node, "RM " + filename);
                any |= response.startsWith("REMOVED");
                System.out.println("[DELETE] " + id + ": " + response);
            } catch (IOException e) {
                System.out.println("[DELETE] " + id + " unavailable; metadata retained until cleanup.");
            }
        }
        if (any) {
            service.removeByName(filename);
            System.out.println("[COMMIT] Metadata removed for " + filename);
        } else throw new IOException("No replica could be reached; metadata was not removed.");
    }

    private void nodes() throws Exception {
        List<NodeInfo> list = discoverNodes();
        for (NodeInfo n : list) {
            String info;
            try { info = request(n, "INFO"); }
            catch (IOException e) { info = "UNREACHABLE"; }
            System.out.println(n.id + "@" + n.address + ":" + n.port + " " + info);
        }
        MetadataService metadata = coordinatorService(list);
        System.out.println("RMI coordinator: " + metadata.getCoordinatorId());
    }

    private void benchmark(Path directory) throws Exception {
        if (!Files.isDirectory(directory)) throw new IOException("Not a directory: " + directory);
        List<Path> paths;
        try (var s = Files.list(directory)) { paths = s.filter(Files::isRegularFile).sorted().toList(); }
        long start = System.nanoTime();
        int count = 0;
        for (Path p : paths) { put(p); count++; }
        double seconds = (System.nanoTime() - start) / 1_000_000_000.0;
        long bytes = 0;
        for (Path p : paths) bytes += Files.size(p);
        System.out.printf("BENCHMARK: files=%d data=%.2f MB time=%.2f s throughput=%.2f MB/s (includes 2 replicas + RMI commit)%n",
                count, bytes / 1048576.0, seconds, (bytes / 1048576.0) / seconds);
    }

    private MetadataService coordinatorService(List<NodeInfo> nodes) throws Exception {
        for (NodeInfo n : nodes) {
            try {
                String info = request(n, "INFO");
                String coordinator = field(info, "COORDINATOR");
                if (coordinator == null || coordinator.equals("null")) continue;
                NodeInfo target = nodes.stream().filter(x -> x.id.equals(coordinator)).findFirst().orElse(null);
                if (target == null) continue;
                Registry registry = LocateRegistry.getRegistry(target.address, target.port + 10000);
                return (MetadataService) registry.lookup(MetadataServer.SERVICE_NAME);
            } catch (Exception ignored) {}
        }
        throw new IOException("Coordinator RMI service not found. Check that the elected coordinator is active.");
    }

    private String field(String info, String key) {
        for (String p : info.split("\\|"))
            if (p.startsWith(key + "=")) return p.substring((key + "=").length());
        return null;
    }

    private List<NodeInfo> activeNodes() throws IOException {
        List<NodeInfo> result = new ArrayList<>();
        for (NodeInfo n : discoverNodes()) {
            try { if (request(n, "PING").startsWith("PONG")) result.add(n); }
            catch (IOException ignored) {}
        }
        result.sort(Comparator.comparing(n -> n.id));
        return result;
    }

    private List<NodeInfo> discoverNodes() throws IOException {
        Map<String, NodeInfo> found = new TreeMap<>();
        InetAddress group = InetAddress.getByName(multicastGroup);
        try (MulticastSocket socket = new MulticastSocket(MULTICAST_PORT)) {
            socket.setReuseAddress(true);
            socket.joinGroup(group);
            long until = System.currentTimeMillis() + DISCOVERY_MS;
            byte[] buffer = new byte[1024];
            while (System.currentTimeMillis() < until) {
                socket.setSoTimeout((int)Math.max(1, until - System.currentTimeMillis()));
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                try { socket.receive(packet); } catch (SocketTimeoutException e) { break; }
                String message = new String(packet.getData(), packet.getOffset(),
                        packet.getLength(), StandardCharsets.UTF_8);
                String[] p = message.split(";");
                if (p.length == 3 && p[0].equals("HELLO")) {
                    try {
                        found.put(p[1], new NodeInfo(p[1], packet.getAddress().getHostAddress(),
                                Integer.parseInt(p[2])));
                    } catch (NumberFormatException ignored) {}
                }
            }
            socket.leaveGroup(group);
        }
        return new ArrayList<>(found.values());
    }

    private String request(NodeInfo node, String command) throws IOException {
        try (Socket socket = connect(node);
             DataInputStream in = new DataInputStream(socket.getInputStream());
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
            Protocol.writeHeader(out, command);
            return Protocol.readHeader(in);
        }
    }

    private Socket connect(NodeInfo n) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(n.address, n.port), CONNECT_TIMEOUT);
        socket.setSoTimeout(5000);
        return socket;
    }

    private void move(Path from, Path to) throws IOException {
        try { Files.move(from, to, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE); }
        catch (AtomicMoveNotSupportedException e) { Files.move(from, to, StandardCopyOption.REPLACE_EXISTING); }
    }

    private void help() {
        System.out.println("""
                COMMANDS:
                  nodes
                  put <filepath>
                  get <filename> <destination>
                  ls
                  rm <filename>
                  bench <directory>
                  help
                  exit

                put creates exactly two copies on distinct active nodes.
                get reads coordinator metadata and tries both replicas.
                Paths containing spaces must be enclosed in double quotes.
                """);
    }

    private record NodeInfo(String id, String address, int port) {}
}
