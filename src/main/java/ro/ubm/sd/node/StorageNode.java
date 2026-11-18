package ro.ubm.sd.node;

import ro.ubm.sd.common.*;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class StorageNode {
    private final String nodeId;
    private final int port;
    private final int rmiPort;
    private final Path storageDirectory;
    private final String multicastGroup;

    private ServerSocket serverSocket;
    private NodeDiscovery discovery;
    private HeartbeatManager heartbeatManager;
    private BullyElection bullyElection;
    private MetadataServer metadataServer;

    private final ExecutorService clientPool = Executors.newCachedThreadPool();
    private final ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();
    private volatile boolean running = true;
    private volatile String lastObservedCoordinator = "";

    public StorageNode(String nodeId, int port, String directory, String multicastGroup) {
        this.nodeId = nodeId;
        this.port = port;
        this.rmiPort = port + 10000;
        this.storageDirectory = Path.of(directory).toAbsolutePath().normalize();
        this.multicastGroup = multicastGroup;
    }

    public void start() {
        try {
            Files.createDirectories(storageDirectory);
            serverSocket = new ServerSocket(port);

            metadataServer = new MetadataServer(rmiPort, nodeId);
            metadataServer.start();

            discovery = new NodeDiscovery(nodeId, port, multicastGroup, 4446);
            discovery.start();

            heartbeatManager = new HeartbeatManager(nodeId, port, discovery);
            heartbeatManager.start();

            bullyElection = new BullyElection(nodeId, discovery);

            monitor.schedule(bullyElection::startElection, 5, TimeUnit.SECONDS);
            monitor.scheduleAtFixedRate(this::monitorCoordinator, 2, 1, TimeUnit.SECONDS);

            System.out.println("--------------------------------");
            System.out.println(" DISTRIBUTED STORAGE NODE");
            System.out.println("--------------------------------");
            System.out.println("Node ID: " + nodeId);
            System.out.println("TCP Port: " + port);
            System.out.println("RMI Port: " + rmiPort);
            System.out.println("Directory: " + storageDirectory);
            System.out.println("[" + nodeId + "] ready.");

            while (running) {
                try {
                    Socket socket = serverSocket.accept();
                    clientPool.submit(() -> handleClient(socket));
                } catch (IOException e) {
                    if (running) System.err.println("[SERVER] " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("[SERVER] Startup error: " + e.getMessage());
        } finally {
            shutdown();
        }
    }

    private void monitorCoordinator() {
        if (!running || bullyElection == null || metadataServer == null) return;

        String coordinator = bullyElection.getCoordinatorId();
        if (coordinator == null || coordinator.isBlank()) return;

        // Keep every node's local RMI service synchronized with the Bully result.
        try {
            metadataServer.getService().setCoordinatorId(coordinator);
        } catch (Exception e) {
            System.err.println("[RMI] Could not update coordinator ID: " + e.getMessage());
        }

        // Rebuild the catalog every time this node becomes coordinator,
        // including when it was coordinator earlier and later lost the role.
        if (!coordinator.equals(lastObservedCoordinator)) {
            String previous = lastObservedCoordinator;
            lastObservedCoordinator = coordinator;

            if (nodeId.equals(coordinator)) {
                System.out.println("[RMI] Coordinator transition "
                        + (previous.isBlank() ? "(initial)" : previous)
                        + " -> " + nodeId + "; rebuilding metadata catalog.");
                try {
                    rebuildMetadataFromNodes();
                } catch (Exception e) {
                    // Retry on the next monitor tick instead of marking recovery complete.
                    lastObservedCoordinator = previous;
                    System.err.println("[RMI] Metadata recovery failed: " + e.getMessage());
                }
            } else {
                System.out.println("[RMI] Current coordinator: " + coordinator);
            }
        }

        if (!coordinator.equals(nodeId)) {
            HeartbeatManager.NodeStatusInfo status =
                    heartbeatManager.getNodeStatuses().get(coordinator);
            if (status != null
                    && "CAZUT".equals(String.valueOf(status.getStatus()))
                    && !bullyElection.isElectionRunning()) {
                System.out.println("[ELECTION] Coordinator " + coordinator
                        + " is down. Starting election.");
                bullyElection.startElection();
            }
        }
    }

    private void rebuildMetadataFromNodes() throws IOException {
        Map<String, FileMetadata> merged = new TreeMap<>();
        mergeCatalog(merged, readLocalCatalog());

        for (NodeDiscovery.NodeInfo peer : discovery.getDiscoveredNodes().values()) {
            try (Socket socket = connect(peer);
                 DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                 DataInputStream in = new DataInputStream(socket.getInputStream())) {
                Protocol.writeHeader(out, "CATALOG");
                String header = Protocol.readHeader(in);
                if (!header.startsWith("CATALOG ")) continue;
                int count = Integer.parseInt(header.substring(8).trim());
                for (int i = 0; i < count; i++) {
                    FileMetadata item = decodeMetadata(Protocol.readHeader(in));
                    merged.merge(item.getName(), item, StorageNode::preferMetadata);
                }
            } catch (Exception ignored) {
                // A peer may have stopped during recovery; other replicas remain usable.
            }
        }

        List<FileMetadata> recovered = new ArrayList<>(merged.values());
        metadataServer.getService().replaceAll(recovered);
        metadataServer.getService().setCoordinatorId(nodeId);
        System.out.println("[RMI] Recovered " + recovered.size() + " file metadata record(s).");
    }

    private static FileMetadata preferMetadata(FileMetadata a, FileMetadata b) {
        Set<String> locations = new LinkedHashSet<>(a.getLocations());
        locations.addAll(b.getLocations());
        FileMetadata preferred = a.getLocations().size() >= b.getLocations().size() ? a : b;
        return new FileMetadata(preferred.getId(), preferred.getName(), preferred.getSize(),
                preferred.getChecksum(), new ArrayList<>(locations));
    }

    private void mergeCatalog(Map<String, FileMetadata> target, List<FileMetadata> items) {
        for (FileMetadata item : items)
            target.merge(item.getName(), item, StorageNode::preferMetadata);
    }

    private List<FileMetadata> readLocalCatalog() throws IOException {
        List<FileMetadata> result = new ArrayList<>();
        try (var paths = Files.list(storageDirectory)) {
            for (Path p : paths.filter(x -> x.getFileName().toString().endsWith(".meta")).toList()) {
                Properties props = new Properties();
                try (InputStream in = Files.newInputStream(p)) { props.load(in); }
                String name = props.getProperty("name");
                String id = props.getProperty("id");
                String checksum = props.getProperty("checksum");
                long size = Long.parseLong(props.getProperty("size", "0"));
                String locationsText = props.getProperty("locations", nodeId);
                List<String> locations = Arrays.stream(locationsText.split(","))
                        .filter(s -> !s.isBlank()).distinct().toList();
                if (name != null && id != null && checksum != null)
                    result.add(new FileMetadata(id, name, size, checksum, locations));
            }
        }
        return result;
    }

    private void handleClient(Socket socket) {
        try (Socket client = socket;
             DataInputStream in = new DataInputStream(client.getInputStream());
             DataOutputStream out = new DataOutputStream(client.getOutputStream())) {

            String header = Protocol.readHeader(in);
            if (header.startsWith("COORDINATOR|")) {
                String id = header.substring("COORDINATOR|".length()).trim();
                bullyElection.setCoordinator(id);
                metadataServer.getService().setCoordinatorId(id);
                Protocol.writeHeader(out, "OK");
                return;
            }

            String[] parts = header.trim().split("\\s+");
            switch (parts[0].toUpperCase(Locale.ROOT)) {
                case "PUT" -> handlePut(parts, in, out);
                case "GET" -> handleGet(parts, out);
                case "PING" -> Protocol.writeHeader(out, "PONG " + nodeId);
                case "INFO" -> Protocol.writeHeader(out, "NODE=" + nodeId + "|PORT=" + port
                        + "|RMI=" + rmiPort + "|COORDINATOR="
                        + String.valueOf(bullyElection.getCoordinatorId()));
                case "LIST", "LS" -> Protocol.writeHeader(out, listFiles());
                case "NODES" -> Protocol.writeHeader(out, listNodes());
                case "RM" -> handleRemove(parts, out);
                case "ELECTION" -> Protocol.writeHeader(out, bullyElection.handleElectionRequest());
                case "CATALOG" -> sendCatalog(out);
                default -> Protocol.writeHeader(out, "ERROR unknown command");
            }
        } catch (Exception e) {
            System.err.println("[CLIENT] Connection error: " + e.getMessage());
        }
    }

    private void handlePut(String[] p, DataInputStream in, DataOutputStream out) throws IOException {
        if (p.length != 6) {
            Protocol.writeHeader(out, "ERROR usage: PUT filename size sha256 uuid node1,node2");
            return;
        }
        String filename = p[1];
        long size;
        try { size = Long.parseLong(p[2]); }
        catch (NumberFormatException e) { Protocol.writeHeader(out, "ERROR invalid size"); return; }
        String checksum = p[3], id = p[4];
        List<String> locations = Arrays.stream(p[5].split(",")).filter(s -> !s.isBlank()).distinct().toList();
        if (size < 0 || !checksum.matches("[a-fA-F0-9]{64}") || locations.size() != 2) {
            Protocol.writeHeader(out, "ERROR invalid metadata or expected exactly two locations");
            return;
        }

        Path destination;
        try { destination = resolveFile(filename); }
        catch (IllegalArgumentException e) { Protocol.writeHeader(out, "ERROR invalid filename"); return; }

        Path temp = destination.resolveSibling(destination.getFileName() + ".part");
        try {
            String actual = Protocol.receiveBytes(in, size, temp);
            if (!actual.equalsIgnoreCase(checksum)) {
                Files.deleteIfExists(temp);
                Protocol.writeHeader(out, "ERROR checksum mismatch");
                return;
            }
            moveFile(temp, destination);
            saveMetadata(new FileMetadata(id, filename, size, actual, locations));
            Protocol.writeHeader(out, "STORED " + id + " " + actual);
            System.out.println("[STORAGE] Stored " + filename + " on " + nodeId);
        } catch (IOException e) {
            Files.deleteIfExists(temp);
            throw e;
        }
    }

    private void handleGet(String[] p, DataOutputStream out) throws IOException {
        if (p.length != 2) { Protocol.writeHeader(out, "ERROR usage: GET filename"); return; }
        Path file;
        try { file = resolveFile(p[1]); }
        catch (IllegalArgumentException e) { Protocol.writeHeader(out, "ERROR invalid filename"); return; }
        if (!Files.isRegularFile(file)) { Protocol.writeHeader(out, "NOT_FOUND"); return; }
        Protocol.writeHeader(out, "FILE " + Files.size(file) + " " + Protocol.checksum(file));
        Protocol.sendBytes(file, out);
    }

    private void handleRemove(String[] p, DataOutputStream out) throws IOException {
        if (p.length != 2) { Protocol.writeHeader(out, "ERROR usage: RM filename"); return; }
        Path file = resolveFile(p[1]);
        boolean existed = Files.deleteIfExists(file);
        Files.deleteIfExists(metadataPath(p[1]));
        Protocol.writeHeader(out, existed ? "REMOVED " + p[1] : "NOT_FOUND");
    }

    private void sendCatalog(DataOutputStream out) throws IOException {
        List<FileMetadata> items = readLocalCatalog();
        Protocol.writeHeader(out, "CATALOG " + items.size());
        for (FileMetadata item : items) Protocol.writeHeader(out, encodeMetadata(item));
    }

    private static String encodeMetadata(FileMetadata m) {
        Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
        return enc.encodeToString(m.getId().getBytes(StandardCharsets.UTF_8)) + "|"
                + enc.encodeToString(m.getName().getBytes(StandardCharsets.UTF_8)) + "|"
                + m.getSize() + "|" + m.getChecksum() + "|"
                + enc.encodeToString(String.join(",", m.getLocations()).getBytes(StandardCharsets.UTF_8));
    }

    private static FileMetadata decodeMetadata(String value) throws IOException {
        try {
            String[] p = value.split("\\|", -1);
            Base64.Decoder dec = Base64.getUrlDecoder();
            return new FileMetadata(new String(dec.decode(p[0]), StandardCharsets.UTF_8),
                    new String(dec.decode(p[1]), StandardCharsets.UTF_8),
                    Long.parseLong(p[2]), p[3],
                    Arrays.asList(new String(dec.decode(p[4]), StandardCharsets.UTF_8).split(",")));
        } catch (Exception e) { throw new IOException("Invalid catalog record", e); }
    }

    private void saveMetadata(FileMetadata m) throws IOException {
        Properties props = new Properties();
        props.setProperty("id", m.getId());
        props.setProperty("name", m.getName());
        props.setProperty("size", Long.toString(m.getSize()));
        props.setProperty("checksum", m.getChecksum());
        props.setProperty("locations", String.join(",", m.getLocations()));
        Path target = metadataPath(m.getName());
        Path temp = target.resolveSibling(target.getFileName() + ".part");
        try (OutputStream out = Files.newOutputStream(temp)) { props.store(out, "Distributed storage metadata"); }
        moveFile(temp, target);
    }

    private Path metadataPath(String filename) { return storageDirectory.resolve(filename + ".meta"); }

    private String listFiles() throws IOException {
        try (var files = Files.list(storageDirectory)) {
            return "FILES " + files.filter(Files::isRegularFile)
                    .map(x -> x.getFileName().toString())
                    .filter(n -> !n.endsWith(".part") && !n.endsWith(".meta"))
                    .sorted().reduce((a,b) -> a + "," + b).orElse("");
        }
    }

    private String listNodes() {
        StringBuilder result = new StringBuilder("NODES coordinator=")
                .append(bullyElection.getCoordinatorId());
        heartbeatManager.getNodeStatuses().values().stream()
                .sorted(Comparator.comparing(HeartbeatManager.NodeStatusInfo::getId))
                .forEach(n -> result.append(" | ").append(n.getId()).append("@")
                        .append(n.getAddress()).append(":").append(n.getPort())
                        .append(" ").append(n.getStatus()));
        return result.toString();
    }

    private Path resolveFile(String filename) {
        if (filename == null || filename.isBlank() || filename.contains("/")
                || filename.contains("\\") || filename.equals(".") || filename.equals(".."))
            throw new IllegalArgumentException("Invalid filename");
        Path path = storageDirectory.resolve(filename).normalize();
        if (!path.startsWith(storageDirectory)) throw new IllegalArgumentException("Invalid path");
        return path;
    }

    private Socket connect(NodeDiscovery.NodeInfo peer) throws IOException {
        Socket socket = new Socket();
        socket.connect(new java.net.InetSocketAddress(peer.getAddress(), peer.getPort()), 1500);
        socket.setSoTimeout(2500);
        return socket;
    }

    private String request(NodeDiscovery.NodeInfo peer, String command) throws IOException {
        try (Socket socket = connect(peer);
             DataInputStream in = new DataInputStream(socket.getInputStream());
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
            Protocol.writeHeader(out, command);
            return Protocol.readHeader(in);
        }
    }

    private void moveFile(Path from, Path to) throws IOException {
        try { Files.move(from, to, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE); }
        catch (AtomicMoveNotSupportedException e) { Files.move(from, to, StandardCopyOption.REPLACE_EXISTING); }
    }

    private void shutdown() {
        running = false;
        monitor.shutdownNow();
        if (bullyElection != null) bullyElection.close();
        if (heartbeatManager != null) heartbeatManager.close();
        if (discovery != null) discovery.close();
        if (metadataServer != null) metadataServer.close();
        try { if (serverSocket != null) serverSocket.close(); } catch (IOException ignored) {}
        clientPool.shutdownNow();
        System.out.println("[SERVER] Node " + nodeId + " stopped.");
    }

    private static Map<String,String> parseArguments(String[] args) {
        Map<String,String> result = new HashMap<>();
        for (int i=0;i<args.length;i++) {
            String key=args[i];
            if (!key.startsWith("--") || i+1>=args.length) throw new IllegalArgumentException("Invalid argument: "+key);
            result.put(key,args[++i]);
        }
        return result;
    }

    public static void main(String[] args) {
        try {
            Map<String,String> c=parseArguments(args);
            String id=c.getOrDefault("--id","n1");
            int port=Integer.parseInt(c.getOrDefault("--port","7001"));
            String dir=c.getOrDefault("--dir","data/"+id);
            String group=c.getOrDefault("--group","239.255.0.1");
            StorageNode node=new StorageNode(id,port,dir,group);
            Runtime.getRuntime().addShutdownHook(new Thread(node::shutdown));
            node.start();
        } catch (Exception e) {
            System.err.println("[StorageNode] "+e.getMessage());
            System.err.println("Usage: --id n1 --port 7001 --dir data/n1 --group 239.255.0.1");
            System.exit(1);
        }
    }
}
