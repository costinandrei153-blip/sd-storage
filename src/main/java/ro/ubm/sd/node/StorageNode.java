package ro.ubm.sd.node;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class StorageNode {

    public static void main(String[] args) throws Exception {
        String id = null;
        int port = -1;
        String dir = null;
        boolean coordinator = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--id" -> id = args[++i];
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--dir" -> dir = args[++i];
                case "--coordinator" -> coordinator = true;
                default -> {
                    System.err.println("Argument necunoscut: " + args[i]);
                    System.exit(1);
                }
            }
        }

        if (id == null || port < 0 || dir == null) {
            System.err.println("Utilizare: StorageNode --id <id> --port <port> --dir <director> [--coordinator]");
            System.exit(1);
        }

        Path path = Paths.get(dir);
        Files.createDirectories(path);

        String rol = coordinator ? "coordonator" : "nod de stocare";
        System.out.println("[" + id + "] pornit: port " + port + ", director " + dir + ", rol: " + rol);
    }
}