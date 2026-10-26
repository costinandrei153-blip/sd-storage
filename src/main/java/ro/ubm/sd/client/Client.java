package ro.ubm.sd.client;

public class Client {

    public static void main(String[] args) {
        String coordinator = null;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--coordinator")) {
                coordinator = args[++i];
            } else {
                System.err.println("Argument necunoscut: " + args[i]);
                System.exit(1);
            }
        }

        if (coordinator == null) {
            System.err.println("Utilizare: Client --coordinator <host:port>");
            System.exit(1);
        }

        System.out.println("[client] coordonator configurat: " + coordinator
                + "; comenzi: put, get, ls, rm, nodes, exit");
    }
}