package ro.ubm.sd.common;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public final class MetadataServer implements AutoCloseable {
    public static final String SERVICE_NAME = "MetadataService";
    private final int port;
    private final MetadataServiceImpl service;
    private Registry registry;

    public MetadataServer(int port, String nodeId) throws RemoteException {
        this.port = port;
        this.service = new MetadataServiceImpl(nodeId, port + 10000);
    }

    public void start() throws RemoteException {
        try {
            registry = LocateRegistry.createRegistry(port);
        } catch (RemoteException alreadyRunning) {
            registry = LocateRegistry.getRegistry(port);
            registry.list();
        }
        registry.rebind(SERVICE_NAME, service);
        System.out.println("[RMI] Metadata service ready on port " + port);
    }

    public MetadataService getService() { return service; }

    @Override public void close() {
        try { if (registry != null) registry.unbind(SERVICE_NAME); }
        catch (Exception ignored) {}
        try { UnicastRemoteObject.unexportObject(service, true); }
        catch (Exception ignored) {}
        try { if (registry != null) UnicastRemoteObject.unexportObject(registry, true); }
        catch (Exception ignored) {}
    }
}
