package ro.ubm.sd.common;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MetadataServiceImpl extends UnicastRemoteObject implements MetadataService {
    private static final long serialVersionUID = 1L;
    private final ConcurrentMap<String, FileMetadata> files = new ConcurrentHashMap<>();
    private volatile String coordinatorId;

    public MetadataServiceImpl(String coordinatorId, int exportPort) throws RemoteException {
        super(exportPort);
        this.coordinatorId = coordinatorId;
    }

    @Override public void commit(FileMetadata metadata) throws RemoteException {
        if (metadata == null || metadata.getName() == null || metadata.getName().isBlank())
            throw new RemoteException("Invalid metadata");
        files.put(metadata.getName(), metadata);
    }

    @Override public FileMetadata findByName(String name) throws RemoteException {
        return files.get(name);
    }

    @Override public List<FileMetadata> listFiles() throws RemoteException {
        List<FileMetadata> result = new ArrayList<>(files.values());
        result.sort(Comparator.comparing(FileMetadata::getName));
        return result;
    }

    @Override public boolean removeByName(String name) throws RemoteException {
        return files.remove(name) != null;
    }

    @Override public void replaceAll(List<FileMetadata> metadata) throws RemoteException {
        files.clear();
        if (metadata != null) {
            for (FileMetadata item : metadata) {
                if (item != null && item.getName() != null)
                    files.merge(item.getName(), item, (a, b) ->
                            a.getLocations().size() >= b.getLocations().size() ? a : b);
            }
        }
    }

    @Override public String getCoordinatorId() { return coordinatorId; }
    @Override public void setCoordinatorId(String coordinatorId) { this.coordinatorId = coordinatorId; }
}
