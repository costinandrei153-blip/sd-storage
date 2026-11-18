package ro.ubm.sd.common;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface MetadataService extends Remote {
    void commit(FileMetadata metadata) throws RemoteException;
    FileMetadata findByName(String name) throws RemoteException;
    List<FileMetadata> listFiles() throws RemoteException;
    boolean removeByName(String name) throws RemoteException;
    void replaceAll(List<FileMetadata> metadata) throws RemoteException;
    String getCoordinatorId() throws RemoteException;
    void setCoordinatorId(String coordinatorId) throws RemoteException;
}
