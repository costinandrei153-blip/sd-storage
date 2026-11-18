package ro.ubm.sd.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class FileMetadata implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String id;
    private final String name;
    private final long size;
    private final String checksum;
    private final List<String> locations;

    public FileMetadata(String id, String name, long size, String checksum, List<String> locations) {
        this.id = id == null || id.isBlank() ? UUID.randomUUID().toString() : id;
        this.name = name;
        this.size = size;
        this.checksum = checksum;
        this.locations = new ArrayList<>(locations == null ? List.of() : locations);
    }

    public String getId() { return id; }
    public String getName() { return name; }
    public long getSize() { return size; }
    public String getChecksum() { return checksum; }
    public List<String> getLocations() { return new ArrayList<>(locations); }
}
