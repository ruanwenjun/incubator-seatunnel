package org.apache.seatunnel.connectors.dws.guassdb.sink.writer;

import java.io.Serializable;
import java.util.UUID;

public class SnapshotIdManager implements Serializable {

    private String currentSnapshotId;

    public SnapshotIdManager() {
        // When initialize create a new snapshot id
        increaseSnapshotId();
    }

    public String getCurrentSnapshotId() {
        return currentSnapshotId;
    }

    public void increaseSnapshotId() {
        this.currentSnapshotId = UUID.randomUUID().toString();
    }
}
