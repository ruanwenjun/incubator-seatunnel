package org.apache.seatunnel.connectors.dws.guassdb.sink.writer;

import java.io.Serializable;

public class SnapshotIdManager implements Serializable {

    private Long currentSnapshotId;

    public SnapshotIdManager() {
        // When initialize create a new snapshot id
        increaseSnapshotId();
    }

    public Long getCurrentSnapshotId() {
        return currentSnapshotId;
    }

    public void increaseSnapshotId() {
        this.currentSnapshotId = System.currentTimeMillis();
    }
}
