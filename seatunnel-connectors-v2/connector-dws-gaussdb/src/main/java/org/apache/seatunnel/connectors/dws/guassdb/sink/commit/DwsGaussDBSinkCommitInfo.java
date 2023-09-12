package org.apache.seatunnel.connectors.dws.guassdb.sink.commit;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import lombok.Data;

import java.io.Serializable;

@Data
public class DwsGaussDBSinkCommitInfo implements Serializable {

    private final String temporaryTableName;
    private final String targetTableName;
    private final String currentSnapshotId;
    private final SeaTunnelRowType rowType;

    public DwsGaussDBSinkCommitInfo(
            String temporaryTableName,
            String targetTableName,
            String currentSnapshotId,
            SeaTunnelRowType rowType) {
        this.temporaryTableName = temporaryTableName;
        this.targetTableName = targetTableName;
        this.currentSnapshotId = currentSnapshotId;
        this.rowType = rowType;
    }
}
