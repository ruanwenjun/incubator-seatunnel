package org.apache.seatunnel.connectors.dws.guassdb.sink.commit;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
@AllArgsConstructor
public class DwsGaussDBSinkAggregatedCommitInfo implements Serializable {

    private final String temporaryTableName;
    private final String targetTableName;
    private final List<String> currentSnapshotIds;
    private final SeaTunnelRowType rowType;
}
