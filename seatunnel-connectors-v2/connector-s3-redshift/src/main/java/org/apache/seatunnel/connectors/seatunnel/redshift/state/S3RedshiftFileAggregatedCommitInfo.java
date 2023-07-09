package org.apache.seatunnel.connectors.seatunnel.redshift.state;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;

import java.util.LinkedHashMap;
import java.util.List;

public class S3RedshiftFileAggregatedCommitInfo extends FileAggregatedCommitInfo {

    private final SeaTunnelRowType rowType;

    public S3RedshiftFileAggregatedCommitInfo(
            LinkedHashMap<String, LinkedHashMap<String, String>> transactionMap,
            LinkedHashMap<String, List<String>> partitionDirAndValuesMap,
            SeaTunnelRowType seaTunnelRowType) {
        super(transactionMap, partitionDirAndValuesMap);
        this.rowType = seaTunnelRowType;
    }

    public SeaTunnelRowType getRowType() {
        return rowType;
    }
}
