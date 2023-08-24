package org.apache.seatunnel.connectors.seatunnel.redshift.state;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;

import lombok.Getter;

import java.util.LinkedHashMap;
import java.util.List;

@Getter
public class S3RedshiftFileAggregatedCommitInfo extends FileAggregatedCommitInfo {

    private final SeaTunnelRowType rowType;
    private final boolean appendOnly;

    public S3RedshiftFileAggregatedCommitInfo(
            LinkedHashMap<String, LinkedHashMap<String, String>> transactionMap,
            LinkedHashMap<String, List<String>> partitionDirAndValuesMap,
            SeaTunnelRowType seaTunnelRowType,
            boolean appendOnly) {
        super(transactionMap, partitionDirAndValuesMap);
        this.rowType = seaTunnelRowType;
        this.appendOnly = appendOnly;
    }
}
