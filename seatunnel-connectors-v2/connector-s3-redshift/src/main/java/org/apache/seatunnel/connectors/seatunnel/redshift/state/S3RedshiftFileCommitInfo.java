package org.apache.seatunnel.connectors.seatunnel.redshift.state;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;

import java.util.LinkedHashMap;
import java.util.List;

public class S3RedshiftFileCommitInfo extends FileCommitInfo {

    private final SeaTunnelRowType rowType;

    public S3RedshiftFileCommitInfo(
            LinkedHashMap<String, String> needMoveFiles,
            LinkedHashMap<String, List<String>> partitionDirAndValuesMap,
            String transactionDir,
            SeaTunnelRowType rowType) {
        super(needMoveFiles, partitionDirAndValuesMap, transactionDir);
        this.rowType = rowType;
    }

    public SeaTunnelRowType getRowType() {
        return rowType;
    }
}
