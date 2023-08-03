package org.apache.seatunnel.connectors.seatunnel.redshift.state;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;

import lombok.Getter;

import java.util.LinkedHashMap;
import java.util.List;

@Getter
public class S3RedshiftFileCommitInfo extends FileCommitInfo {
    private final SeaTunnelRowType rowType;
    private final boolean appendOnly;

    public S3RedshiftFileCommitInfo(
            LinkedHashMap<String, String> needMoveFiles,
            LinkedHashMap<String, List<String>> partitionDirAndValuesMap,
            String transactionDir,
            SeaTunnelRowType rowType,
            boolean appendOnly) {
        super(needMoveFiles, partitionDirAndValuesMap, transactionDir);
        this.rowType = rowType;
        this.appendOnly = appendOnly;
    }
}
