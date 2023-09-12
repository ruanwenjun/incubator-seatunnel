package org.apache.seatunnel.connectors.dws.guassdb.sink.commit;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.dws.guassdb.catalog.DwsGaussDBCatalog;
import org.apache.seatunnel.connectors.dws.guassdb.catalog.DwsGaussDBCatalogFactory;
import org.apache.seatunnel.connectors.dws.guassdb.sink.sql.DwsGaussSqlGenerator;

import org.apache.commons.collections4.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class DwsGaussDBSinkAggregatedCommitter
        implements SinkAggregatedCommitter<
                DwsGaussDBSinkCommitInfo, DwsGaussDBSinkAggregatedCommitInfo> {

    private final DwsGaussSqlGenerator dwsGaussSqlGenerator;
    private final DwsGaussDBCatalog dwsGaussDBCatalog;

    public DwsGaussDBSinkAggregatedCommitter(
            DwsGaussSqlGenerator dwsGaussSqlGenerator,
            CatalogTable catalogTable,
            ReadonlyConfig readonlyConfig) {
        this.dwsGaussSqlGenerator = dwsGaussSqlGenerator;
        this.dwsGaussDBCatalog =
                new DwsGaussDBCatalogFactory()
                        .createCatalog(catalogTable.getCatalogName(), readonlyConfig);
    }

    @Override
    public List<DwsGaussDBSinkAggregatedCommitInfo> commit(
            List<DwsGaussDBSinkAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
        if (CollectionUtils.isEmpty(aggregatedCommitInfo)) {
            return Collections.emptyList();
        }

        List<DwsGaussDBSinkAggregatedCommitInfo> needRetryCommitInfo = new ArrayList<>();
        for (DwsGaussDBSinkAggregatedCommitInfo commitInfo : aggregatedCommitInfo) {
            try {
                for (String snapshotId : commitInfo.getCurrentSnapshotIds()) {
                    dealWithDeleteRowsInTemporaryTable(snapshotId);
                    // using thread-pool
                    mergeTemporaryIntoTargetTable(snapshotId);
                    clearTemporaryTable(snapshotId);
                }
            } catch (Exception ex) {
                needRetryCommitInfo.add(commitInfo);
            }
        }
        return needRetryCommitInfo;
    }

    @Override
    public DwsGaussDBSinkAggregatedCommitInfo combine(List<DwsGaussDBSinkCommitInfo> commitInfos) {
        if (CollectionUtils.isEmpty(commitInfos)) {
            return null;
        }
        DwsGaussDBSinkCommitInfo dwsGaussDBSinkCommitInfo = commitInfos.get(0);
        List<String> snapshotIds =
                commitInfos.stream()
                        .map(DwsGaussDBSinkCommitInfo::getCurrentSnapshotId)
                        .collect(Collectors.toList());
        return new DwsGaussDBSinkAggregatedCommitInfo(
                dwsGaussDBSinkCommitInfo.getTargetTableName(),
                dwsGaussDBSinkCommitInfo.getTemporaryTableName(),
                snapshotIds,
                dwsGaussDBSinkCommitInfo.getRowType());
    }

    @Override
    public void abort(List<DwsGaussDBSinkAggregatedCommitInfo> aggregatedCommitInfo) {
        // clear the temporary table
        if (CollectionUtils.isEmpty(aggregatedCommitInfo)) {
            return;
        }
        for (DwsGaussDBSinkAggregatedCommitInfo dwsGaussDBSinkAggregatedCommitInfo :
                aggregatedCommitInfo) {
            // todo: use batch clear
            for (String snapshotId : dwsGaussDBSinkAggregatedCommitInfo.getCurrentSnapshotIds()) {
                clearTemporaryTable(snapshotId);
            }
        }
    }

    @Override
    public void close() {
        try (DwsGaussDBCatalog dwsGaussDBCatalog1 = dwsGaussDBCatalog) {}
    }

    private void dealWithDeleteRowsInTemporaryTable(String currentSnapshotId) {
        String deleteRowsInTargetTableSql =
                dwsGaussSqlGenerator.getDeleteRowsInTargetTableSql(currentSnapshotId);
        dwsGaussDBCatalog.executeUpdateSql(deleteRowsInTargetTableSql);

        String deleteRowsInTemporaryTableSql =
                dwsGaussSqlGenerator.getDeleteRowsInTemporaryTableSql(currentSnapshotId);
        dwsGaussDBCatalog.executeUpdateSql(deleteRowsInTemporaryTableSql);
    }

    private void mergeTemporaryIntoTargetTable(String currentSnapshotId) {
        String mergeInTargetTableSql =
                dwsGaussSqlGenerator.getMergeInTargetTableSql(currentSnapshotId);
        dwsGaussDBCatalog.executeUpdateSql(mergeInTargetTableSql);
    }

    private void clearTemporaryTable(String currentSnapshotIds) {
        String deleteTemporarySnapshotSql =
                dwsGaussSqlGenerator.getDeleteTemporarySnapshotSql(currentSnapshotIds);
        dwsGaussDBCatalog.executeUpdateSql(deleteTemporarySnapshotSql);
    }
}
