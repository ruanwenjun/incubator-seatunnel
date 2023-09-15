package org.apache.seatunnel.connectors.dws.guassdb.sink.writer;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.dws.guassdb.catalog.DwsGaussDBCatalog;
import org.apache.seatunnel.connectors.dws.guassdb.catalog.DwsGaussDBCatalogFactory;
import org.apache.seatunnel.connectors.dws.guassdb.sink.commit.DwsGaussDBSinkCommitInfo;
import org.apache.seatunnel.connectors.dws.guassdb.sink.sql.DwsGaussSqlGenerator;
import org.apache.seatunnel.connectors.dws.guassdb.sink.state.DwsGaussDBSinkState;

import org.apache.commons.collections4.CollectionUtils;

import com.google.common.collect.Lists;
import com.huawei.gauss200.jdbc.copy.CopyManager;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption.PRIMARY_KEY;

@Slf4j
public class DwsGaussDBUsingTemporaryTableSinkWriter
        implements SinkWriter<SeaTunnelRow, DwsGaussDBSinkCommitInfo, DwsGaussDBSinkState> {

    private final DwsGaussDBMemoryTable dwsGaussDBMemoryTable;

    private final transient DwsGaussDBCatalog dwsGaussDBCatalog;
    private final DwsGaussSqlGenerator sqlGenerator;
    private final SeaTunnelRowType seaTunnelRowType;
    private final String primaryKey;

    private final SnapshotIdManager snapshotIdManager;

    private final transient CopyManager copyManager;

    public DwsGaussDBUsingTemporaryTableSinkWriter(
            DwsGaussSqlGenerator sqlGenerator,
            CatalogTable catalogTable,
            ReadonlyConfig readonlyConfig)
            throws SQLException {
        this.dwsGaussDBCatalog =
                new DwsGaussDBCatalogFactory()
                        .createCatalog(catalogTable.getCatalogName(), readonlyConfig);
        this.sqlGenerator = sqlGenerator;
        this.seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        this.primaryKey = readonlyConfig.get(PRIMARY_KEY);
        this.snapshotIdManager = new SnapshotIdManager();
        this.dwsGaussDBMemoryTable = new DwsGaussDBMemoryTable(seaTunnelRowType, primaryKey);
        this.copyManager = new CopyManager(dwsGaussDBCatalog.getDefaultConnection());
    }

    @Override
    public void write(SeaTunnelRow element) {
        dwsGaussDBMemoryTable.write(element);
    }

    @Override
    public Optional<DwsGaussDBSinkCommitInfo> prepareCommit() {
        try {
            // Write the data to temporary table
            // clear the data in memory table
            // increase the snapshotId
            Collection<SeaTunnelRow> deleteRows = dwsGaussDBMemoryTable.getDeleteRows();
            Collection<SeaTunnelRow> upsertRows = dwsGaussDBMemoryTable.getUpsertRows();
            writeDeleteRowsInTemplateTable(deleteRows);
            writeUpsertRowsInTemplateTable(upsertRows);
            dwsGaussDBMemoryTable.truncate();

            DwsGaussDBSinkCommitInfo dwsGaussDBSinkCommitInfo =
                    new DwsGaussDBSinkCommitInfo(
                            sqlGenerator.getTemporaryTableName(),
                            sqlGenerator.getTargetTableName(),
                            snapshotIdManager.getCurrentSnapshotId(),
                            seaTunnelRowType);
            snapshotIdManager.increaseSnapshotId();
            return Optional.of(dwsGaussDBSinkCommitInfo);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Failed to Write rows in temporaryTable: "
                            + sqlGenerator.getTemporaryTableName(),
                    ex);
        }
    }

    @Override
    public List<DwsGaussDBSinkState> snapshotState(long checkpointId) {
        return Lists.newArrayList(
                new DwsGaussDBSinkState(snapshotIdManager.getCurrentSnapshotId()));
    }

    @Override
    public void abortPrepare() {
        // clear the template table and memory table
        deleteRowsInTemplateTable(snapshotIdManager.getCurrentSnapshotId());
        dwsGaussDBMemoryTable.truncate();
        snapshotIdManager.increaseSnapshotId();
    }

    @Override
    public void close() {
        dwsGaussDBMemoryTable.truncate();
        try (DwsGaussDBCatalog closedCatalog = dwsGaussDBCatalog) {}
    }

    private void writeDeleteRowsInTemplateTable(Collection<SeaTunnelRow> deleteRows)
            throws SQLException, IOException {
        if (CollectionUtils.isEmpty(deleteRows)) {
            return;
        }
        String currentSnapshotId = snapshotIdManager.getCurrentSnapshotId();
        String temporaryRows = sqlGenerator.getTemporaryRows(deleteRows, true, currentSnapshotId);
        try (StringReader stringReader = new StringReader(temporaryRows)) {
            copyManager.copyIn(sqlGenerator.getCopyInTemporaryTableSql(), stringReader);
            log.debug(
                    "Success write delete rows of snapshot: {} in temporary table: {}",
                    currentSnapshotId,
                    sqlGenerator.getTemporaryTableName());
        }
    }

    private void writeUpsertRowsInTemplateTable(Collection<SeaTunnelRow> upsertRows)
            throws SQLException, IOException {
        if (CollectionUtils.isEmpty(upsertRows)) {
            return;
        }
        String currentSnapshotId = snapshotIdManager.getCurrentSnapshotId();
        String temporaryRows = sqlGenerator.getTemporaryRows(upsertRows, false, currentSnapshotId);
        try (StringReader stringReader = new StringReader(temporaryRows)) {
            copyManager.copyIn(sqlGenerator.getCopyInTemporaryTableSql(), stringReader);
            log.debug(
                    "Success write upsert rows of snapshot: {} in temporary table: {}",
                    currentSnapshotId,
                    sqlGenerator.getTemporaryTableName());
        }
    }

    private void deleteRowsInTemplateTable(String snapshotId) {
        try {
            dwsGaussDBCatalog.executeUpdateSql(
                    sqlGenerator.getDeleteTemporarySnapshotSql(snapshotId));
            log.debug(
                    "Success delete snapshot: {} in temporary table: {}",
                    snapshotId,
                    sqlGenerator.getTemporaryTableName());
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Failed to delete rows in temporaryTable: "
                            + sqlGenerator.getTemporaryTableName(),
                    ex);
        }
    }
}
