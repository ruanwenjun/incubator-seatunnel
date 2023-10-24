package org.apache.seatunnel.connectors.dws.guassdb.sink.writer;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.dws.guassdb.catalog.DwsGaussDBCatalog;
import org.apache.seatunnel.connectors.dws.guassdb.catalog.DwsGaussDBCatalogFactory;
import org.apache.seatunnel.connectors.dws.guassdb.sink.commit.DwsGaussDBSinkCommitInfo;
import org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption;
import org.apache.seatunnel.connectors.dws.guassdb.sink.sql.DwsGaussSqlGenerator;
import org.apache.seatunnel.connectors.dws.guassdb.sink.state.DwsGaussDBSinkState;

import org.apache.commons.collections4.CollectionUtils;

import com.google.common.collect.Lists;
import com.huawei.gauss200.jdbc.copy.CopyManager;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption.PRIMARY_KEY;

@Slf4j
public class DwsGaussDBUsingTemporaryTableSinkWriter
        implements SinkWriter<SeaTunnelRow, DwsGaussDBSinkCommitInfo, DwsGaussDBSinkState>,
                SupportMultiTableSinkWriter {

    private final DwsGaussDBMemoryTable dwsGaussDBMemoryTable;

    private final transient DwsGaussDBCatalog dwsGaussDBCatalog;
    private final DwsGaussSqlGenerator sqlGenerator;
    private final SeaTunnelRowType seaTunnelRowType;
    private final String primaryKey;
    private final int batchSize;
    private volatile boolean directlyCopyToTargetTable;

    private final List<Long> snapshotIds = Collections.synchronizedList(new ArrayList<>());

    private final SnapshotIdManager snapshotIdManager;

    private final transient CopyManager copyManager;

    public DwsGaussDBUsingTemporaryTableSinkWriter(
            DwsGaussSqlGenerator sqlGenerator,
            CatalogTable catalogTable,
            ReadonlyConfig readonlyConfig,
            boolean isRestore)
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
        this.batchSize = readonlyConfig.get(DwsGaussDBSinkOption.BATCH_SIZE);
        this.directlyCopyToTargetTable = !isRestore;
    }

    @Override
    public void write(SeaTunnelRow element) {
        dwsGaussDBMemoryTable.write(element);
        // If the row kind is not INSERT, means this is in cdc incremental mode
        // We need to write the data to temporary table and merge to target table
        if (element.getRowKind() != RowKind.INSERT) {
            directlyCopyToTargetTable = false;
        }
        if (dwsGaussDBMemoryTable.size() > batchSize) {
            // If the directlyCopyToTargetTable is true, means we can directly copy the data to
            // target table
            if (directlyCopyToTargetTable) {
                flushMemoryTableToTargetTable();
            } else {
                flushMemoryTableToTemporaryTable();
            }
        }
    }

    @Override
    public Optional<DwsGaussDBSinkCommitInfo> prepareCommit() {
        try {
            // Write the data to temporary table
            // clear the data in memory table
            flushMemoryTableToTemporaryTable();

            // increase the snapshotId
            DwsGaussDBSinkCommitInfo dwsGaussDBSinkCommitInfo =
                    new DwsGaussDBSinkCommitInfo(
                            sqlGenerator.getTemporaryTableName(),
                            sqlGenerator.getTargetTableName(),
                            snapshotIds,
                            seaTunnelRowType);

            snapshotIds.clear();
            return Optional.of(dwsGaussDBSinkCommitInfo);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Failed to Write rows in temporaryTable: "
                            + sqlGenerator.getTemporaryTableName(),
                    ex);
        }
    }

    private void flushMemoryTableToTemporaryTable() {
        // Write the data to temporary table
        // clear the data in memory table
        try {
            Collection<SeaTunnelRow> deleteRows = dwsGaussDBMemoryTable.getDeleteRows();
            Collection<SeaTunnelRow> upsertRows = dwsGaussDBMemoryTable.getUpsertRows();
            writeDeleteRowsInTemplateTable(deleteRows);
            writeUpsertRowsInTemplateTable(upsertRows);
            dwsGaussDBMemoryTable.truncate();

            snapshotIds.add(snapshotIdManager.getCurrentSnapshotId());
            snapshotIdManager.increaseSnapshotId();
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Failed to Write rows in temporaryTable: "
                            + sqlGenerator.getTemporaryTableName(),
                    ex);
        }
    }

    private void flushMemoryTableToTargetTable() {
        try {
            Collection<SeaTunnelRow> upsertRows = dwsGaussDBMemoryTable.getUpsertRows();
            copyInsertRowsInTargetTable(upsertRows);
            dwsGaussDBMemoryTable.truncate();
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Failed to Write rows in targetTable: " + sqlGenerator.getTargetTableName(),
                    ex);
        }
    }

    @Override
    public List<DwsGaussDBSinkState> snapshotState(long checkpointId) {
        return Lists.newArrayList(new DwsGaussDBSinkState(snapshotIds));
    }

    @Override
    public void abortPrepare() {
        // clear the template table and memory table
        deleteRowsInTemplateTable(snapshotIds);
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
        Long currentSnapshotId = snapshotIdManager.getCurrentSnapshotId();
        String temporaryRows = sqlGenerator.getTemporaryRows(deleteRows, true, currentSnapshotId);
        try (StringReader stringReader = new StringReader(temporaryRows)) {
            copyManager.copyIn(sqlGenerator.getCopyInTemporaryTableSql(), stringReader);
            log.debug(
                    "Success write delete rows of snapshot: {} in temporary table: {}",
                    currentSnapshotId,
                    sqlGenerator.getTemporaryTableName());
        }
    }

    private void copyInsertRowsInTargetTable(Collection<SeaTunnelRow> insertRows)
            throws SQLException, IOException {
        if (CollectionUtils.isEmpty(insertRows)) {
            return;
        }
        String temporaryRows = sqlGenerator.getTargetTableRows(insertRows);
        try (StringReader stringReader = new StringReader(temporaryRows)) {
            copyManager.copyIn(sqlGenerator.getCopyInTargetTableSql(), stringReader);
            log.debug(
                    "Success write insert rows has been copy to target table{}",
                    sqlGenerator.getTargetTableName());
        }
    }

    private void writeUpsertRowsInTemplateTable(Collection<SeaTunnelRow> upsertRows)
            throws SQLException, IOException {
        if (CollectionUtils.isEmpty(upsertRows)) {
            return;
        }
        Long currentSnapshotId = snapshotIdManager.getCurrentSnapshotId();
        String temporaryRows = sqlGenerator.getTemporaryRows(upsertRows, false, currentSnapshotId);
        try (StringReader stringReader = new StringReader(temporaryRows)) {
            copyManager.copyIn(sqlGenerator.getCopyInTemporaryTableSql(), stringReader);
            log.debug(
                    "Success write upsert rows of snapshot: {} in temporary table: {}",
                    currentSnapshotId,
                    sqlGenerator.getTemporaryTableName());
        }
    }

    private void deleteRowsInTemplateTable(List<Long> snapshotId) {
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
