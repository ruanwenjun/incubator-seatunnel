package org.apache.seatunnel.connectors.dws.guassdb.sink.writer;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.dws.guassdb.catalog.DwsGaussDBCatalog;
import org.apache.seatunnel.connectors.dws.guassdb.catalog.DwsGaussDBCatalogFactory;
import org.apache.seatunnel.connectors.dws.guassdb.sink.commit.DwsGaussDBSinkCommitInfo;
import org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption;
import org.apache.seatunnel.connectors.dws.guassdb.sink.sql.DwsGaussSqlGenerator;
import org.apache.seatunnel.connectors.dws.guassdb.sink.state.DwsGaussDBSinkState;

import com.huawei.gauss200.jdbc.copy.CopyManager;
import com.huawei.gauss200.jdbc.core.BaseConnection;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * The writer for {@link DwsGaussDBSinkOption.WriteMode#APPEND_ONLY}, this writer will append the
 * data to the table, it will not guarantee the exactly-once.
 */
public class DwsGaussDBAppendOnlySinkWriter
        implements SinkWriter<SeaTunnelRow, DwsGaussDBSinkCommitInfo, DwsGaussDBSinkState>,
                SupportMultiTableSinkWriter {

    private final String tableName;
    private final transient DwsGaussDBCatalog dwsGaussDBCatalog;
    private final DwsGaussSqlGenerator sqlGenerator;
    private final transient CopyManager copyManager;
    private final transient BaseConnection connection;

    private final int batchSize;
    private final BlockingQueue<SeaTunnelRow> appendRows = new LinkedBlockingQueue<>();

    public DwsGaussDBAppendOnlySinkWriter(
            DwsGaussSqlGenerator sqlGenerator,
            CatalogTable catalogTable,
            ReadonlyConfig readonlyConfig)
            throws SQLException {
        this.dwsGaussDBCatalog =
                new DwsGaussDBCatalogFactory()
                        .createCatalog(catalogTable.getCatalogName(), readonlyConfig);
        this.tableName = catalogTable.getTableId().getTableName();
        this.sqlGenerator = sqlGenerator;
        this.connection = dwsGaussDBCatalog.getDefaultConnection();
        this.copyManager = createCopyManager();
        this.batchSize = readonlyConfig.get(DwsGaussDBSinkOption.BATCH_SIZE);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        // todo: Do we need to handle the delete/update element in append_only mode?
        appendRows.offer(element);
        if (appendRows.size() > batchSize) {
            try {
                flush();
            } catch (Exception ex) {
                throw new IOException("Write data to Dws-GaussDB failed.", ex);
            }
        }
    }

    @Override
    public Optional<DwsGaussDBSinkCommitInfo> prepareCommit() {
        try {
            flush();
        } catch (SQLException | IOException e) {
            throw new RuntimeException("Write data to Dws-GaussDB failed", e);
        }
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() {
        try (DwsGaussDBCatalog closedCatalog = dwsGaussDBCatalog) {}
    }

    private CopyManager createCopyManager() throws SQLException {
        return new CopyManager(connection);
    }

    private void flush() throws SQLException, IOException {
        if (appendRows.isEmpty()) {
            return;
        }
        String targetTableRows = sqlGenerator.getTargetTableRows(appendRows);
        try (StringReader stringReader = new StringReader(targetTableRows)) {
            copyManager.copyIn(sqlGenerator.getCopyInTargetTableSql(), stringReader);
            appendRows.clear();
        }
    }
}
