package org.apache.seatunnel.connectors.dws.guassdb.sink.writer;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.dws.guassdb.sink.commit.DwsGaussDBSinkCommitInfo;
import org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption;
import org.apache.seatunnel.connectors.dws.guassdb.sink.sql.DwsGaussSqlGenerator;
import org.apache.seatunnel.connectors.dws.guassdb.sink.state.DwsGaussDBSinkState;

import java.sql.SQLException;
import java.util.List;

import static org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption.WRITE_MODE;

public class DwsGaussDBSinkWriterFactory {

    public static SinkWriter<SeaTunnelRow, DwsGaussDBSinkCommitInfo, DwsGaussDBSinkState>
            createDwsGaussDBSinkWriter(
                    DwsGaussSqlGenerator sqlGenerator,
                    CatalogTable catalogTable,
                    ReadonlyConfig readonlyConfig)
                    throws SQLException {

        DwsGaussDBSinkOption.WriteMode writeMode = readonlyConfig.get(WRITE_MODE);
        switch (writeMode) {
            case APPEND_ONLY:
                return new DwsGaussDBAppendOnlySinkWriter(
                        sqlGenerator, catalogTable, readonlyConfig);
            case USING_TEMPORARY_TABLE:
                return new DwsGaussDBUsingTemporaryTableSinkWriter(
                        sqlGenerator, catalogTable, readonlyConfig, false);
            default:
                throw new IllegalArgumentException("Unsupported write mode: " + writeMode);
        }
    }

    public static SinkWriter<SeaTunnelRow, DwsGaussDBSinkCommitInfo, DwsGaussDBSinkState>
            createDwsGaussDBRestoreWriter(
                    DwsGaussSqlGenerator sqlGenerator,
                    CatalogTable catalogTable,
                    ReadonlyConfig readonlyConfig,
                    SinkWriter.Context context,
                    List<DwsGaussDBSinkState> states)
                    throws SQLException {

        DwsGaussDBSinkOption.WriteMode writeMode = readonlyConfig.get(WRITE_MODE);
        switch (writeMode) {
            case APPEND_ONLY:
                return new DwsGaussDBAppendOnlySinkWriter(
                        sqlGenerator, catalogTable, readonlyConfig);
            case USING_TEMPORARY_TABLE:
                return new DwsGaussDBUsingTemporaryTableSinkWriter(
                        sqlGenerator, catalogTable, readonlyConfig, true);
            default:
                throw new IllegalArgumentException("Unsupported write mode: " + writeMode);
        }
    }
}
