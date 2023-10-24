package org.apache.seatunnel.connectors.dws.guassdb.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.dws.guassdb.catalog.DwsGaussDBCatalog;
import org.apache.seatunnel.connectors.dws.guassdb.catalog.DwsGaussDBCatalogFactory;
import org.apache.seatunnel.connectors.dws.guassdb.config.DwsGaussDBConfig;
import org.apache.seatunnel.connectors.dws.guassdb.sink.commit.DwsGaussDBSinkAggregatedCommitInfo;
import org.apache.seatunnel.connectors.dws.guassdb.sink.commit.DwsGaussDBSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.dws.guassdb.sink.commit.DwsGaussDBSinkCommitInfo;
import org.apache.seatunnel.connectors.dws.guassdb.sink.savemode.DwsGaussDBSaveModeHandler;
import org.apache.seatunnel.connectors.dws.guassdb.sink.sql.DwsGaussSqlGenerator;
import org.apache.seatunnel.connectors.dws.guassdb.sink.state.DwsGaussDBSinkState;
import org.apache.seatunnel.connectors.dws.guassdb.sink.writer.DwsGaussDBSinkWriterFactory;

import org.apache.commons.collections4.CollectionUtils;

import lombok.Getter;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption.FIELD_IDE;
import static org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption.PRIMARY_KEY;

public class DwsGaussDBSink
        implements SeaTunnelSink<
                        SeaTunnelRow,
                        DwsGaussDBSinkState,
                        DwsGaussDBSinkCommitInfo,
                        DwsGaussDBSinkAggregatedCommitInfo>,
                SupportMultiTableSink,
                SupportSaveMode {

    @Getter private final String pluginName = DwsGaussDBConfig.CONNECTOR_NAME;

    private SeaTunnelRowType seaTunnelRowType;
    private final ReadonlyConfig readonlyConfig;
    private final CatalogTable catalogTable;

    private final DwsGaussSqlGenerator sqlGenerator;

    public DwsGaussDBSink(ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        this.readonlyConfig = readonlyConfig;
        this.catalogTable = catalogTable;
        this.sqlGenerator =
                new DwsGaussSqlGenerator(
                        readonlyConfig.get(PRIMARY_KEY),
                        readonlyConfig.get(FIELD_IDE),
                        catalogTable);
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {}

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return seaTunnelRowType;
    }

    @Override
    public SinkWriter<SeaTunnelRow, DwsGaussDBSinkCommitInfo, DwsGaussDBSinkState> createWriter(
            SinkWriter.Context context) {
        try {
            return DwsGaussDBSinkWriterFactory.createDwsGaussDBSinkWriter(
                    sqlGenerator, catalogTable, readonlyConfig);
        } catch (Exception ex) {
            throw new RuntimeException("Create SinkWriter failed", ex);
        }
    }

    @Override
    public SinkWriter<SeaTunnelRow, DwsGaussDBSinkCommitInfo, DwsGaussDBSinkState> restoreWriter(
            SinkWriter.Context context, List<DwsGaussDBSinkState> states) {
        try {
            if (CollectionUtils.isNotEmpty(states)) {
                try (DwsGaussDBCatalog dwsGaussDBCatalog =
                        new DwsGaussDBCatalogFactory()
                                .createCatalog(catalogTable.getCatalogName(), readonlyConfig)) {

                    List<Long> snapshotIds =
                            states.stream()
                                    .flatMap(state -> state.getSnapshotId().stream())
                                    .collect(Collectors.toList());

                    if (CollectionUtils.isNotEmpty(snapshotIds)) {
                        String deleteTemporarySnapshotSql =
                                sqlGenerator.getDeleteTemporarySnapshotSql(snapshotIds);
                        dwsGaussDBCatalog.executeUpdateSql(deleteTemporarySnapshotSql);
                    }
                }
            }
            return DwsGaussDBSinkWriterFactory.createDwsGaussDBRestoreWriter(
                    sqlGenerator, catalogTable, readonlyConfig, context, states);
        } catch (SQLException e) {
            throw new RuntimeException("Create SinkWriter failed", e);
        }
    }

    @Override
    public Optional<Serializer<DwsGaussDBSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<Serializer<DwsGaussDBSinkCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<
                    SinkAggregatedCommitter<
                            DwsGaussDBSinkCommitInfo, DwsGaussDBSinkAggregatedCommitInfo>>
            createAggregatedCommitter() {
        return Optional.of(
                new DwsGaussDBSinkAggregatedCommitter(sqlGenerator, catalogTable, readonlyConfig));
    }

    @Override
    public Optional<Serializer<DwsGaussDBSinkAggregatedCommitInfo>>
            getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public SaveModeHandler getSaveModeHandler() {
        return new DwsGaussDBSaveModeHandler(readonlyConfig, catalogTable, sqlGenerator);
    }
}
