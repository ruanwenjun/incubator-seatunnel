package org.apache.seatunnel.connectors.dolphindb.sink.writter;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Optional;

@Slf4j
public class DolphinDBSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final ReadonlyConfig pluginConfig;

    private DolphinDBUpsertWriter dolphinDBUpsertWriter;
    private DolphinDbDeleteWriter dolphinDbDeleteWriter;

    public DolphinDBSinkWriter(ReadonlyConfig pluginConfig, SeaTunnelRowType seaTunnelRowType)
            throws Exception {
        this.pluginConfig = pluginConfig;
        this.dolphinDBUpsertWriter = new DolphinDBUpsertWriter(pluginConfig, seaTunnelRowType);
        this.dolphinDbDeleteWriter = new DolphinDbDeleteWriter(pluginConfig, seaTunnelRowType);
    }

    @Override
    public void write(SeaTunnelRow element) {
        RowKind rowKind = element.getRowKind();
        if (rowKind == RowKind.DELETE) {
            // delete the data
            dolphinDbDeleteWriter.write(element);
        } else {
            dolphinDBUpsertWriter.write(element);
        }
    }

    @SneakyThrows
    @Override
    public Optional<Void> prepareCommit() {
        dolphinDBUpsertWriter.prepareCommit();
        dolphinDbDeleteWriter.prepareCommit();
        return super.prepareCommit();
    }

    @Override
    public void close() throws IOException {
        try (DolphinDBUpsertWriter dolphinDBUpsertWriter1 = dolphinDBUpsertWriter;
                DolphinDbDeleteWriter dolphinDbDeleteWriter1 = dolphinDbDeleteWriter) {

        } catch (Exception ex) {
            throw new IOException("Failed to close DolphinDBSinkWriter", ex);
        }
    }
}
