package org.apache.seatunnel.connectors.dolphindb.sink.writter;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.dolphindb.exception.DolphinDBConnectorException;
import org.apache.seatunnel.connectors.dolphindb.exception.DolphinDBErrorCode;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import org.apache.commons.lang3.StringUtils;

import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Optional;

@Slf4j
public class DolphinDBSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final MultithreadedTableWriter multithreadedTableWriter;

    public DolphinDBSinkWriter(ReadonlyConfig pluginConfig, SeaTunnelRowType seaTunnelRowType)
            throws Exception {
        this.multithreadedTableWriter =
                MultithreadedTableWriterFactory.createMultithreadedTableWriter(pluginConfig);
    }

    @Override
    public void write(SeaTunnelRow element) {
        // The field will be transformed by BasicEntityFactory.createScalar
        ErrorCodeInfo errorCodeInfo = multithreadedTableWriter.insert(element.getFields());
        if (errorCodeInfo.hasError()) {
            throw new DolphinDBConnectorException(
                    DolphinDBErrorCode.WRITE_DATA_ERROR, errorCodeInfo.toString());
        }
    }

    @SneakyThrows
    @Override
    public Optional<Void> prepareCommit() {
        multithreadedTableWriter.waitForThreadCompletion();
        MultithreadedTableWriter.Status status = multithreadedTableWriter.getStatus();
        if (StringUtils.isNotEmpty(status.errorInfo)) {
            log.error("MultithreadedTableWriter write data error: {}", status.errorInfo);
            throw new DolphinDBConnectorException(
                    DolphinDBErrorCode.WRITE_DATA_ERROR, status.errorInfo);
        }
        return super.prepareCommit();
    }

    @Override
    public void close() throws IOException {
        try {
            multithreadedTableWriter.waitForThreadCompletion();
            MultithreadedTableWriter.Status status = multithreadedTableWriter.getStatus();
            if (StringUtils.isNotEmpty(status.errorInfo)) {
                log.error("MultithreadedTableWriter completion error: {}", status.errorInfo);
            }
        } catch (Exception ex) {
            throw new IOException("Close MultithreadedTableWriter failed", ex);
        }
    }
}
