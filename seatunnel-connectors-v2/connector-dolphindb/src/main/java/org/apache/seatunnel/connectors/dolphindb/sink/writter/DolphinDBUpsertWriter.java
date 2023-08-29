package org.apache.seatunnel.connectors.dolphindb.sink.writter;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.dolphindb.exception.DolphinDBConnectorException;
import org.apache.seatunnel.connectors.dolphindb.exception.DolphinDBErrorCode;

import org.apache.commons.lang3.StringUtils;

import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Optional;

@Slf4j
public class DolphinDBUpsertWriter implements DolphinDBWriter {

    private ReadonlyConfig pluginConfig;
    private SeaTunnelRowType seaTunnelRowType;
    private final MultithreadedTableWriter multithreadedTableWriter;

    public DolphinDBUpsertWriter(ReadonlyConfig pluginConfig, SeaTunnelRowType seaTunnelRowType)
            throws Exception {
        this.pluginConfig = pluginConfig;
        this.seaTunnelRowType = seaTunnelRowType;
        this.multithreadedTableWriter =
                MultithreadedTableWriterFactory.createMultithreadedTableWriter(pluginConfig);
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow) {
        // The field will be transformed by BasicEntityFactory.createScalar
        Object[] fields = seaTunnelRow.getFields();
        Object[] finalFields = new Object[fields.length];
        for (int i = 0; i < finalFields.length; i++) {
            SeaTunnelDataType<?> fieldType = seaTunnelRowType.getFieldType(i);
            if (fieldType.getSqlType().equals(SqlType.DECIMAL)) {
                // dolphinDB support decimal after 2.00.8
                BigDecimal bigDecimal = (BigDecimal) fields[i];
                finalFields[i] = bigDecimal.doubleValue();
                continue;
            }
            if (fieldType.getSqlType().equals(SqlType.DATE)
                    || fieldType.getSqlType().equals(SqlType.TIME)
                    || fieldType.getSqlType().equals(SqlType.TIMESTAMP)) {
                // todo: how to deal with date/time/timestamp
                finalFields[i] = fields[i].toString();
                continue;
            }
            finalFields[i] = fields[i];
        }
        ErrorCodeInfo errorCodeInfo = multithreadedTableWriter.insert(finalFields);
        if (errorCodeInfo.hasError()) {
            throw new DolphinDBConnectorException(
                    DolphinDBErrorCode.WRITE_DATA_ERROR, errorCodeInfo.toString());
        }
    }

    @Override
    public Optional<Void> prepareCommit() throws Exception {
        multithreadedTableWriter.waitForThreadCompletion();
        MultithreadedTableWriter.Status status = multithreadedTableWriter.getStatus();
        if (StringUtils.isNotEmpty(status.errorInfo)) {
            log.error("MultithreadedTableWriter write data error: {}", status.errorInfo);
            throw new DolphinDBConnectorException(
                    DolphinDBErrorCode.WRITE_DATA_ERROR, status.errorInfo);
        }
        return Optional.empty();
    }

    @Override
    public void close() throws Exception {
        multithreadedTableWriter.waitForThreadCompletion();
        MultithreadedTableWriter.Status status = multithreadedTableWriter.getStatus();
        if (StringUtils.isNotEmpty(status.errorInfo)) {
            log.error("MultithreadedTableWriter completion error: {}", status.errorInfo);
            throw new IOException("Close MultithreadedTableWriter failed" + status.errorInfo);
        }
    }
}
