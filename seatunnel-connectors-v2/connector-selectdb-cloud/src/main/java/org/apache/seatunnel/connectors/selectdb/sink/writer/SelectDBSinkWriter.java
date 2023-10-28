/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.selectdb.sink.writer;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig;
import org.apache.seatunnel.connectors.selectdb.serialize.SeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.selectdb.serialize.SelectDBSerializer;
import org.apache.seatunnel.connectors.selectdb.sink.committer.SelectDBCommitInfo;
import org.apache.seatunnel.connectors.selectdb.util.UnsupportedTypeConverterUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

@Slf4j
public class SelectDBSinkWriter
        implements SinkWriter<SeaTunnelRow, SelectDBCommitInfo, SelectDBSinkState>,
                SupportMultiTableSinkWriter {
    private final SelectDBConfig selectdbConfig;
    private final long lastCheckpointId;
    private SelectDBStageLoad selectDBStageLoad;
    private CatalogTable catalogTable;
    volatile boolean loading;
    private final String labelPrefix;
    private final byte[] lineDelimiter;
    private final LabelGenerator labelGenerator;
    private final SelectDBSinkState selectdbSinkState;
    private final SelectDBSerializer serializer;

    public SelectDBSinkWriter(
            SinkWriter.Context context,
            List<SelectDBSinkState> state,
            CatalogTable catalogTable,
            SelectDBConfig selectdbConfig,
            String jobId) {
        this.selectdbConfig = selectdbConfig;
        this.catalogTable = catalogTable;
        this.lastCheckpointId = state.size() != 0 ? state.get(0).getCheckpointId() : 0;
        log.info("restore checkpointId {}", lastCheckpointId);
        // filename prefix is uuid
        log.info("labelPrefix " + selectdbConfig.getLabelPrefix());
        this.selectdbSinkState =
                new SelectDBSinkState(selectdbConfig.getLabelPrefix(), lastCheckpointId);

        this.labelPrefix =
                selectdbConfig.getLabelPrefix()
                        + "_"
                        + catalogTable.getTableId().toTablePath()
                        + "_"
                        + jobId
                        + "_"
                        + context.getIndexOfSubtask();

        this.lineDelimiter =
                selectdbConfig
                        .getStageLoadProps()
                        .getProperty(
                                LoadConstants.LINE_DELIMITER_KEY,
                                LoadConstants.LINE_DELIMITER_DEFAULT)
                        .getBytes();
        this.labelGenerator = new LabelGenerator(labelPrefix);
        this.serializer =
                createSerializer(
                        selectdbConfig, catalogTable.getTableSchema().toPhysicalRowDataType());
        this.loading = false;
    }

    public void initializeLoad(List<SelectDBSinkState> state) throws IOException {
        this.selectDBStageLoad = new SelectDBStageLoad(selectdbConfig, labelGenerator);
        this.selectDBStageLoad.setCurrentCheckpointID(lastCheckpointId + 1);
        serializer.open();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        byte[] serialize = serializer.serialize(UnsupportedTypeConverterUtils.convertRow(element));
        if (Objects.isNull(serialize)) {
            // schema change is null
            return;
        }
        try {
            this.selectDBStageLoad.writeRecord(serialize);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<SelectDBCommitInfo> prepareCommit() throws IOException {
        checkState(selectDBStageLoad != null);
        log.info("checkpoint arrived, upload buffer to storage");
        try {
            this.selectDBStageLoad.flush(true);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        CopySQLBuilder copySQLBuilder =
                new CopySQLBuilder(selectdbConfig, catalogTable, selectDBStageLoad.getFileList());
        String copySql = copySQLBuilder.buildCopySQL();
        return Optional.of(
                new SelectDBCommitInfo(
                        selectDBStageLoad.getHostPort(), selectdbConfig.getClusterName(), copySql));
    }

    @Override
    public List<SelectDBSinkState> snapshotState(long checkpointId) throws IOException {
        checkState(selectDBStageLoad != null);
        log.info("clear the file list {}", selectDBStageLoad.getFileList());
        this.selectDBStageLoad.clearFileList();
        this.selectDBStageLoad.setCurrentCheckpointID(checkpointId + 1);
        return Collections.singletonList(selectdbSinkState);
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        if (selectDBStageLoad != null) {
            selectDBStageLoad.close();
        }
        serializer.close();
    }

    public static SelectDBSerializer createSerializer(
            SelectDBConfig selectdbConfig, SeaTunnelRowType seaTunnelRowType) {
        String format = selectdbConfig.getStageLoadProps().getProperty(LoadConstants.FORMAT_KEY);
        return new SeaTunnelRowSerializer(
                (format != null ? format : LoadConstants.JSON).toLowerCase(),
                seaTunnelRowType,
                selectdbConfig.getStageLoadProps().getProperty(LoadConstants.FIELD_DELIMITER_KEY),
                selectdbConfig.getEnableDelete());
    }
}
