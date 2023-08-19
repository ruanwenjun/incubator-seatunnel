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

package org.apache.seatunnel.connectors.cdc.informix.source.reader.fetch.cdc;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.cdc.informix.config.InformixSourceConfig;
import org.apache.seatunnel.connectors.cdc.informix.source.offset.InformixOffset;
import org.apache.seatunnel.connectors.cdc.informix.source.reader.fetch.snapshot.InformixSnapshotFetchTask;

import com.informix.jdbc.IfmxReadableType;
import com.informix.stream.cdc.records.IfxCDCOperationRecord;
import io.debezium.DebeziumException;
import io.debezium.connector.informix.InformixConnection;
import io.debezium.connector.informix.InformixDatabaseSchema;
import io.debezium.connector.informix.InformixOffsetContext;
import io.debezium.connector.informix.InformixStreamingChangeEventSource;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.connectors.cdc.informix.source.offset.InformixOffset.NO_STOPPING_OFFSET;

@Slf4j
public class InformixCDCLogSplitReadTask extends InformixStreamingChangeEventSource {

    private final IncrementalSplit split;
    private final InformixOffsetContext offsetContext;
    private final JdbcSourceEventDispatcher eventDispatcher;
    private final ErrorHandler errorHandler;
    private ChangeEventSourceContext context;

    public InformixCDCLogSplitReadTask(
            InformixOffsetContext offsetContext,
            InformixSourceConfig sourceConfig,
            InformixConnection connection,
            JdbcSourceEventDispatcher eventDispatcher,
            ErrorHandler errorHandler,
            InformixDatabaseSchema databaseSchema,
            IncrementalSplit split) {
        super(
                sourceConfig,
                connection,
                split.getTableIds(),
                eventDispatcher,
                errorHandler,
                Clock.SYSTEM,
                databaseSchema);
        this.split = split;
        this.offsetContext = offsetContext;
        this.eventDispatcher = eventDispatcher;
        this.errorHandler = errorHandler;
    }

    @Override
    public void execute(ChangeEventSourceContext context, InformixOffsetContext offsetContext)
            throws InterruptedException {
        this.context = context;
        super.execute(context, this.offsetContext);
    }

    @Override
    public void handleEvent(
            TableId tableId,
            InformixOffsetContext offsetContext,
            IfxCDCOperationRecord record,
            Integer operation,
            Map<String, IfmxReadableType> data,
            Map<String, IfmxReadableType> dataNext,
            Clock clock)
            throws SQLException {
        super.handleEvent(tableId, offsetContext, record, operation, data, dataNext, clock);
        // check do we need to stop for fetch cdc for snapshot split.
        if (isBoundedRead()) {
            InformixOffset currentInformixOffset = getLogMinerPosition(offsetContext.getOffset());
            // reach the high watermark, the cdc fetcher should be finished
            if (currentInformixOffset.isAtOrAfter(split.getStopOffset())) {
                // send cdc end event
                try {
                    eventDispatcher.dispatchWatermarkEvent(
                            offsetContext.getPartition(),
                            split,
                            currentInformixOffset,
                            WatermarkKind.END);
                } catch (InterruptedException e) {
                    log.error("Send signal event error.", e);
                    errorHandler.setProducerThrowable(
                            new DebeziumException("Error processing logminer signal event", e));
                }
                // tell fetcher the cdc task finished
                ((InformixSnapshotFetchTask.SnapshotScnSplitChangeEventSourceContext) context)
                        .finished();
            }
        }
    }

    private boolean isBoundedRead() {
        return !NO_STOPPING_OFFSET.equals(split.getStopOffset());
    }

    public static InformixOffset getLogMinerPosition(Map<String, ?> offset) {
        Map<String, String> offsetStrMap = new HashMap<>();
        for (Map.Entry<String, ?> entry : offset.entrySet()) {
            offsetStrMap.put(
                    entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
        }
        return new InformixOffset(offsetStrMap);
    }
}
