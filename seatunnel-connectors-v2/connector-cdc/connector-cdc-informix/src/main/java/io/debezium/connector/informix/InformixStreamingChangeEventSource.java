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

package io.debezium.connector.informix;

import org.apache.seatunnel.connectors.cdc.informix.config.InformixSourceConfig;

import org.apache.kafka.connect.data.Field;

import com.informix.jdbc.IfmxReadableType;
import com.informix.stream.api.IfmxStreamRecord;
import com.informix.stream.cdc.records.IfxCDCBeginTransactionRecord;
import com.informix.stream.cdc.records.IfxCDCCommitTransactionRecord;
import com.informix.stream.cdc.records.IfxCDCMetaDataRecord;
import com.informix.stream.cdc.records.IfxCDCOperationRecord;
import com.informix.stream.cdc.records.IfxCDCRollbackTransactionRecord;
import com.informix.stream.cdc.records.IfxCDCTimeoutRecord;
import com.informix.stream.impl.IfxStreamException;
import com.informix.stream.transactions.IfmxStreamTransactionRecord;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class InformixStreamingChangeEventSource
        implements StreamingChangeEventSource<InformixOffsetContext> {
    private final InformixSourceConfig sourceConfig;
    private final InformixConnection connection;
    private final List<TableId> tableIds;
    private final EventDispatcher<TableId> eventDispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final InformixDatabaseSchema databaseSchema;

    public InformixStreamingChangeEventSource(
            InformixSourceConfig sourceConfig,
            InformixConnection connection,
            List<TableId> tableIds,
            EventDispatcher<TableId> eventDispatcher,
            ErrorHandler errorHandler,
            Clock clock,
            InformixDatabaseSchema databaseSchema) {
        this.sourceConfig = sourceConfig;
        this.connection = connection;
        this.tableIds = tableIds;
        this.eventDispatcher = eventDispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.databaseSchema = databaseSchema;
    }

    @Override
    public void execute(ChangeEventSourceContext context, InformixOffsetContext offsetContext)
            throws InterruptedException {
        InformixCDCEngine cdcEngine = connection.getCdcEngine();
        InformixTransactionCache transCache = offsetContext.getInformixTransactionCache();

        TxLogPosition lastPosition = offsetContext.getChangePosition();
        Long fromLsn = lastPosition.getCommitLsn();
        cdcEngine.setStartLsn(fromLsn);
        cdcEngine.init(databaseSchema);

        // restore
        while (context.isRunning()) {
            if (lastPosition.getChangeLsn() <= lastPosition.getCommitLsn()) {
                log.info(
                        "Recover skipped, since changeLsn='{}' >= commitLsn='{}'",
                        lastPosition.getChangeLsn(),
                        lastPosition.getCommitLsn());
                break;
            }

            try {
                IfmxStreamRecord record = cdcEngine.getCdcEngine().getRecord();
                if (record.getSequenceId() >= lastPosition.getChangeLsn()) {
                    log.info(
                            "Recover finished: from {} to {}, now Current seqId={}",
                            lastPosition.getCommitLsn(),
                            lastPosition.getChangeLsn(),
                            record.getSequenceId());
                    break;
                }
                switch (record.getType()) {
                    case TIMEOUT:
                        handleTimeout(offsetContext, (IfxCDCTimeoutRecord) record);
                        break;
                    case BEFORE_UPDATE:
                        handleBeforeUpdate(
                                offsetContext, (IfxCDCOperationRecord) record, transCache, true);
                        break;
                    case AFTER_UPDATE:
                        handleAfterUpdate(
                                cdcEngine,
                                offsetContext,
                                (IfxCDCOperationRecord) record,
                                transCache,
                                true);
                        break;
                    case BEGIN:
                        handleBegin(
                                offsetContext,
                                (IfxCDCBeginTransactionRecord) record,
                                transCache,
                                true);
                        break;
                    case INSERT:
                        handleInsert(
                                cdcEngine,
                                offsetContext,
                                (IfxCDCOperationRecord) record,
                                transCache,
                                true);
                        break;
                    case COMMIT:
                        handleCommit(
                                offsetContext,
                                (IfxCDCCommitTransactionRecord) record,
                                transCache,
                                true);
                        break;
                    case ROLLBACK:
                        handleRollback(
                                offsetContext,
                                (IfxCDCRollbackTransactionRecord) record,
                                transCache,
                                false);
                        break;
                    case METADATA:
                        handleMetadata(cdcEngine, (IfxCDCMetaDataRecord) record);
                        break;
                    case DELETE:
                        break;
                    default:
                        log.info("Handle unknown record-type = {}", record.getType());
                }
            } catch (SQLException e) {
                log.error("Caught SQLException", e);
                errorHandler.setProducerThrowable(e);
            } catch (IfxStreamException e) {
                log.error("Caught IfxStreamException", e);
                errorHandler.setProducerThrowable(e);
            }
        }

        try {
            while (context.isRunning()) {
                cdcEngine.stream(
                        (IfmxStreamRecord record) -> {
                            switch (record.getType()) {
                                case TIMEOUT:
                                    handleTimeout(offsetContext, (IfxCDCTimeoutRecord) record);
                                    break;
                                case BEFORE_UPDATE:
                                    handleBeforeUpdate(
                                            offsetContext,
                                            (IfxCDCOperationRecord) record,
                                            transCache,
                                            false);
                                    break;
                                case AFTER_UPDATE:
                                    handleAfterUpdate(
                                            cdcEngine,
                                            offsetContext,
                                            (IfxCDCOperationRecord) record,
                                            transCache,
                                            false);
                                    break;
                                case BEGIN:
                                    handleBegin(
                                            offsetContext,
                                            (IfxCDCBeginTransactionRecord) record,
                                            transCache,
                                            false);
                                    break;
                                case INSERT:
                                    handleInsert(
                                            cdcEngine,
                                            offsetContext,
                                            (IfxCDCOperationRecord) record,
                                            transCache,
                                            false);
                                    break;
                                case COMMIT:
                                    handleCommit(
                                            offsetContext,
                                            (IfxCDCCommitTransactionRecord) record,
                                            transCache,
                                            false);
                                    break;
                                case ROLLBACK:
                                    handleRollback(
                                            offsetContext,
                                            (IfxCDCRollbackTransactionRecord) record,
                                            transCache,
                                            false);
                                    break;
                                case DELETE:
                                    handleDelete(
                                            cdcEngine,
                                            offsetContext,
                                            (IfxCDCOperationRecord) record,
                                            transCache,
                                            false);
                                    break;
                                case TRANSACTION_GROUP:
                                    handleTransactionGroup(
                                            offsetContext, (IfmxStreamTransactionRecord) record);
                                default:
                                    log.info("Handle unknown record-type = {}", record.getType());
                            }

                            return false;
                        });
            }
        } catch (SQLException e) {
            log.error("Caught SQLException", e);
            errorHandler.setProducerThrowable(e);
        } catch (IfxStreamException e) {
            log.error("Caught IfxStreamException", e);
            errorHandler.setProducerThrowable(e);
        } catch (Exception e) {
            log.error("Caught Unknown Exception", e);
            errorHandler.setProducerThrowable(e);
        } finally {
            cdcEngine.close();
        }
    }

    private void handleTransactionGroup(
            InformixOffsetContext offsetContext, IfmxStreamTransactionRecord record) {
        log.debug(
                "getTransactionGroup: seqId={}, groupSeqId={}, groupSize={}",
                record.getSequenceId(),
                record.getSequenceId(),
                record.getOperationRecords().size());
    }

    public void handleDelete(
            InformixCDCEngine cdcEngine,
            InformixOffsetContext offsetContext,
            IfxCDCOperationRecord record,
            InformixTransactionCache transactionCache,
            boolean recover)
            throws IfxStreamException, SQLException {
        long _start = System.nanoTime();
        Long transId = (long) record.getTransactionId();

        Map<String, IfmxReadableType> data = record.getData();
        Map<Integer, TableId> label2TableId = cdcEngine.convertLabel2TableId();
        TableId tid = label2TableId.get(Integer.parseInt(record.getLabel()));
        handleEvent(
                tid,
                offsetContext,
                record,
                InformixChangeRecordEmitter.OP_DELETE,
                data,
                null,
                clock);

        Optional<InformixTransactionCache.TransactionCacheBuffer> minTransactionCache =
                transactionCache.getMinTransactionCache();
        Long minSeqId =
                minTransactionCache.isPresent()
                        ? minTransactionCache.get().getBeginSeqId()
                        : record.getSequenceId();

        if (!recover) {
            offsetContext.setChangePosition(
                    TxLogPosition.cloneAndSet(
                            offsetContext.getChangePosition(),
                            minSeqId,
                            record.getSequenceId(),
                            transId,
                            TxLogPosition.LSN_NULL));
        }

        long _end = System.nanoTime();
        log.debug(
                "Received INSERT :: transId={} seqId={} elapsedTime={} ms",
                record.getTransactionId(),
                record.getSequenceId(),
                (_end - _start) / 1000000d);
    }

    public void handleTimeout(InformixOffsetContext offsetContext, IfxCDCTimeoutRecord record) {
        offsetContext.setChangePosition(
                TxLogPosition.cloneAndSet(
                        offsetContext.getChangePosition(),
                        TxLogPosition.LSN_NULL,
                        record.getSequenceId(),
                        TxLogPosition.LSN_NULL,
                        TxLogPosition.LSN_NULL));
    }

    public void handleMetadata(InformixCDCEngine cdcEngine, IfxCDCMetaDataRecord record) {
        log.debug(
                "Received A Metadata: type={}, label={}, seqId={}",
                record.getType(),
                record.getLabel(),
                record.getSequenceId());
    }

    public void handleBeforeUpdate(
            InformixOffsetContext offsetContext,
            IfxCDCOperationRecord record,
            InformixTransactionCache transactionCache,
            boolean recover)
            throws IfxStreamException {

        Map<String, IfmxReadableType> data = record.getData();
        Long transId = (long) record.getTransactionId();
        transactionCache.beforeUpdate(transId, data);

        if (!recover) {
            offsetContext.setChangePosition(
                    TxLogPosition.cloneAndSet(
                            offsetContext.getChangePosition(),
                            TxLogPosition.LSN_NULL,
                            record.getSequenceId(),
                            transId,
                            TxLogPosition.LSN_NULL));
        }
    }

    public void handleAfterUpdate(
            InformixCDCEngine cdcEngine,
            InformixOffsetContext offsetContext,
            IfxCDCOperationRecord record,
            InformixTransactionCache transactionCache,
            boolean recover)
            throws IfxStreamException, SQLException {
        Long transId = (long) record.getTransactionId();

        Map<String, IfmxReadableType> newData = record.getData();
        Map<String, IfmxReadableType> oldData = transactionCache.afterUpdate(transId).get();

        Map<Integer, TableId> label2TableId = cdcEngine.convertLabel2TableId();
        TableId tid = label2TableId.get(Integer.parseInt(record.getLabel()));
        handleEvent(
                tid,
                offsetContext,
                record,
                InformixChangeRecordEmitter.OP_UPDATE,
                oldData,
                newData,
                clock);

        if (!recover) {
            offsetContext.setChangePosition(
                    TxLogPosition.cloneAndSet(
                            offsetContext.getChangePosition(),
                            TxLogPosition.LSN_NULL,
                            record.getSequenceId(),
                            transId,
                            TxLogPosition.LSN_NULL));
        }
    }

    public void handleBegin(
            InformixOffsetContext offsetContext,
            IfxCDCBeginTransactionRecord record,
            InformixTransactionCache transactionCache,
            boolean recover)
            throws IfxStreamException {
        long _start = System.nanoTime();

        Long transId = (long) record.getTransactionId();
        Long beginTs = record.getTime();
        Long seqId = record.getSequenceId();

        Optional<InformixTransactionCache.TransactionCacheBuffer> transactionCacheBuffer =
                transactionCache.beginTxn(transId, beginTs, seqId);
        if (!recover) {
            Optional<InformixTransactionCache.TransactionCacheBuffer> minTransactionCache =
                    transactionCache.getMinTransactionCache();
            Long minSeqId =
                    minTransactionCache.isPresent()
                            ? minTransactionCache.get().getBeginSeqId()
                            : record.getSequenceId();

            if (!transactionCacheBuffer.isPresent()) {
                offsetContext.setChangePosition(
                        TxLogPosition.cloneAndSet(
                                offsetContext.getChangePosition(),
                                minSeqId,
                                record.getSequenceId(),
                                transId,
                                record.getSequenceId()));

                offsetContext
                        .getTransactionContext()
                        .beginTransaction(String.valueOf(record.getTransactionId()));
            }
        }

        long _end = System.nanoTime();

        log.debug(
                "Received BEGIN :: transId={} seqId={} time={} userId={} elapsedTs={}ms",
                record.getTransactionId(),
                record.getSequenceId(),
                record.getTime(),
                record.getUserId(),
                (_end - _start) / 1000000d);
    }

    public void handleCommit(
            InformixOffsetContext offsetContext,
            IfxCDCCommitTransactionRecord record,
            InformixTransactionCache transactionCache,
            boolean recover)
            throws InterruptedException, IfxStreamException {
        long _start = System.nanoTime();
        Long transId = (long) record.getTransactionId();
        Long endTime = record.getTime();

        Optional<InformixTransactionCache.TransactionCacheBuffer> transactionCacheBuffer =
                transactionCache.commitTxn(transId, endTime);
        if (!recover) {
            Optional<InformixTransactionCache.TransactionCacheBuffer> minTransactionCache =
                    transactionCache.getMinTransactionCache();
            Long minSeqId =
                    minTransactionCache.isPresent()
                            ? minTransactionCache.get().getBeginSeqId()
                            : record.getSequenceId();

            if (transactionCacheBuffer.isPresent()) {
                offsetContext.setChangePosition(
                        TxLogPosition.cloneAndSet(
                                offsetContext.getChangePosition(),
                                minSeqId,
                                record.getSequenceId(),
                                transId,
                                TxLogPosition.LSN_NULL));

                for (InformixTransactionCache.TransactionCacheRecord r :
                        transactionCacheBuffer.get().getTransactionCacheRecords()) {
                    eventDispatcher.dispatchDataChangeEvent(
                            r.getTableId(), r.getInformixChangeRecordEmitter());
                }
                log.info(
                        "Handle Commit {} Events, transElapsedTime={}",
                        transactionCacheBuffer.get().size(),
                        transactionCacheBuffer.get().getElapsed());
            }
            offsetContext.getTransactionContext().endTransaction();
        }

        long _end = System.nanoTime();
        log.debug(
                "Received COMMIT :: transId={} seqId={} time={} elapsedTime={} ms",
                record.getTransactionId(),
                record.getSequenceId(),
                record.getTime(),
                (_end - _start) / 1000000d);
    }

    public void handleInsert(
            InformixCDCEngine cdcEngine,
            InformixOffsetContext offsetContext,
            IfxCDCOperationRecord record,
            InformixTransactionCache transactionCache,
            boolean recover)
            throws IfxStreamException, SQLException {
        long _start = System.nanoTime();
        Long transId = (long) record.getTransactionId();

        Optional<InformixTransactionCache.TransactionCacheBuffer> minTransactionCache =
                transactionCache.getMinTransactionCache();
        Long minSeqId =
                minTransactionCache.isPresent()
                        ? minTransactionCache.get().getBeginSeqId()
                        : record.getSequenceId();

        Map<String, IfmxReadableType> data = record.getData();
        Map<Integer, TableId> label2TableId = cdcEngine.convertLabel2TableId();
        TableId tid = label2TableId.get(Integer.parseInt(record.getLabel()));
        handleEvent(
                tid,
                offsetContext,
                record,
                InformixChangeRecordEmitter.OP_INSERT,
                null,
                data,
                clock);

        if (!recover) {
            offsetContext.setChangePosition(
                    TxLogPosition.cloneAndSet(
                            offsetContext.getChangePosition(),
                            minSeqId,
                            record.getSequenceId(),
                            transId,
                            TxLogPosition.LSN_NULL));
        }

        long _end = System.nanoTime();
        log.debug(
                "Received INSERT :: transId={} seqId={} elapsedTime={} ms",
                record.getTransactionId(),
                record.getSequenceId(),
                (_end - _start) / 1000000d);
    }

    public void handleRollback(
            InformixOffsetContext offsetContext,
            IfxCDCRollbackTransactionRecord record,
            InformixTransactionCache transactionCache,
            boolean recover)
            throws IfxStreamException {
        long _start = System.nanoTime();
        Long transId = (long) record.getTransactionId();

        Optional<InformixTransactionCache.TransactionCacheBuffer> transactionCacheBuffer =
                transactionCache.rollbackTxn(transId);
        if (!recover) {
            Optional<InformixTransactionCache.TransactionCacheBuffer> minTransactionCache =
                    transactionCache.getMinTransactionCache();
            Long minSeqId =
                    minTransactionCache.isPresent()
                            ? minTransactionCache.get().getBeginSeqId()
                            : record.getSequenceId();

            if (minTransactionCache.isPresent()) {
                minSeqId = minTransactionCache.get().getBeginSeqId();
            }
            if (transactionCacheBuffer.isPresent()) {
                offsetContext.setChangePosition(
                        TxLogPosition.cloneAndSet(
                                offsetContext.getChangePosition(),
                                minSeqId,
                                record.getSequenceId(),
                                transId,
                                TxLogPosition.LSN_NULL));

                log.debug("Rollback Txn: {}", record.getTransactionId());
            }
            offsetContext.getTransactionContext().endTransaction();
        }

        long _end = System.nanoTime();
        log.debug(
                "Received ROLLBACK :: transId={} seqId={} elapsedTime={} ms",
                record.getTransactionId(),
                record.getSequenceId(),
                (_end - _start) / 1000000d);
    }

    public void handleEvent(
            TableId tableId,
            InformixOffsetContext offsetContext,
            IfxCDCOperationRecord record,
            Integer operation,
            Map<String, IfmxReadableType> data,
            Map<String, IfmxReadableType> dataNext,
            Clock clock)
            throws SQLException {

        Long transId = (long) record.getTransactionId();
        Long beginTs = System.currentTimeMillis();
        Long seqId = record.getSequenceId();

        offsetContext.event(tableId, clock.currentTime());
        List<String> fields =
                this.databaseSchema.schemaFor(tableId).valueSchema().fields().stream()
                        .map(Field::name)
                        .collect(Collectors.toList());

        InformixChangeRecordEmitter informixChangeRecordEmitter =
                new InformixChangeRecordEmitter(
                        offsetContext,
                        operation,
                        InformixChangeRecordEmitter.convertIfxData2Array(data, fields),
                        InformixChangeRecordEmitter.convertIfxData2Array(dataNext, fields),
                        clock);

        offsetContext
                .getInformixTransactionCache()
                .addEvent2Tx(tableId, informixChangeRecordEmitter, transId, beginTs, seqId);
    }
}
