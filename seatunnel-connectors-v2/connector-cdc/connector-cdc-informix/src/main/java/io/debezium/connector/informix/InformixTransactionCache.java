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

import com.informix.jdbc.IfmxReadableType;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class InformixTransactionCache {

    private Map<Long, TransactionCacheBuffer> transactionCacheBufferMap;
    private Map<Long, Map<String, IfmxReadableType>> beforeAndAfter;

    public InformixTransactionCache() {
        // TODO: try HPPC or FastUtil?
        this.transactionCacheBufferMap = new Hashtable<>();
        this.beforeAndAfter = new HashMap<>();
    }

    public Optional<TransactionCacheBuffer> beginTxn(Long txn, Long beginTs, Long beginSeqId) {
        if (transactionCacheBufferMap.containsKey(txn)) {
            log.warn("Transaction key={} already exists in InformixTransactionCache", txn);
            return Optional.empty();
        }
        TransactionCacheBuffer tb = new TransactionCacheBuffer(4096, beginTs, beginSeqId);
        return Optional.ofNullable(transactionCacheBufferMap.put(txn, tb));
    }

    public Optional<TransactionCacheBuffer> commitTxn(Long txn, Long endTime) {
        if (!transactionCacheBufferMap.containsKey(txn)) {
            log.warn(
                    "Transaction key={} does not exist in InformixTransactionCache while commitTxn()",
                    txn);
            return Optional.empty();
        }

        TransactionCacheBuffer transactionCacheBuffer = transactionCacheBufferMap.remove(txn);
        transactionCacheBuffer.setEndTime(endTime);
        return Optional.of(transactionCacheBuffer);
    }

    public Optional<TransactionCacheBuffer> rollbackTxn(Long txn) {
        if (!transactionCacheBufferMap.containsKey(txn)) {
            log.warn(
                    "Transaction key={} does not exist in InformixTransactionCache while rollbackTxn()",
                    txn);
            return Optional.empty();
        }

        return Optional.ofNullable(transactionCacheBufferMap.remove(txn));
    }

    public void addEvent2Tx(
            TableId tableId,
            InformixChangeRecordEmitter event,
            Long txn,
            long beginTs,
            long beginSeqId) {
        if (event != null) {
            TransactionCacheBuffer buffer =
                    transactionCacheBufferMap.computeIfAbsent(
                            txn, t -> new TransactionCacheBuffer(4096, beginTs, beginSeqId));
            buffer.getTransactionCacheRecords().add(new TransactionCacheRecord(tableId, event));
        }
    }

    public Optional<Map<String, IfmxReadableType>> beforeUpdate(
            Long txn, Map<String, IfmxReadableType> data) {
        if (beforeAndAfter.containsKey(txn)) {
            log.warn("Transaction key={} already exists in BeforeAfterCache", txn);
            return Optional.empty();
        }

        return Optional.ofNullable(beforeAndAfter.put(txn, data));
    }

    public Optional<Map<String, IfmxReadableType>> afterUpdate(Long txn) {
        if (!beforeAndAfter.containsKey(txn)) {
            log.warn("Transaction key={} does not exist in BeforeAfterCache", txn);
            return Optional.empty();
        }

        return Optional.ofNullable(beforeAndAfter.remove(txn));
    }

    public Optional<TransactionCacheBuffer> getMinTransactionCache() {
        // TODO: Find the TransactionCache with the minimal beginTime/sequenceIdx
        Map.Entry<Long, TransactionCacheBuffer> minEntry = null;
        for (Map.Entry<Long, TransactionCacheBuffer> entry : transactionCacheBufferMap.entrySet()) {
            if (minEntry == null
                    || minEntry.getValue().getBeginSeqId() > entry.getValue().getBeginSeqId()) {
                minEntry = entry;
            }
        }
        return Optional.ofNullable(minEntry != null ? minEntry.getValue() : null);
    }

    public static class TransactionCacheBuffer {

        private final List<TransactionCacheRecord> transactionCacheRecordList;
        private Long beginTime; // Begin time of transaction
        private Long endTime; // Commit/Rollback of the transaction
        private Long beginSeqId;

        public TransactionCacheBuffer(int initialSize, Long beginTs, Long beginSeqId) {
            transactionCacheRecordList = new ArrayList<>(initialSize);
            this.beginTime = beginTs;
            this.endTime = -1L;
            this.beginSeqId = beginSeqId;
        }

        public List<TransactionCacheRecord> getTransactionCacheRecords() {
            return transactionCacheRecordList;
        }

        public Long getBeginTime() {
            return beginTime;
        }

        public Long getEndTime() {
            return endTime;
        }

        public void setEndTime(Long endTime) {
            this.endTime = endTime;
        }

        public Long getBeginSeqId() {
            return beginSeqId;
        }

        public void setBeginSeqId(Long beginSeqId) {
            this.beginSeqId = beginSeqId;
        }

        public Long getElapsed() {
            return this.endTime - this.beginTime;
        }

        public int size() {
            return transactionCacheRecordList.size();
        }
    }

    public static class TransactionCacheRecord {

        private TableId tableId;
        private InformixChangeRecordEmitter informixChangeRecordEmitter;

        public TransactionCacheRecord(
                TableId tableId, InformixChangeRecordEmitter informixChangeRecordEmitter) {
            this.tableId = tableId;
            this.informixChangeRecordEmitter = informixChangeRecordEmitter;
        }

        public TableId getTableId() {
            return tableId;
        }

        public void setTableId(TableId tableId) {
            this.tableId = tableId;
        }

        public InformixChangeRecordEmitter getInformixChangeRecordEmitter() {
            return informixChangeRecordEmitter;
        }

        public void setInformixChangeRecordEmitter(
                InformixChangeRecordEmitter informixChangeRecordEmitter) {
            this.informixChangeRecordEmitter = informixChangeRecordEmitter;
        }
    }
}
