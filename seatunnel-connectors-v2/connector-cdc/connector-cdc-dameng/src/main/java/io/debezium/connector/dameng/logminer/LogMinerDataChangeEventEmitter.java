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

package io.debezium.connector.dameng.logminer;

import io.debezium.DebeziumException;
import io.debezium.connector.dameng.DamengDataChangeEventEmitter;
import io.debezium.connector.dameng.logminer.parser.LogMinerDmlEntry;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Table;
import io.debezium.util.Clock;

/** Emits change record based on a single {@link LogMinerDmlEntry} event. */
public class LogMinerDataChangeEventEmitter extends DamengDataChangeEventEmitter {

    private final int operation;
    private final Object[] oldValues;
    private final Object[] newValues;

    public LogMinerDataChangeEventEmitter(
            OffsetContext offset,
            int operation,
            Object[] oldValues,
            Object[] newValues,
            Table table,
            Clock clock) {
        super(offset, clock);
        this.operation = operation;
        this.oldValues = oldValues;
        this.newValues = newValues;
    }

    @Override
    protected Operation getOperation() {
        switch (operation) {
            case RowMapper.INSERT:
                return Operation.CREATE;
            case RowMapper.UPDATE:
            case RowMapper.SELECT_LOB_LOCATOR:
                return Operation.UPDATE;
            case RowMapper.DELETE:
                return Operation.DELETE;
            default:
                throw new DebeziumException("Unsupported operation type: " + operation);
        }
    }

    @Override
    protected Object[] getOldColumnValues() {
        return oldValues;
    }

    @Override
    protected Object[] getNewColumnValues() {
        return newValues;
    }
}
