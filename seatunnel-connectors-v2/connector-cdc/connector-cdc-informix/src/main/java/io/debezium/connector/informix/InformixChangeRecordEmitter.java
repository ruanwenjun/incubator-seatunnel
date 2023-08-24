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
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.util.Clock;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InformixChangeRecordEmitter extends RelationalChangeRecordEmitter {
    public static final int OP_DELETE = 1;
    public static final int OP_INSERT = 2;
    public static final int OP_UPDATE = 3;
    // public static final int OP_UPDATE_AFTER = 4;
    public static final int OP_TRUNCATE = 5;

    private final int operation;
    private final Object[] data;
    private final Object[] dataNext;

    public InformixChangeRecordEmitter(
            OffsetContext offset, int operation, Object[] data, Object[] dataNext, Clock clock) {
        super(offset, clock);

        this.operation = operation;
        this.data = data;
        this.dataNext = dataNext;
    }

    @Override
    protected Operation getOperation() {
        if (operation == OP_DELETE) {
            return Operation.DELETE;
        } else if (operation == OP_INSERT) {
            return Operation.CREATE;
        } else if (operation == OP_UPDATE) {
            return Operation.UPDATE;
        }
        throw new IllegalArgumentException(
                "Received event of unexpected command type: " + operation);
    }

    @Override
    protected Object[] getOldColumnValues() {
        switch (getOperation()) {
            case CREATE:
            case READ:
                return null;
            default:
                return data;
        }
    }

    @Override
    protected Object[] getNewColumnValues() {
        switch (getOperation()) {
            case CREATE:
            case UPDATE:
                return dataNext;
            case READ:
                return data;
            default:
                return null;
        }
    }

    public static Object[] convertIfxData2Array(
            Map<String, IfmxReadableType> data, List<String> fields) throws SQLException {
        if (data == null) {
            return new Object[0];
        }

        List<Object> list = new ArrayList<>();
        for (String field : fields) {
            Object toObject = data.get(field).toObject();
            list.add(toObject);
        }
        return list.toArray();
    }
}
