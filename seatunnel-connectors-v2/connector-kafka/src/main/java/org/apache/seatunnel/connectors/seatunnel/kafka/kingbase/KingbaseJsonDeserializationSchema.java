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

package org.apache.seatunnel.connectors.seatunnel.kafka.kingbase;

import org.apache.seatunnel.api.serialization.DeserializationSchemaWithTopic;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MultipleRowType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.SeaTunnelException;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class KingbaseJsonDeserializationSchema
        implements DeserializationSchemaWithTopic<SeaTunnelRow> {

    private final MultipleRowType dataType;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public KingbaseJsonDeserializationSchema(MultipleRowType dataType) {
        this.dataType = dataType;
    }

    @Override
    public void deserialize(String topic, byte[] message, Collector<SeaTunnelRow> out)
            throws IOException {
        KingBaseRow kingBaseRow = OBJECT_MAPPER.readValue(message, KingBaseRow.class);
        if (kingBaseRow.getOp().equals(KingBaseOp.DDL)) {
            return;
        }
        String tableId =
                topic.toUpperCase()
                        + "."
                        + kingBaseRow.getSchema().toUpperCase()
                        + "."
                        + kingBaseRow.getTable().toUpperCase();
        SeaTunnelRowType rowType = dataType.getRowType(tableId);
        if (rowType == null) {
            return;
        }
        SeaTunnelRow row = new SeaTunnelRow(rowType.getTotalFields());
        switch (kingBaseRow.getOp()) {
            case INSERT:
                row.setRowKind(RowKind.INSERT);
                break;
            case UPDATE:
                row.setRowKind(RowKind.UPDATE_BEFORE);
                break;
            case DELETE:
                row.setRowKind(RowKind.DELETE);
                break;
            default:
                return;
        }
        row.setTableId(tableId);
        for (int i = 0; i < rowType.getFieldNames().length; i++) {
            row.setField(
                    i,
                    convertDataType(
                            kingBaseRow.getRecord().get(rowType.getFieldName(i)),
                            rowType.getFieldType(i)));
        }
        out.collect(row);
        if (kingBaseRow.getOp().equals(KingBaseOp.UPDATE)) {
            SeaTunnelRow updateRow = row.copy();
            updateRow.setRowKind(RowKind.UPDATE_AFTER);
            out.collect(updateRow);
        }
    }

    private Object convertDataType(String data, SeaTunnelDataType<?> dataType) {
        if (data == null) {
            return null;
        }
        switch (dataType.getSqlType()) {
            case TINYINT:
                return Byte.parseByte(data);
            case SMALLINT:
                return Short.parseShort(data);
            case INT:
                return Integer.parseInt(data);
            case BIGINT:
                return Long.parseLong(data);
            case FLOAT:
                return Float.parseFloat(data);
            case DOUBLE:
                return Double.parseDouble(data);
            case DECIMAL:
                DecimalType type = ((DecimalType) dataType);
                return new BigDecimal(data).setScale(type.getScale(), RoundingMode.HALF_UP);
            case STRING:
                return data;
            case DATE:
                return LocalDate.parse(
                        data.substring(0, data.lastIndexOf(".")),
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            case TIME:
                return LocalTime.parse(data);
            case TIMESTAMP:
                return LocalDateTime.parse(
                        data.substring(0, data.lastIndexOf(".")),
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            case BOOLEAN:
                return Boolean.parseBoolean(data);
            case BYTES:
                return data.getBytes();
            case NULL:
                return null;
            case ARRAY:
                try {
                    return OBJECT_MAPPER
                            .readValue(
                                    "[" + data.substring(1).substring(0, data.length() - 2) + "]",
                                    new TypeReference<List<String>>() {})
                            .stream()
                            .map(d -> convertDataType(d, ((ArrayType) dataType).getElementType()))
                            .toArray();
                } catch (Exception e) {
                    throw new SeaTunnelException("parse value in json failed ", e);
                }
            case MAP:
            case ROW:
            default:
                throw new SeaTunnelException("kingbase not support type: " + dataType.getSqlType());
        }
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return dataType;
    }

    @Override
    public SeaTunnelRow deserialize(String topic, byte[] message) {
        throw new UnsupportedOperationException();
    }
}
