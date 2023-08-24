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

package org.apache.seatunnel.connectors.seatunnel.redshift.sink;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

@ToString
public class TableSchemaEnhancer {
    public static final String METADATA_DELETE_FIELD = "st_metadata_column_delete";
    private static final SeaTunnelDataType METADATA_DELETE_FIELD_TYPE = BasicType.BOOLEAN_TYPE;
    private static final Column METADATA_DELETE_COLUMN =
            PhysicalColumn.of(METADATA_DELETE_FIELD, METADATA_DELETE_FIELD_TYPE, 0, true, null, "");

    // Exclude metadata field from table schema.
    @Getter private final CatalogTable primitiveTable;
    // Include metadata field in table schema.
    @Getter private final CatalogTable enhanceTable;
    // Exclude metadata field from row type.
    @Getter private final SeaTunnelRowType primitiveRowType;
    // Include metadata field in row type.
    @Getter private final SeaTunnelRowType enhanceRowType;
    private final SeaTunnelRow reuseContainer;

    public TableSchemaEnhancer(CatalogTable table) {
        this(table, false);
    }

    public TableSchemaEnhancer(SeaTunnelRowType rowType) {
        this(rowType, false);
    }

    public TableSchemaEnhancer(CatalogTable table, boolean reuseMemory) {
        this.primitiveTable = removeMetadataField(table);
        this.primitiveRowType = primitiveTable.getTableSchema().toPhysicalRowDataType();
        this.enhanceTable = addMetadataField(table);
        this.enhanceRowType = enhanceTable.getTableSchema().toPhysicalRowDataType();
        this.reuseContainer =
                reuseMemory
                        ? new SeaTunnelRow(enhanceTable.getTableSchema().getColumns().size())
                        : null;
    }

    public TableSchemaEnhancer(SeaTunnelRowType rowType, boolean reuseMemory) {
        this.primitiveTable = null;
        this.enhanceTable = null;
        this.primitiveRowType = removeMetadataField(rowType);
        this.enhanceRowType = addMetadataField(rowType);
        this.reuseContainer =
                reuseMemory ? new SeaTunnelRow(enhanceRowType.getTotalFields()) : null;
    }

    public SeaTunnelRow enhanceRow(SeaTunnelRow row) {
        SeaTunnelRow copyRow;
        if (this.reuseContainer == null) {
            copyRow = new SeaTunnelRow(row.getArity() + 1);
        } else {
            copyRow = getAndCleanReuseContainer();
        }

        System.arraycopy(row.getFields(), 0, copyRow.getFields(), 0, copyRow.getArity() - 1);
        copyRow.setField(copyRow.getArity() - 1, RowKind.DELETE.equals(row.getRowKind()));
        copyRow.setRowKind(row.getRowKind());
        copyRow.setTableId(row.getTableId());
        return copyRow;
    }

    private SeaTunnelRow getAndCleanReuseContainer() {
        for (int i = 0; i < reuseContainer.getArity(); i++) {
            reuseContainer.setField(i, null);
        }
        reuseContainer.setTableId(null);
        reuseContainer.setRowKind(null);
        return reuseContainer;
    }

    public CatalogTable getEnhanceTable() {
        if (enhanceTable != null) {
            List<Column> columns = enhanceTable.getTableSchema().getColumns();
            if (!columns.get(columns.size() - 1).getName().equals(METADATA_DELETE_FIELD)) {
                throw new IllegalStateException("The last column must be " + METADATA_DELETE_FIELD);
            }

            long deleteFieldCount =
                    columns.stream()
                            .filter(column -> column.getName().equals(METADATA_DELETE_FIELD))
                            .count();
            if (deleteFieldCount != 1) {
                throw new IllegalStateException("The last column must be " + METADATA_DELETE_FIELD);
            }
        }
        return enhanceTable;
    }

    public SeaTunnelRowType getEnhanceRowType() {
        if (enhanceRowType != null) {
            if (!METADATA_DELETE_FIELD.equals(
                    enhanceRowType.getFieldName(enhanceRowType.getTotalFields() - 1))) {
                throw new IllegalStateException("The last column must be " + METADATA_DELETE_FIELD);
            }

            long deleteFieldCount =
                    Stream.of(enhanceRowType.getFieldNames())
                            .filter(field -> field.equals(METADATA_DELETE_FIELD))
                            .count();
            if (deleteFieldCount != 1) {
                throw new IllegalStateException("The last column must be " + METADATA_DELETE_FIELD);
            }
        }
        return enhanceRowType;
    }

    private static SeaTunnelRowType addMetadataField(SeaTunnelRowType rowType) {
        List<String> originFields = new ArrayList<>();
        List<SeaTunnelDataType<?>> originFieldTypes = new ArrayList();
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            String fieldName = rowType.getFieldName(i);
            SeaTunnelDataType<?> fieldType = rowType.getFieldType(i);
            if (METADATA_DELETE_FIELD.equals(fieldName)) {
                continue;
            }

            originFields.add(fieldName);
            originFieldTypes.add(fieldType);
        }

        originFields.add(METADATA_DELETE_FIELD);
        originFieldTypes.add(METADATA_DELETE_FIELD_TYPE);
        return new SeaTunnelRowType(
                originFields.toArray(new String[0]),
                originFieldTypes.toArray(new SeaTunnelDataType<?>[0]));
    }

    private static SeaTunnelRowType removeMetadataField(SeaTunnelRowType rowType) {
        List<String> originFields = new ArrayList<>();
        List<SeaTunnelDataType<?>> originFieldTypes = new ArrayList();
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            String fieldName = rowType.getFieldName(i);
            SeaTunnelDataType<?> fieldType = rowType.getFieldType(i);
            if (METADATA_DELETE_FIELD.equals(fieldName)) {
                continue;
            }

            originFields.add(fieldName);
            originFieldTypes.add(fieldType);
        }
        return new SeaTunnelRowType(
                originFields.toArray(new String[0]),
                originFieldTypes.toArray(new SeaTunnelDataType<?>[0]));
    }

    private static CatalogTable addMetadataField(CatalogTable table) {
        List<Column> copyColumns = new ArrayList<>();
        for (Column column : table.getTableSchema().getColumns()) {
            if (column.getName().equals(METADATA_DELETE_FIELD)) {
                continue;
            }
            copyColumns.add(column);
        }

        copyColumns.add(METADATA_DELETE_COLUMN);
        TableSchema copySchema =
                TableSchema.builder()
                        .columns(copyColumns)
                        .constraintKey(table.getTableSchema().getConstraintKeys())
                        .primaryKey(table.getTableSchema().getPrimaryKey())
                        .build();
        return CatalogTable.of(
                table.getTableId(),
                copySchema,
                table.getOptions(),
                table.getPartitionKeys(),
                table.getComment(),
                table.getCatalogName());
    }

    private static CatalogTable removeMetadataField(CatalogTable table) {
        List<Column> copyColumns = new ArrayList<>();
        for (Column column : table.getTableSchema().getColumns()) {
            if (column.getName().equals(METADATA_DELETE_FIELD)) {
                continue;
            }
            copyColumns.add(column);
        }

        TableSchema copySchema =
                TableSchema.builder()
                        .columns(copyColumns)
                        .constraintKey(table.getTableSchema().getConstraintKeys())
                        .primaryKey(table.getTableSchema().getPrimaryKey())
                        .build();
        return CatalogTable.of(
                table.getTableId(),
                copySchema,
                table.getOptions(),
                table.getPartitionKeys(),
                table.getComment(),
                table.getCatalogName());
    }
}
