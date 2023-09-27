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

package org.apache.seatunnel.api.table.catalog.schema;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableSchema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.util.List;

class ConfigParserTest extends BaseConfigParserTest {

    private final String columnConfig = "/conf/catalog/schema_column.conf";
    private final String fieldConfig = "/conf/catalog/schema_field.conf";

    @Test
    void parseColumn() throws FileNotFoundException, URISyntaxException {
        Config config = getConfig(columnConfig);
        Config schemaConfig = config.getConfig(TableSchemaOptions.SCHEMA.key());
        ConfigParser configParser = new ConfigParser();
        TableSchema tableSchema = configParser.parse(schemaConfig);
        assertPrimaryKey(tableSchema);
        assertConstraintKey(tableSchema);
        assertColumn(tableSchema);
    }

    @Test
    void parseField() throws FileNotFoundException, URISyntaxException {
        Config config = getConfig(fieldConfig);
        Config schemaConfig = config.getConfig(TableSchemaOptions.SCHEMA.key());
        ConfigParser configParser = new ConfigParser();
        TableSchema tableSchema = configParser.parse(schemaConfig);
        assertPrimaryKey(tableSchema);
        assertConstraintKey(tableSchema);
        assertColumn(tableSchema);
    }

    private void assertPrimaryKey(TableSchema tableSchema) {
        PrimaryKey primaryKey = tableSchema.getPrimaryKey();
        Assertions.assertEquals("id", primaryKey.getPrimaryKey());
        Assertions.assertEquals("id", primaryKey.getColumnNames().get(0));
    }

    private void assertConstraintKey(TableSchema tableSchema) {
        List<ConstraintKey> constraintKeys = tableSchema.getConstraintKeys();
        ConstraintKey constraintKey = constraintKeys.get(0);
        Assertions.assertEquals("id_index", constraintKey.getConstraintName());
        Assertions.assertEquals(
                ConstraintKey.ConstraintType.KEY, constraintKey.getConstraintType());
        Assertions.assertEquals("id", constraintKey.getColumnNames().get(0).getColumnName());
        Assertions.assertEquals(
                ConstraintKey.ColumnSortType.ASC,
                constraintKey.getColumnNames().get(0).getSortType());
    }

    private void assertColumn(TableSchema tableSchema) {
        List<Column> columns = tableSchema.getColumns();
        Assertions.assertEquals("id", columns.get(0).getName());
        Assertions.assertEquals("map", columns.get(1).getName());
        Assertions.assertEquals("map_array", columns.get(2).getName());
        Assertions.assertEquals("array", columns.get(3).getName());
        Assertions.assertEquals("string", columns.get(4).getName());
    }
}
