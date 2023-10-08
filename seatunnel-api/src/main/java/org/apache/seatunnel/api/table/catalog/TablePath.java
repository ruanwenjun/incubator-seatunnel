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

package org.apache.seatunnel.api.table.catalog;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;
import java.util.StringJoiner;

@Getter
@EqualsAndHashCode
@RequiredArgsConstructor
public final class TablePath implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String databaseName;
    private final String schemaName;
    private final String tableName;

    public static TablePath of(String fullName) {
        return of(fullName, false);
    }

    public static TablePath of(String fullName, boolean schemaFirst) {
        String[] paths = fullName.split("\\.");

        if (paths.length == 1) {
            return of(null, paths[0]);
        }
        if (paths.length == 2) {
            if (schemaFirst) {
                return of(null, paths[0], paths[1]);
            }
            return of(paths[0], null, paths[1]);
        }
        if (paths.length == 3) {
            return of(paths[0], paths[1], paths[2]);
        }
        throw new IllegalArgumentException(
                String.format("Cannot get split '%s' to get databaseName and tableName", fullName));
    }

    public static TablePath of(String databaseName, String tableName) {
        return of(databaseName, null, tableName);
    }

    public static TablePath of(String databaseName, String schemaName, String tableName) {
        return new TablePath(databaseName, schemaName, tableName);
    }

    public String getSchemaAndTableName() {
        return String.format("%s.%s", schemaName, tableName);
    }

    public String getSchemaAndTableName(String quote) {
        return String.format("%s%s%s.%s%s%s", quote, schemaName, quote, quote, tableName, quote);
    }

    public String getFullName() {
        StringJoiner joiner = new StringJoiner(".");
        if (databaseName != null) {
            joiner.add(databaseName);
        }
        if (schemaName != null) {
            joiner.add(schemaName);
        }
        joiner.add(tableName);

        return joiner.toString();
    }

    public String getFullNameWithQuoted() {
        return getFullNameWithQuoted("`");
    }

    public String getFullNameWithQuoted(String quote) {
        return getFullNameWithQuoted(quote);
    }

    public String getFullNameWithQuoted(String quoteLeft, String quoteRight) {
        StringJoiner joiner = new StringJoiner(".", quoteLeft, quoteRight);
        if (databaseName != null) {
            joiner.add(databaseName);
        }
        if (schemaName != null) {
            joiner.add(schemaName);
        }
        joiner.add(tableName);

        return joiner.toString();
    }

    @Override
    public String toString() {
        return getFullName();
    }
}
