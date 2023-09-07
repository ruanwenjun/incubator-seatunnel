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

import io.debezium.connector.dameng.DamengConnectorConfig;
import io.debezium.connector.dameng.DamengDatabaseSchema;
import io.debezium.util.Strings;

import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class LogMinerQueryBuilder {
    private static final String LOGMNR_CONTENTS_VIEW = "V$LOGMNR_CONTENTS";

    public static String build(
            DamengConnectorConfig connectorConfig, DamengDatabaseSchema schema, String userName) {
        final StringBuilder query = new StringBuilder(1024);
        query.append(
                "SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, ");
        query.append("USERNAME, ROW_ID, ROLL_BACK, RS_ID ");
        query.append("FROM ").append(LOGMNR_CONTENTS_VIEW).append(" ");

        // These bind parameters will be bound when the query is executed by the caller.
        query.append("WHERE SCN > ? AND SCN <= ? ");

        query.append("AND (");

        // Always include START, COMMIT, MISSING_SCN, and ROLLBACK operations
        query.append("(OPERATION_CODE IN (6,7,34,36)");

        if (!schema.storeOnlyCapturedTables()) {
            // In this mode, the connector will always be fed DDL operations for all tables even if
            // they
            // are not part of the inclusion/exclusion lists.
            query.append(" OR ").append(buildDdlPredicate(userName)).append(" ");
            // Insert, Update, Delete, SelectLob, LobWrite, LobTrim, and LobErase
            if (connectorConfig.isLobEnabled()) {
                query.append(") OR (OPERATION_CODE IN (1,2,3,9,10,11,28) ");
            } else {
                query.append(") OR (OPERATION_CODE IN (1,2,3) ");
            }
        } else {
            // Insert, Update, Delete, SelectLob, LobWrite, LobTrim, and LobErase
            if (connectorConfig.isLobEnabled()) {
                query.append(") OR ((OPERATION_CODE IN (1,2,3,9,10,11,28) ");
            } else {
                query.append(") OR ((OPERATION_CODE IN (1,2,3) ");
            }
            // In this mode, the connector will filter DDL operations based on the table
            // inclusion/exclusion lists
            query.append("OR ").append(buildDdlPredicate(userName)).append(") ");
        }

        // There are some common schemas that we automatically ignore when building the runtime
        // Filter
        // predicates and we put that same list of schemas here and apply those in the generated
        // SQL.
        if (!DamengConnectorConfig.EXCLUDED_SCHEMAS.isEmpty()) {
            query.append("AND SEG_OWNER NOT IN (");
            for (Iterator<String> i = DamengConnectorConfig.EXCLUDED_SCHEMAS.iterator();
                    i.hasNext(); ) {
                String excludedSchema = i.next();
                query.append("'").append(excludedSchema.toUpperCase()).append("'");
                if (i.hasNext()) {
                    query.append(",");
                }
            }
            query.append(") ");
        }

        String schemaPredicate = buildSchemaPredicate(connectorConfig);
        if (!Strings.isNullOrEmpty(schemaPredicate)) {
            query.append("AND ").append(schemaPredicate).append(" ");
        }

        String tablePredicate = buildTablePredicate(connectorConfig);
        if (!Strings.isNullOrEmpty(tablePredicate)) {
            query.append("AND ").append(tablePredicate).append(" ");
        }

        query.append("))");
        query.append(" ORDER BY SCN ASC");

        return query.toString();
    }

    /**
     * Builds a common SQL fragment used to obtain DDL operations via LogMiner.
     *
     * @param userName jdbc connection username, should not be {@code null}
     * @return predicate that can be used to obtain DDL operations via LogMiner
     */
    private static String buildDdlPredicate(String userName) {
        final StringBuilder predicate = new StringBuilder(256);
        predicate.append("(OPERATION_CODE = 5 ");
        predicate
                .append("AND USERNAME NOT IN ('SYS','SYSTEM','")
                .append(userName.toUpperCase())
                .append("') ");
        // predicate.append("AND INFO NOT LIKE 'INTERNAL DDL%' ");
        // predicate.append("AND (TABLE_NAME IS NULL OR TABLE_NAME NOT LIKE 'ORA_TEMP_%'))");
        predicate.append(")");
        return predicate.toString();
    }

    /**
     * Builds a SQL predicate of what schemas to include/exclude based on the connector
     * configuration.
     *
     * @param connectorConfig connector configuration, should not be {@code null}
     * @return SQL predicate to filter results based on schema include/exclude configurations
     */
    private static String buildSchemaPredicate(DamengConnectorConfig connectorConfig) {
        StringBuilder predicate = new StringBuilder();
        if (Strings.isNullOrEmpty(connectorConfig.schemaIncludeList())) {
            if (!Strings.isNullOrEmpty(connectorConfig.schemaExcludeList())) {
                List<Pattern> patterns =
                        Strings.listOfRegex(connectorConfig.schemaExcludeList(), 0);
                predicate
                        .append("(")
                        .append(listOfPatternsToSql(patterns, "SEG_OWNER", true))
                        .append(")");
            }
        } else {
            List<Pattern> patterns = Strings.listOfRegex(connectorConfig.schemaIncludeList(), 0);
            predicate
                    .append("(")
                    .append(listOfPatternsToSql(patterns, "SEG_OWNER", false))
                    .append(")");
        }
        return predicate.toString();
    }

    /**
     * Builds a SQL predicate of what tables to include/exclude based on the connector
     * configuration.
     *
     * @param connectorConfig connector configuration, should not be {@code null}
     * @return SQL predicate to filter results based on table include/exclude configuration
     */
    private static String buildTablePredicate(DamengConnectorConfig connectorConfig) {
        StringBuilder predicate = new StringBuilder();
        if (Strings.isNullOrEmpty(connectorConfig.tableIncludeList())) {
            if (!Strings.isNullOrEmpty(connectorConfig.tableExcludeList())) {
                List<Pattern> patterns = Strings.listOfRegex(connectorConfig.tableExcludeList(), 0);
                predicate
                        .append("(")
                        .append(
                                listOfPatternsToSql(
                                        patterns, "SEG_OWNER || '.' || TABLE_NAME", true))
                        .append(")");
            }
        } else {
            List<Pattern> patterns = Strings.listOfRegex(connectorConfig.tableIncludeList(), 0);
            predicate
                    .append("(")
                    .append(listOfPatternsToSql(patterns, "SEG_OWNER || '.' || TABLE_NAME", false))
                    .append(")");
        }
        return predicate.toString();
    }

    /**
     * Takes a list of reg-ex patterns and builds an Oracle-specific predicate using {@code
     * REGEXP_LIKE} in order to take the connector configuration include/exclude lists and assemble
     * them as SQL predicates.
     *
     * @param patterns list of each individual include/exclude reg-ex patterns from connector
     *     configuration
     * @param columnName the column in which the reg-ex patterns are to be applied against
     * @param inclusion should be {@code true} when passing inclusion patterns, {@code false}
     *     otherwise
     * @return
     */
    private static String listOfPatternsToSql(
            List<Pattern> patterns, String columnName, boolean inclusion) {
        StringBuilder predicate = new StringBuilder();
        for (Iterator<Pattern> i = patterns.iterator(); i.hasNext(); ) {
            Pattern pattern = i.next();
            if (inclusion) {
                predicate.append("NOT ");
            }
            // NOTE: The REGEXP_LIKE operator was added in Oracle 10g (10.1.0.0.0)
            final String text = resolveRegExpLikePattern(pattern);
            predicate
                    .append("REGEXP_LIKE(")
                    .append(columnName)
                    .append(",'")
                    .append(text)
                    .append("','i')");
            if (i.hasNext()) {
                // Exclude lists imply combining them via AND, Include lists imply combining them
                // via OR?
                predicate.append(inclusion ? " AND " : " OR ");
            }
        }
        return predicate.toString();
    }

    /**
     * The {@code REGEXP_LIKE} Oracle operator acts identical to the {@code LIKE} operator.
     * Internally, it prepends and appends a "%" qualifier. The include/exclude lists are meant to
     * be explicit in that they have an implied "^" and "$" qualifier for start/end so that the LIKE
     * operation does not mistakently filter "DEBEZIUM2" when using the reg-ex of "DEBEZIUM".
     *
     * @param pattern the pattern to be analyzed, should not be {@code null}
     * @return the adjusted predicate, if necessary and doesn't already explicitly specify "^" or
     *     "$"
     */
    private static String resolveRegExpLikePattern(Pattern pattern) {
        String text = pattern.pattern();
        if (!text.startsWith("^")) {
            text = "^" + text;
        }
        if (!text.endsWith("$")) {
            text += "$";
        }
        return text;
    }
}
