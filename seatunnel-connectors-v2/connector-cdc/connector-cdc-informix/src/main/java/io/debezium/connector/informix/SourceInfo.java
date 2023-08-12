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

import com.informix.jdbc.IfxColumnInfo;
import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.relational.TableId;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@NotThreadSafe
@Getter
@Setter
public class SourceInfo extends BaseSourceInfo {
    public static String CHANGE_LSN_KEY = "change_lsn";
    public static String COMMIT_LSN_KEY = "commit_lsn";
    public static String BEGIN_LSN_KEY = "begin_ls";
    public static String TX_ID = "tx_id";
    public static String DEBEZIUM_VERSION_KEY = AbstractSourceInfo.DEBEZIUM_VERSION_KEY;
    public static String DEBEZIUM_CONNECTOR_KEY = AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY;
    public static String SERVER_NAME_KEY = AbstractSourceInfo.SERVER_NAME_KEY;
    public static String TIMESTAMP_KEY = AbstractSourceInfo.TIMESTAMP_KEY;
    public static String SNAPSHOT_KEY = AbstractSourceInfo.SNAPSHOT_KEY;
    public static String DATABASE_NAME_KEY = AbstractSourceInfo.DATABASE_NAME_KEY;
    public static String SCHEMA_NAME_KEY = AbstractSourceInfo.SCHEMA_NAME_KEY;
    public static String TABLE_NAME_KEY = AbstractSourceInfo.TABLE_NAME_KEY;
    public static String COLLECTION_NAME_KEY = AbstractSourceInfo.COLLECTION_NAME_KEY;

    private Long changeLsn = -1L;
    private Long commitLsn = -1L;
    private Long beginLsn = -1L;
    private Long txId = -1L;
    private Instant sourceTime = null;
    private Set<TableId> tableIds;
    private String databaseName;
    private List<IfxColumnInfo> streamMetadata;

    public SourceInfo(InformixConnectorConfig config) {
        super(config);
        this.databaseName = config.getDatabaseName();
    }

    /** @param changeLsn - LSN of the change in the database log */
    public void setChangeLsn(Long changeLsn) {
        this.changeLsn = changeLsn;
    }

    public Long getChangeLsn() {
        return changeLsn;
    }

    /** @param beginLsn - LSN of the { @code COMMIT} of the transaction whose part the change is */
    public void setBeginLsn(Long beginLsn) {
        this.beginLsn = beginLsn;
    }

    public Long getBeginLsn() {
        return beginLsn;
    }

    /** @param txId - LSN of the { @code COMMIT} of the transaction whose part the change is */
    public void setTxId(Long txId) {
        this.txId = txId;
    }

    public Long getTxId() {
        return this.txId;
    }

    /** @param commitLsn - LSN of the { @code COMMIT} of the transaction whose part the change is */
    public void setCommitLsn(Long commitLsn) {
        this.commitLsn = commitLsn;
    }

    public Long getCommitLsn() {
        return commitLsn;
    }

    /** @param instant a time at which the transaction commit was executed */
    public void setSourceTime(Instant instant) {
        this.sourceTime = instant;
    }

    public void setColumns(List<IfxColumnInfo> cols) {
        this.streamMetadata = cols;
    }

    @Override
    public String toString() {
        return "SourceInfo ["
                + "serverName="
                + serverName()
                + ","
                + " changeLsn="
                + changeLsn
                + ","
                + " commitLsn="
                + commitLsn
                + ","
                + " snapshot="
                + snapshotRecord
                + ","
                + " sourceTime="
                + sourceTime
                + "]";
    }

    public void tableEvent(Set<TableId> tableIds) {
        this.tableIds = new HashSet<>(tableIds);
    }

    public void tableEvent(TableId tableId) {
        this.tableIds = Collections.singleton(tableId);
    }

    public String tableSchema() {
        return tableIds.isEmpty()
                ? null
                : tableIds.stream()
                        .filter(Objects::nonNull)
                        .map(TableId::schema)
                        .distinct()
                        .collect(Collectors.joining(","));
    }

    public String table() {
        return tableIds.isEmpty()
                ? null
                : tableIds.stream()
                        .filter(Objects::nonNull)
                        .map(TableId::table)
                        .collect(Collectors.joining(","));
    }

    /** @return timestamp of the event */
    @Override
    protected Instant timestamp() {
        return sourceTime;
    }

    /** @return name of the database */
    @Override
    protected String database() {
        return databaseName;
    }
}
