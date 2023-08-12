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

import com.informix.jdbcx.IfxDataSource;
import com.informix.stream.api.IfmxStreamRecord;
import com.informix.stream.cdc.IfxCDCEngine;
import com.informix.stream.impl.IfxStreamException;
import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class InformixCDCEngine {

    private static final String URL_PATTERN = "jdbc:informix-sqli://%s:%s/syscdcv1";

    private String hostname;
    private String port;
    private String user;
    private String password;

    private long lsn;
    private int timeOut = 5;
    private boolean inited = false;

    private IfxDataSource ds;

    private IfxCDCEngine cdcEngine;

    private final Map<Integer, TableId> tableIdByLabelId;

    public InformixCDCEngine(Configuration config) {
        hostname = config.getString(JdbcConfiguration.HOSTNAME);
        port = config.getString(JdbcConfiguration.PORT);
        user = config.getString(JdbcConfiguration.USER);
        password = config.getString(JdbcConfiguration.PASSWORD);
        tableIdByLabelId = new HashMap<>();
    }

    public void init(InformixDatabaseSchema schema) throws InterruptedException {
        try {
            String url = InformixCDCEngine.genURLStr(hostname, port);
            this.cdcEngine = this.buildCDCEngine(url, user, password, schema);

            this.cdcEngine.init();
            this.inited = true;
        } catch (SQLException ex) {
            log.error("Caught SQLException", ex);
            throw new InterruptedException("Failed while while initialize CDC Engine");
        } catch (IfxStreamException ex) {
            log.error("Caught IfxStreamException", ex);
            throw new InterruptedException("Failed while while initialize CDC Engine");
        }
    }

    public IfxCDCEngine buildCDCEngine(
            String url, String user, String password, InformixDatabaseSchema schema)
            throws SQLException {
        ds = new IfxDataSource(url);
        ds.setUser(user);
        ds.setPassword(password);

        IfxCDCEngine.Builder builder = new IfxCDCEngine.Builder(ds);

        builder.buffer(819200);
        builder.timeout(this.timeOut);

        schema.tableIds()
                .forEach(
                        (TableId tid) -> {
                            String tableName =
                                    tid.catalog() + ":" + tid.schema() + "." + tid.table();
                            String[] colNames =
                                    schema.tableFor(tid).columns().stream()
                                            .map(Column::name)
                                            .toArray(String[]::new);
                            builder.watchTable(tableName, colNames);
                        });

        if (this.lsn > 0) {
            builder.sequenceId(this.lsn);
        }

        /*
         * Build Map of Label_id to TableId.
         */
        for (IfxCDCEngine.IfmxWatchedTable tbl : builder.getWatchedTables()) {
            TableId tid =
                    new TableId(tbl.getDatabaseName(), tbl.getNamespace(), tbl.getTableName());
            tableIdByLabelId.put(tbl.getLabel(), tid);
            log.info("Added WatchedTable : label={} -> tableId={}", tbl.getLabel(), tid);
        }

        return builder.build();
    }

    public IfxDataSource getDs() {
        return ds;
    }

    public void close() {
        try {
            this.cdcEngine.close();
        } catch (IfxStreamException e) {
            log.error("Caught a exception while closing cdcEngine", e);
        }
    }

    public void setStartLsn(long fromLsn) {
        this.lsn = fromLsn;
    }

    public Map<Integer, TableId> convertLabel2TableId() {
        return this.tableIdByLabelId;
    }

    public void stream(StreamHandler streamHandler)
            throws InterruptedException, SQLException, IfxStreamException {
        while (streamHandler.accept(cdcEngine.getRecord())) {}
    }

    public IfxCDCEngine getCdcEngine() {
        return cdcEngine;
    }

    public static String genURLStr(String host, String port) {
        return String.format(URL_PATTERN, host, port);
    }

    public static InformixCDCEngine build(Configuration config) {
        return new InformixCDCEngine(config);
    }

    public interface StreamHandler {

        boolean accept(IfmxStreamRecord record)
                throws SQLException, IfxStreamException, InterruptedException;
    }
}
