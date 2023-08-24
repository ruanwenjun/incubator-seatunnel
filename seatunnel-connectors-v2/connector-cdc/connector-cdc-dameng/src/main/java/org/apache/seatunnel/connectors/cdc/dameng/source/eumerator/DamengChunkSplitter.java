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

package org.apache.seatunnel.connectors.cdc.dameng.source.eumerator;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.AbstractJdbcSourceChunkSplitter;
import org.apache.seatunnel.connectors.cdc.dameng.source.DamengDialect;
import org.apache.seatunnel.connectors.cdc.dameng.utils.DamengConncetionUtils;
import org.apache.seatunnel.connectors.cdc.dameng.utils.DamengTypeConverter;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;

@Slf4j
public class DamengChunkSplitter extends AbstractJdbcSourceChunkSplitter {

    public DamengChunkSplitter(JdbcSourceConfig sourceConfig, DamengDialect dialect) {
        super(sourceConfig, dialect);
    }

    @Override
    public Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, String columnName)
            throws SQLException {
        return DamengConncetionUtils.queryMinMax(jdbc, tableId, columnName);
    }

    @Override
    public Object queryMin(
            JdbcConnection jdbc, TableId tableId, String columnName, Object excludedLowerBound)
            throws SQLException {
        return DamengConncetionUtils.queryMin(jdbc, tableId, columnName, excludedLowerBound);
    }

    @Override
    public Object[] sampleDataFromColumn(
            JdbcConnection jdbc, TableId tableId, String columnName, int inverseSamplingRate)
            throws SQLException {
        return DamengConncetionUtils.skipReadAndSortSampleData(
                jdbc, tableId, columnName, inverseSamplingRate);
    }

    @Override
    public Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            String columnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        return DamengConncetionUtils.queryNextChunkMax(
                jdbc, tableId, columnName, chunkSize, includedLowerBound);
    }

    @Override
    public Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId) throws SQLException {
        return DamengConncetionUtils.queryApproximateRowCnt(jdbc, tableId);
    }

    @Override
    public String buildSplitScanQuery(
            TableId tableId,
            SeaTunnelRowType splitKeyType,
            boolean isFirstSplit,
            boolean isLastSplit) {
        return DamengConncetionUtils.buildSplitQuery(
                tableId, splitKeyType, isFirstSplit, isLastSplit);
    }

    @Override
    public SeaTunnelDataType<?> fromDbzColumn(Column splitColumn) {
        return DamengTypeConverter.convert(splitColumn);
    }
}
