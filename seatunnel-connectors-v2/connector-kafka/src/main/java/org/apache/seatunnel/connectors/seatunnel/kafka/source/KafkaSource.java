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

package org.apache.seatunnel.connectors.seatunnel.kafka.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.MessageFormat;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.MessageFormatErrorHandleWay;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.StartMode;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorException;
import org.apache.seatunnel.connectors.seatunnel.kafka.kingbase.KingbaseJsonDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSourceState;
import org.apache.seatunnel.format.compatible.kafka.connect.json.CompatibleKafkaConnectDeserializationSchema;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.json.canal.CanalJsonDeserializationSchema;
import org.apache.seatunnel.format.json.debezium.DebeziumJsonDeserializationSchema;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;
import org.apache.seatunnel.format.text.TextDeserializationSchema;

import org.apache.kafka.common.TopicPartition;

import com.google.auto.service.AutoService;
import com.google.common.collect.Lists;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.BOOTSTRAP_SERVERS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.COMMIT_ON_CHECKPOINT;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.CONSUMER_GROUP;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.DEBEZIUM_RECORD_INCLUDE_SCHEMA;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.KAFKA_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.MESSAGE_FORMAT_ERROR_HANDLE_WAY_OPTION;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.PATTERN;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.START_MODE;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.START_MODE_OFFSETS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.START_MODE_TIMESTAMP;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.TOPIC;

@AutoService(SeaTunnelSource.class)
@NoArgsConstructor
public class KafkaSource
        implements SeaTunnelSource<SeaTunnelRow, KafkaSourceSplit, KafkaSourceState>,
                SupportParallelism {

    private final ConsumerMetadata metadata = new ConsumerMetadata();
    private DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private List<CatalogTable> catalogTables;
    private JobContext jobContext;
    private long discoveryIntervalMillis;
    private MessageFormatErrorHandleWay messageFormatErrorHandleWay =
            MessageFormatErrorHandleWay.FAIL;

    public KafkaSource(ReadonlyConfig option, List<CatalogTable> catalogTables) {
        this.catalogTables = catalogTables;

        this.metadata.setTopic(option.get(TOPIC));
        this.metadata.setPattern(option.get(PATTERN));

        this.metadata.setBootstrapServers(option.get(BOOTSTRAP_SERVERS));
        this.metadata.setProperties(new Properties());
        this.metadata.setConsumerGroup(option.get(CONSUMER_GROUP));
        this.metadata.setCommitOnCheckpoint(option.get(COMMIT_ON_CHECKPOINT));

        StartMode startMode = option.get(START_MODE);
        this.metadata.setStartMode(startMode);
        switch (startMode) {
            case TIMESTAMP:
                long startOffsetsTimestamp = option.get(START_MODE_TIMESTAMP);
                long currentTimestamp = System.currentTimeMillis();
                if (startOffsetsTimestamp < 0 || startOffsetsTimestamp > currentTimestamp) {
                    throw new IllegalArgumentException(
                            "start_mode.timestamp The value is smaller than 0 or smaller than the current time");
                }
                this.metadata.setStartOffsetsTimestamp(startOffsetsTimestamp);
                break;
            case SPECIFIC_OFFSETS:
                Map<String, String> offsets = option.get(START_MODE_OFFSETS);
                if (offsets.isEmpty()) {
                    throw new IllegalArgumentException(
                            "start mode is "
                                    + StartMode.SPECIFIC_OFFSETS
                                    + "but no specific offsets were specified.");
                }
                Map<TopicPartition, Long> specificStartOffsets = new HashMap<>();
                offsets.keySet()
                        .forEach(
                                key -> {
                                    int splitIndex = key.lastIndexOf("-");
                                    String topic = key.substring(0, splitIndex);
                                    String partition = key.substring(splitIndex + 1);
                                    long offset = Long.parseLong(offsets.get(key));
                                    TopicPartition topicPartition =
                                            new TopicPartition(topic, Integer.parseInt(partition));
                                    specificStartOffsets.put(topicPartition, offset);
                                });
                this.metadata.setSpecificStartOffsets(specificStartOffsets);
                break;
            default:
                break;
        }

        this.discoveryIntervalMillis = option.get(KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS);

        if (option.get(KAFKA_CONFIG) != null) {
            option.get(KAFKA_CONFIG)
                    .forEach((key, value) -> this.metadata.getProperties().put(key, value));
        }

        MessageFormatErrorHandleWay formatErrorWayOption =
                option.get(MESSAGE_FORMAT_ERROR_HANDLE_WAY_OPTION);
        switch (formatErrorWayOption) {
            case FAIL:
            case SKIP:
                this.messageFormatErrorHandleWay = formatErrorWayOption;
                break;
            default:
                break;
        }

        setDeserialization(option, catalogTables);
    }

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public String getPluginName() {
        return org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.CONNECTOR_IDENTITY;
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        CheckResult result =
                CheckConfigUtil.checkAllExists(config, TOPIC.key(), BOOTSTRAP_SERVERS.key());
        // TODO remove
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return CatalogTableUtil.toSeaTunnelRowType(catalogTables);
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Lists.newArrayList(catalogTables);
    }

    @Override
    public SourceReader<SeaTunnelRow, KafkaSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new KafkaSourceReader(
                this.metadata, deserializationSchema, readerContext, messageFormatErrorHandleWay);
    }

    @Override
    public SourceSplitEnumerator<KafkaSourceSplit, KafkaSourceState> createEnumerator(
            SourceSplitEnumerator.Context<KafkaSourceSplit> enumeratorContext) throws Exception {
        return new KafkaSourceSplitEnumerator(
                this.metadata, enumeratorContext, discoveryIntervalMillis);
    }

    @Override
    public SourceSplitEnumerator<KafkaSourceSplit, KafkaSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<KafkaSourceSplit> enumeratorContext,
            KafkaSourceState checkpointState)
            throws Exception {
        return new KafkaSourceSplitEnumerator(
                this.metadata, enumeratorContext, checkpointState, discoveryIntervalMillis);
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    private void setDeserialization(ReadonlyConfig option, List<CatalogTable> catalogTables) {
        deserializationSchema = getDeserializationSchema(option, catalogTables);
    }

    private DeserializationSchema<SeaTunnelRow> getDeserializationSchema(
            ReadonlyConfig option, List<CatalogTable> catalogTables) {
        MessageFormat format = option.get(FORMAT);
        switch (format) {
            case JSON:
                if (catalogTables.size() > 1) {
                    throw new KafkaConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unsupported multi-table format: " + format);
                }
                return new JsonDeserializationSchema(
                        false,
                        false,
                        (SeaTunnelRowType) catalogTables.get(0).getSeaTunnelRowType());
            case TEXT:
                if (catalogTables.size() > 1) {
                    throw new KafkaConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unsupported multi-table format: " + format);
                }
                return TextDeserializationSchema.builder()
                        .seaTunnelRowType(catalogTables.get(0).getSeaTunnelRowType())
                        .delimiter(option.get(FIELD_DELIMITER))
                        .build();
            case CANAL_JSON:
                if (catalogTables.size() > 1) {
                    throw new KafkaConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unsupported multi-table format: " + format);
                }
                return CanalJsonDeserializationSchema.builder(
                                catalogTables.get(0).getSeaTunnelRowType())
                        .setIgnoreParseErrors(true)
                        .build();
            case KINGBASE_JSON:
                return new KingbaseJsonDeserializationSchema(catalogTables);
            case COMPATIBLE_KAFKA_CONNECT_JSON:
                if (catalogTables.size() > 1) {
                    throw new KafkaConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unsupported multi-table format: " + format);
                }
                return new CompatibleKafkaConnectDeserializationSchema(
                        catalogTables.get(0).getSeaTunnelRowType(), option, false, false);
            case DEBEZIUM_JSON:
                if (catalogTables.size() > 1) {
                    throw new KafkaConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unsupported multi-table format: " + format);
                }
                boolean includeSchema = option.get(DEBEZIUM_RECORD_INCLUDE_SCHEMA);
                return new DebeziumJsonDeserializationSchema(
                        catalogTables.get(0).getSeaTunnelRowType(), true, includeSchema);
            default:
                throw new SeaTunnelJsonFormatException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE, "Unsupported format: " + format);
        }
    }
}
