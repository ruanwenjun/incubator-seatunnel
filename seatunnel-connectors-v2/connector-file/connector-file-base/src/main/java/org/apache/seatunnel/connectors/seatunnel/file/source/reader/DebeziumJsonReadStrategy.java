package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.format.json.debezium.DebeziumJsonDeserializationSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import io.airlift.compress.lzo.LzopCodec;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Slf4j
public class DebeziumJsonReadStrategy extends AbstractReadStrategy {

    private DebeziumJsonDeserializationSchema deserializationSchema;
    private CompressFormat compressFormat = BaseSourceConfig.COMPRESS_CODEC.defaultValue();

    @Override
    public void init(HadoopConf conf) {
        super.init(conf);
        if (pluginConfig.hasPath(BaseSourceConfig.COMPRESS_CODEC.key())) {
            String compressCodec = pluginConfig.getString(BaseSourceConfig.COMPRESS_CODEC.key());
            compressFormat = CompressFormat.valueOf(compressCodec.toUpperCase());
        }
        if (isMergePartition) {
            log.warn("DebeziumJsonReadStrategy not support merge partition");
        }
    }

    @Override
    public void setSeaTunnelRowTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        super.setSeaTunnelRowTypeInfo(seaTunnelRowType);
        deserializationSchema =
                new DebeziumJsonDeserializationSchema(seaTunnelRowType, false, true);
    }

    @Override
    public void read(String path, String tableId, Collector<SeaTunnelRow> output)
            throws IOException, FileConnectorException {
        Configuration conf = getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path filePath = new Path(path);
        InputStream inputStream;
        switch (compressFormat) {
            case LZO:
                LzopCodec lzo = new LzopCodec();
                inputStream = lzo.createInputStream(fs.open(filePath));
                break;
            case NONE:
                inputStream = fs.open(filePath);
                break;
            default:
                log.warn(
                        "Text file does not support this compress type: {}",
                        compressFormat.getCompressCodec());
                inputStream = fs.open(filePath);
                break;
        }

        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            reader.lines()
                    .forEach(
                            line -> {
                                try {
                                    List<SeaTunnelRow> seaTunnelRows =
                                            deserializationSchema.deserializeList(line.getBytes());
                                    for (SeaTunnelRow seaTunnelRow : seaTunnelRows) {
                                        seaTunnelRow.setTableId(tableId);
                                        output.collect(seaTunnelRow);
                                    }
                                } catch (IOException e) {
                                    String errorMsg =
                                            String.format(
                                                    "Read data from this file [%s] failed",
                                                    filePath);
                                    throw new FileConnectorException(
                                            CommonErrorCode.FILE_OPERATION_FAILED, errorMsg);
                                }
                            });
        }
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(HadoopConf hadoopConf, String path)
            throws FileConnectorException {
        throw new FileConnectorException(
                CommonErrorCode.UNSUPPORTED_OPERATION,
                "User must defined schema for json file type");
    }
}
