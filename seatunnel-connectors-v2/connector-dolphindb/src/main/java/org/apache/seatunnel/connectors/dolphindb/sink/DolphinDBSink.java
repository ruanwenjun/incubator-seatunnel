package org.apache.seatunnel.connectors.dolphindb.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig;
import org.apache.seatunnel.connectors.dolphindb.sink.writter.DolphinDBSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import com.google.auto.service.AutoService;

import java.io.IOException;

@AutoService(SeaTunnelSink.class)
public class DolphinDBSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private Config pluginConfig;

    private SeaTunnelRowType seaTunnelRowType;

    @Override
    public String getPluginName() {
        return DolphinDBConfig.PLUGIN_NAME;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        // check config
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        DolphinDBConfig.ADDRESS.key(),
                        DolphinDBConfig.USER.key(),
                        DolphinDBConfig.PASSWORD.key(),
                        DolphinDBConfig.DATABASE.key(),
                        DolphinDBConfig.TABLE.key());
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SINK, result.getMsg());
        }
        this.pluginConfig = pluginConfig;
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return seaTunnelRowType;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context)
            throws IOException {
        try {
            return new DolphinDBSinkWriter(pluginConfig, seaTunnelRowType);
        } catch (Exception ex) {
            throw new IOException("Create DolphinDBSinkWriter failed", ex);
        }
    }
}
