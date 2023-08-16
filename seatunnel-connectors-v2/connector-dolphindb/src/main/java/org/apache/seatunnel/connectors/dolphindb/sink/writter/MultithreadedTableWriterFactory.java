package org.apache.seatunnel.connectors.dolphindb.sink.writter;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig;

import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;

import java.util.List;

import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.BATCH_SIZE;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.KEY_COL_NAMES;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.THROTTLE;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.USE_SSL;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.WRITE_MODE;

public class MultithreadedTableWriterFactory {

    public static MultithreadedTableWriter createMultithreadedTableWriter(Config pluginConfig)
            throws Exception {
        List<String> addresses = pluginConfig.getStringList(DolphinDBConfig.ADDRESS.key());
        String address = addresses.get(0);
        String host = address.substring(0, address.lastIndexOf(":"));
        int port = Integer.parseInt(address.substring(address.lastIndexOf(":") + 1));

        int[] compressType = null;
        if (pluginConfig.hasPath(DolphinDBConfig.COMPRESS_TYPE.key())) {
            List<Integer> compressTypeList =
                    pluginConfig.getIntList(DolphinDBConfig.COMPRESS_TYPE.key());
            compressType = compressTypeList.stream().mapToInt(Integer::intValue).toArray();
        }
        String partitionColumn = null;
        if (pluginConfig.hasPath(DolphinDBConfig.PARTITION_COLUMN.key())) {
            partitionColumn = pluginConfig.getString(DolphinDBConfig.PARTITION_COLUMN.key());
        }
        MultithreadedTableWriter.Mode writeMode = MultithreadedTableWriter.Mode.M_Upsert;
        if (pluginConfig.hasPath(WRITE_MODE.key())) {
            writeMode =
                    MultithreadedTableWriter.Mode.valueOf(pluginConfig.getString(WRITE_MODE.key()));
        }
        String[] pModelOption = null;
        if (pluginConfig.hasPath(KEY_COL_NAMES.key())) {
            pModelOption = pluginConfig.getStringList(KEY_COL_NAMES.key()).toArray(new String[0]);
        }

        return new MultithreadedTableWriter(
                host,
                port,
                pluginConfig.getString(DolphinDBConfig.USER.key()),
                pluginConfig.getString(DolphinDBConfig.PASSWORD.key()),
                pluginConfig.getString(DolphinDBConfig.DATABASE.key()),
                pluginConfig.getString(DolphinDBConfig.TABLE.key()),
                TypesafeConfigUtils.getConfig(pluginConfig, USE_SSL.key(), USE_SSL.defaultValue()),
                address.length() > 1,
                addresses.toArray(new String[0]),
                TypesafeConfigUtils.getConfig(
                        pluginConfig, BATCH_SIZE.key(), BATCH_SIZE.defaultValue()),
                TypesafeConfigUtils.getConfig(
                        pluginConfig, THROTTLE.key(), THROTTLE.defaultValue()),
                1,
                partitionColumn,
                compressType,
                writeMode,
                pModelOption);
    }
}
