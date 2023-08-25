package org.apache.seatunnel.connectors.dolphindb.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig;

import com.google.auto.service.AutoService;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;

import static org.apache.seatunnel.api.table.catalog.CatalogTableUtil.SCHEMA;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.ADDRESS;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.BATCH_SIZE;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.COMPRESS_TYPE;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.CUSTOM_SQL;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.DATABASE;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.KEY_COL_NAMES;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.PARTITION_COLUMN;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.PASSWORD;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.SAVE_MODE;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.SAVE_MODE_CREATE_TEMPLATE;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.TABLE;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.THROTTLE;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.USER;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.USE_SSL;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.WRITE_MODE;

@AutoService(Factory.class)
public class DolphinDBSinkFactory implements TableSinkFactory<SeaTunnelRow, Void, Void, Void> {

    @Override
    public String factoryIdentifier() {
        return DolphinDBConfig.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(ADDRESS, USER, PASSWORD, DATABASE, TABLE)
                .optional(
                        USE_SSL,
                        SCHEMA,
                        BATCH_SIZE,
                        THROTTLE,
                        PARTITION_COLUMN,
                        WRITE_MODE,
                        COMPRESS_TYPE,
                        SAVE_MODE,
                        CUSTOM_SQL,
                        SAVE_MODE_CREATE_TEMPLATE)
                .conditional(WRITE_MODE, MultithreadedTableWriter.Mode.M_Upsert, KEY_COL_NAMES)
                .conditional(SAVE_MODE, DataSaveMode.CUSTOM_PROCESSING, CUSTOM_SQL)
                .build();
    }

    @Override
    public TableSink<SeaTunnelRow, Void, Void, Void> createSink(TableFactoryContext context) {
        return () -> new DolphinDBSink(context.getCatalogTable(), context.getOptions());
    }

    public Class<? extends SeaTunnelSink<SeaTunnelRow, Void, Void, Void>> getSinkClass() {
        return DolphinDBSink.class;
    }
}
