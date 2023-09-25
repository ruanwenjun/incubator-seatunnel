package org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.MultipleRowType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.config.Oracle9BridgeSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@AutoService(Factory.class)
public class Oracle9BridgeIncrementalSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return Oracle9BridgeIncrementalSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return JdbcSourceOptions.getBaseRule()
                .required(
                        JdbcSourceOptions.USERNAME,
                        JdbcSourceOptions.PASSWORD,
                        CatalogOptions.TABLE_NAMES,
                        JdbcCatalogOptions.BASE_URL,
                        Oracle9BridgeSourceOptions.ORACLE9BRIDGE_AGENT_HOST,
                        Oracle9BridgeSourceOptions.ORACLE9BRIDGE_AGENT_PORT)
                .optional(
                        JdbcSourceOptions.PORT,
                        JdbcSourceOptions.DATABASE_NAMES,
                        JdbcSourceOptions.SERVER_TIME_ZONE,
                        JdbcSourceOptions.CONNECT_TIMEOUT_MS,
                        JdbcSourceOptions.CONNECT_MAX_RETRIES,
                        JdbcSourceOptions.CONNECTION_POOL_SIZE)
                .optional(
                        Oracle9BridgeSourceOptions.STARTUP_MODE,
                        Oracle9BridgeSourceOptions.STOP_MODE)
                .build();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> {
            SeaTunnelDataType<SeaTunnelRow> dataType;
            List<CatalogTable> catalogTables =
                    CatalogTableUtil.getCatalogTablesFromConfig(
                            "Oracle", context.getOptions(), context.getClassLoader());
            if (catalogTables.size() == 1) {
                dataType = catalogTables.get(0).getTableSchema().toPhysicalRowDataType();
            } else {
                Map<String, SeaTunnelRowType> rowTypeMap = new HashMap<>();
                for (CatalogTable catalogTable : catalogTables) {
                    rowTypeMap.put(
                            catalogTable.getTableId().toTablePath().toString(),
                            catalogTable.getTableSchema().toPhysicalRowDataType());
                }
                dataType = new MultipleRowType(rowTypeMap);
            }
            return (SeaTunnelSource<T, SplitT, StateT>)
                    new Oracle9BridgeIncrementalSource<>(context.getOptions(), dataType);
        };
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return Oracle9BridgeIncrementalSource.class;
    }
}
