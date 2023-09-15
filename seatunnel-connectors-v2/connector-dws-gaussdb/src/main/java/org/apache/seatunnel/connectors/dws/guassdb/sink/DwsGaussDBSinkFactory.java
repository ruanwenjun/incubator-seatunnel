package org.apache.seatunnel.connectors.dws.guassdb.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.dws.guassdb.config.BaseDwsGaussDBOption;
import org.apache.seatunnel.connectors.dws.guassdb.config.DwsGaussDBConfig;
import org.apache.seatunnel.connectors.dws.guassdb.sink.commit.DwsGaussDBSinkAggregatedCommitInfo;
import org.apache.seatunnel.connectors.dws.guassdb.sink.commit.DwsGaussDBSinkCommitInfo;
import org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption;
import org.apache.seatunnel.connectors.dws.guassdb.sink.state.DwsGaussDBSinkState;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.api.table.catalog.CatalogTableUtil.SCHEMA;
import static org.apache.seatunnel.connectors.dws.guassdb.config.BaseDwsGaussDBOption.DATABASE;
import static org.apache.seatunnel.connectors.dws.guassdb.config.BaseDwsGaussDBOption.PRIMARY_KEY;
import static org.apache.seatunnel.connectors.dws.guassdb.config.BaseDwsGaussDBOption.TABLE;
import static org.apache.seatunnel.connectors.dws.guassdb.config.BaseDwsGaussDBOption.TABLE_PREFIX;
import static org.apache.seatunnel.connectors.dws.guassdb.config.BaseDwsGaussDBOption.TABLE_SUFFIX;

@AutoService(Factory.class)
public class DwsGaussDBSinkFactory
        implements TableSinkFactory<
                SeaTunnelRow,
                DwsGaussDBSinkState,
                DwsGaussDBSinkCommitInfo,
                DwsGaussDBSinkAggregatedCommitInfo> {
    @Override
    public String factoryIdentifier() {
        return DwsGaussDBConfig.CONNECTOR_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return new DwsGaussDBSinkOption().getOptionRule();
    }

    @Override
    public TableSink<
                    SeaTunnelRow,
                    DwsGaussDBSinkState,
                    DwsGaussDBSinkCommitInfo,
                    DwsGaussDBSinkAggregatedCommitInfo>
            createSink(TableFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        Map<String, String> catalogOptions = config.get(CatalogOptions.CATALOG_OPTIONS);
        if (config.getOptional(TABLE).isPresent()) {
            catalogOptions.put(TABLE.key(), config.get(TABLE));
            catalogOptions.put(PRIMARY_KEY.key(), config.get(PRIMARY_KEY));
        } else {
            String prefix = catalogOptions.get(TABLE_PREFIX.key());
            String suffix = catalogOptions.get(TABLE_SUFFIX.key());
            if (StringUtils.isNotEmpty(prefix) || StringUtils.isNotEmpty(suffix)) {
                TableIdentifier tableId = catalogTable.getTableId();
                String tableName =
                        StringUtils.isNotEmpty(prefix)
                                ? prefix + tableId.getTableName()
                                : tableId.getTableName();
                tableName = StringUtils.isNotEmpty(suffix) ? tableName + suffix : tableName;
                TableIdentifier newTableId =
                        TableIdentifier.of(
                                tableId.getCatalogName(),
                                tableId.getDatabaseName(),
                                tableId.getSchemaName(),
                                tableName);
                catalogTable =
                        CatalogTable.of(
                                newTableId,
                                catalogTable.getTableSchema(),
                                catalogTable.getOptions(),
                                catalogTable.getPartitionKeys(),
                                catalogTable.getCatalogName());
            }
            Map<String, String> map = config.toMap();
            if (StringUtils.isNotBlank(catalogOptions.get(SCHEMA.key()))) {
                map.put(
                        TABLE.key(),
                        catalogOptions.get(SCHEMA.key())
                                + "."
                                + catalogTable.getTableId().getTableName());
            } else if (StringUtils.isNotBlank(catalogTable.getTableId().getSchemaName())) {
                map.put(
                        TABLE.key(),
                        catalogTable.getTableId().getSchemaName()
                                + "."
                                + catalogTable.getTableId().getTableName());
            } else {
                map.put(TABLE.key(), catalogTable.getTableId().getTableName());
            }

            PrimaryKey primaryKey = catalogTable.getTableSchema().getPrimaryKey();
            if (primaryKey != null && !CollectionUtils.isEmpty(primaryKey.getColumnNames())) {
                map.put(PRIMARY_KEY.key(), String.join(",", primaryKey.getColumnNames()));
            }
            config = ReadonlyConfig.fromMap(new HashMap<>(map));
        }
        final ReadonlyConfig options = config;
        String[] split = config.get(TABLE).split("\\.");
        String schemaName = null;
        String tableName = null;
        if (split.length == 2) {
            schemaName = split[0];
            tableName = split[1];
        } else {
            tableName = split[0];
        }
        if (StringUtils.isNotEmpty(config.get(BaseDwsGaussDBOption.DATABASE_SCHEMA))) {
            schemaName = config.get(BaseDwsGaussDBOption.DATABASE_SCHEMA);
        }
        TableIdentifier tableIdentifier =
                TableIdentifier.of(
                        catalogTable.getCatalogName(),
                        options.get(DATABASE),
                        schemaName,
                        tableName);
        CatalogTable finalCatalogTable = CatalogTable.of(tableIdentifier, catalogTable);

        return () -> new DwsGaussDBSink(options, finalCatalogTable);
    }
}
