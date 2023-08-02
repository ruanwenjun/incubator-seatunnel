package org.apache.seatunnel.connectors.seatunnel.redshift.handler;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.redshift.RedshiftJdbcClient;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConf;
import org.apache.seatunnel.connectors.seatunnel.redshift.exception.S3RedshiftJdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.redshift.sink.S3RedshiftSQLGenerator;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.SOURCE_ALREADY_HAS_DATA;
import static org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConfig.CUSTOM_SQL;

@Slf4j
public class S3RedshiftSaveModeHandler {

    private final DataSaveMode saveMode;
    private final S3RedshiftConf s3RedshiftConf;
    private final CatalogTable catalogTable;
    private final ReadonlyConfig readonlyConfig;
    private final SeaTunnelRowType seaTunnelRowType;

    public S3RedshiftSaveModeHandler(
            DataSaveMode saveMode,
            S3RedshiftConf s3RedshiftConf,
            CatalogTable catalogTable,
            ReadonlyConfig readonlyConfig,
            SeaTunnelRowType seaTunnelRowType) {
        this.saveMode = saveMode;
        this.s3RedshiftConf = s3RedshiftConf;
        this.catalogTable = catalogTable;
        this.readonlyConfig = readonlyConfig;
        this.seaTunnelRowType = seaTunnelRowType;
    }

    public void doHandleSaveMode() throws SQLException {
        S3RedshiftSQLGenerator sqlGenerator;
        if (catalogTable != null) {
            sqlGenerator = new S3RedshiftSQLGenerator(s3RedshiftConf, catalogTable);
        } else {
            sqlGenerator = new S3RedshiftSQLGenerator(s3RedshiftConf, seaTunnelRowType);
        }
        RedshiftJdbcClient client =
                new RedshiftJdbcClient(
                        s3RedshiftConf.getJdbcUrl(),
                        s3RedshiftConf.getJdbcUser(),
                        s3RedshiftConf.getJdbcPassword(),
                        1);
        try (Connection connection = client.getConnection()) {
            Statement statement = connection.createStatement();
            switch (saveMode) {
                case DROP_SCHEMA:
                    // drop
                    log.info("Drop table sql: {}", sqlGenerator.getCreateTableSQL());
                    statement.execute(sqlGenerator.getDropTableSql());
                    log.info("Drop table sql: {}", sqlGenerator.getDropTemporaryTableSql());
                    statement.execute(sqlGenerator.getDropTemporaryTableSql());
                    // create
                    statement.execute(sqlGenerator.getCreateTableSQL());
                    statement.execute(sqlGenerator.getCreateTemporaryTableSQL());
                    break;
                case KEEP_SCHEMA_DROP_DATA:
                    statement.execute(sqlGenerator.getDropTemporaryTableSql());
                    statement.execute(sqlGenerator.getCreateTemporaryTableSQL());
                    if (existDataForSql(sqlGenerator.generateIsExistTableSql(), statement)) {
                        statement.execute(sqlGenerator.generateCleanTableSql());
                    }
                    break;
                case KEEP_SCHEMA_AND_DATA:
                    connection.createStatement().execute(sqlGenerator.getCreateTableSQL());
                    log.info("Create table sql: {}", sqlGenerator.getCreateTableSQL());
                    if (s3RedshiftConf.isCopyS3FileToTemporaryTableMode()) {
                        statement.execute(sqlGenerator.getDropTemporaryTableSql());
                        statement.execute(sqlGenerator.getCreateTemporaryTableSQL());
                        log.info(
                                "Create temporary table sql: {}",
                                sqlGenerator.getCreateTemporaryTableSQL());
                    }
                    break;
                case CUSTOM_PROCESSING:
                    statement.execute(sqlGenerator.getDropTemporaryTableSql());
                    statement.execute(sqlGenerator.getCreateTableSQL());
                    statement.execute(sqlGenerator.getCreateTemporaryTableSQL());
                    String sql = readonlyConfig.get(CUSTOM_SQL);
                    statement.execute(sql);
                    break;
                case ERROR_WHEN_EXISTS:
                    statement.execute(sqlGenerator.getDropTemporaryTableSql());
                    statement.execute(sqlGenerator.getCreateTemporaryTableSQL());
                    if (existDataForSql(sqlGenerator.getIsExistTableSql(), statement)) {
                        if (existDataForSql(sqlGenerator.getIsExistDataSql(), statement)) {
                            throw new S3RedshiftJdbcConnectorException(
                                    SOURCE_ALREADY_HAS_DATA,
                                    "The target data source already has data");
                        }
                    }
                    statement.execute(sqlGenerator.getCreateTableSQL());
                    break;
            }
        }
    }

    private boolean existDataForSql(String sql, Statement statement) throws SQLException {
        return executeQueryCount(sql, statement) > 0;
    }

    private Integer executeQueryCount(String sql, Statement statement) throws SQLException {
        ResultSet resultSet = statement.executeQuery(sql);
        if (!resultSet.next()) {
            return 0;
        }
        // resultSet.next();
        return resultSet.getInt(1);
    }
}
