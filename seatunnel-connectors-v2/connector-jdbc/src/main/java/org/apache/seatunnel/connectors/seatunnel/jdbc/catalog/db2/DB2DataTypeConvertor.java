package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.db2;

import org.apache.seatunnel.api.table.catalog.DataTypeConvertException;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.apache.commons.collections4.MapUtils;

import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@AutoService(DataTypeConvertor.class)
public class DB2DataTypeConvertor implements DataTypeConvertor<String> {

    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";

    public static final Integer DEFAULT_PRECISION = 38;

    public static final Integer DEFAULT_SCALE = 18;

    public static final String DB2_BOOLEAN = "BOOLEAN";

    public static final String DB2_ROWID = "ROWID";
    public static final String DB2_SMALLINT = "SMALLINT";
    public static final String DB2_INTEGER = "INTEGER";
    public static final String DB2_INT = "INT";
    public static final String DB2_BIGINT = "BIGINT";
    // exact
    public static final String DB2_DECIMAL = "DECIMAL";
    public static final String DB2_DEC = "DEC";
    public static final String DB2_NUMERIC = "NUMERIC";
    public static final String DB2_NUM = "NUM";
    // float
    public static final String DB2_REAL = "REAL";
    public static final String DB2_FLOAT = "FLOAT";
    public static final String DB2_DOUBLE = "DOUBLE";
    public static final String DB2_DOUBLE_PRECISION = "DOUBLE PRECISION";
    public static final String DB2_DECFLOAT = "DECFLOAT";
    // string
    public static final String DB2_CHAR = "CHAR";
    public static final String DB2_CHARACTER = "CHARACTER";
    public static final String DB2_VARCHAR = "VARCHAR";
    public static final String DB2_LONG_VARCHAR = "LONG VARCHAR";
    public static final String DB2_CLOB = "CLOB";
    // graphic
    public static final String DB2_GRAPHIC = "GRAPHIC";
    public static final String DB2_VARGRAPHIC = "VARGRAPHIC";
    public static final String DB2_LONG_VARGRAPHIC = "LONG VARGRAPHIC";
    public static final String DB2_DBCLOB = "DBCLOB";

    // ---------------------------binary---------------------------
    public static final String DB2_BINARY = "BINARY";
    public static final String DB2_VARBINARY = "VARBINARY";

    // ------------------------------time-------------------------
    public static final String DB2_DATE = "DATE";
    public static final String DB2_TIME = "TIME";
    public static final String DB2_TIMESTAMP = "TIMESTAMP";

    // ------------------------------blob-------------------------
    public static final String DB2_BLOB = "BLOB";

    // other
    public static final String DB2_XML = "XML";

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String connectorDataType) {
        return toSeaTunnelType(connectorDataType, new HashMap<>(0));
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String connectorDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        checkNotNull(connectorDataType, "DB2 Type cannot be null");
        switch (connectorDataType) {
            case DB2_BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case DB2_SMALLINT:
                return BasicType.SHORT_TYPE;
            case DB2_INT:
            case DB2_INTEGER:
                return BasicType.INT_TYPE;
            case DB2_BIGINT:
                return BasicType.LONG_TYPE;
            case DB2_DECIMAL:
            case DB2_DEC:
            case DB2_NUMERIC:
            case DB2_NUM:
                int precision =
                        MapUtils.getInteger(dataTypeProperties, PRECISION, DEFAULT_PRECISION);

                int scale = MapUtils.getInteger(dataTypeProperties, SCALE, DEFAULT_SCALE);
                if (precision > 0) {
                    return new DecimalType(precision, scale);
                }
                return new DecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
            case DB2_REAL:
                return BasicType.FLOAT_TYPE;
            case DB2_FLOAT:
            case DB2_DOUBLE:
            case DB2_DOUBLE_PRECISION:
            case DB2_DECFLOAT:
                return BasicType.DOUBLE_TYPE;
            case DB2_CHAR:
            case DB2_CHARACTER:
            case DB2_VARCHAR:
            case DB2_LONG_VARCHAR:
            case DB2_CLOB:
            case DB2_GRAPHIC:
            case DB2_VARGRAPHIC:
            case DB2_LONG_VARGRAPHIC:
            case DB2_DBCLOB:
                return BasicType.STRING_TYPE;
            case DB2_BINARY:
            case DB2_VARBINARY:
            case DB2_BLOB:
                return PrimitiveByteArrayType.INSTANCE;
            case DB2_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case DB2_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case DB2_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case DB2_ROWID:
                // maybe should support
            case DB2_XML:
            default:
                throw new UnsupportedOperationException(
                        String.format("Doesn't support DB2 type '%s''  yet.", connectorDataType));
        }
    }

    @Override
    public String toConnectorType(
            SeaTunnelDataType<?> seaTunnelDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        checkNotNull(seaTunnelDataType, "seaTunnelDataType cannot be null");
        SqlType sqlType = seaTunnelDataType.getSqlType();
        switch (sqlType) {
            case TINYINT:
            case SMALLINT:
            case INT:
                return DB2_INT;
            case BIGINT:
                return DB2_BIGINT;
            case FLOAT:
                return DB2_FLOAT;
            case DOUBLE:
                return DB2_DOUBLE;
            case DECIMAL:
                return DB2_DECIMAL;
            case BOOLEAN:
                return DB2_BOOLEAN;
            case STRING:
                return DB2_VARCHAR;
            case DATE:
                return DB2_DATE;
            case TIME:
                return DB2_TIME;
            case TIMESTAMP:
                return DB2_TIMESTAMP;
            case BYTES:
                return DB2_VARBINARY;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support SeaTunnel type '%s' yet.", seaTunnelDataType));
        }
    }

    @Override
    public String getIdentity() {
        return DatabaseIdentifier.DB_2;
    }
}
