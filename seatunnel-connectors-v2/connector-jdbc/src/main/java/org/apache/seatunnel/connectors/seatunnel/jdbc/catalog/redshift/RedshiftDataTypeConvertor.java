package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.redshift;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.seatunnel.api.table.catalog.DataTypeConvertException;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;

import com.google.auto.service.AutoService;
import org.apache.commons.collections4.MapUtils;

import java.util.Collections;
import java.util.Map;

@AutoService(DataTypeConvertor.class)
public class RedshiftDataTypeConvertor implements DataTypeConvertor<String> {

    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";

    public static final Integer DEFAULT_PRECISION = 10;

    public static final Integer DEFAULT_SCALE = 0;

    /* ============================ data types ===================== */
    private static final String REDSHIFT_SMALLINT = "SMALLINT";
    private static final String REDSHIFT_INT2 = "INT2";
    private static final String REDSHIFT_INTEGER = "INTEGER";
    private static final String REDSHIFT_INT = "INT";
    private static final String REDSHIFT_INT4 = "INT4";
    private static final String REDSHIFT_BIGINT = "BIGINT";
    private static final String REDSHIFT_INT8 = "INT8";

    private static final String REDSHIFT_DECIMAL = "DECIMAL";
    private static final String REDSHIFT_NUMERIC = "NUMERIC";
    private static final String REDSHIFT_REAL = "REAL";
    private static final String REDSHIFT_FLOAT4 = "FLOAT4";
    private static final String REDSHIFT_DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final String REDSHIFT_FLOAT8 = "FLOAT8";
    private static final String REDSHIFT_FLOAT = "FLOAT";

    private static final String REDSHIFT_BOOLEAN = "BOOLEAN";
    private static final String REDSHIFT_BOOL = "BOOL";

    private static final String REDSHIFT_CHAR = "CHAR";
    private static final String REDSHIFT_CHARACTER = "CHARACTER";
    private static final String REDSHIFT_NCHAR = "NCHAR";
    private static final String REDSHIFT_BPCHAR = "BPCHAR";

    private static final String REDSHIFT_VARCHAR = "VARCHAR";
    private static final String REDSHIFT_CHARACTER_VARYING = "CHARACTER VARYING";
    private static final String REDSHIFT_NVARCHAR = "NVARCHAR";
    private static final String REDSHIFT_TEXT = "TEXT";

    private static final String REDSHIFT_DATE = "DATE";
    /*FIXME*/

    private static final String REDSHIFT_GEOMETRY = "GEOMETRY";
    private static final String REDSHIFT_OID = "OID";
    private static final String REDSHIFT_SUPER = "SUPER";

    private static final String REDSHIFT_TIME = "TIME";
    private static final String REDSHIFT_TIME_WITH_TIME_ZONE = "TIME WITH TIME ZONE";

    private static final String REDSHIFT_TIMETZ = "TIMETZ";
    private static final String REDSHIFT_TIMESTAMP = "TIMESTAMP";
    private static final String REDSHIFT_TIMESTAMP_WITH_OUT_TIME_ZONE = "TIMESTAMP WITHOUT TIME ZONE";

    private static final String REDSHIFT_TIMESTAMPTZ = "TIMESTAMPTZ";

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String connectorDataType) {
        return toSeaTunnelType(connectorDataType, Collections.emptyMap());
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String connectorDataType,
                                                Map<String, Object> dataTypeProperties) throws DataTypeConvertException {
        checkNotNull(connectorDataType, "redshiftType cannot be null");
        switch (connectorDataType) {
            case REDSHIFT_SMALLINT:
            case REDSHIFT_INT2:
                return BasicType.SHORT_TYPE;
            case REDSHIFT_INTEGER:
            case REDSHIFT_INT:
            case REDSHIFT_INT4:
                return BasicType.INT_TYPE;
            case REDSHIFT_BIGINT:
            case REDSHIFT_INT8:
            case REDSHIFT_OID:
                return BasicType.LONG_TYPE;
            case REDSHIFT_DECIMAL:
            case REDSHIFT_NUMERIC:
                Integer precision = MapUtils.getInteger(dataTypeProperties, PRECISION, DEFAULT_PRECISION);
                Integer scale = MapUtils.getInteger(dataTypeProperties, SCALE, DEFAULT_SCALE);
                return new DecimalType(precision, scale);
            case REDSHIFT_REAL:
            case REDSHIFT_FLOAT4:
                return BasicType.FLOAT_TYPE;
            case REDSHIFT_DOUBLE_PRECISION:
            case REDSHIFT_FLOAT8:
            case REDSHIFT_FLOAT:
                return BasicType.DOUBLE_TYPE;
            case REDSHIFT_BOOLEAN:
            case REDSHIFT_BOOL:
                return BasicType.BOOLEAN_TYPE;
            case REDSHIFT_CHAR:
            case REDSHIFT_CHARACTER:
            case REDSHIFT_NCHAR:
            case REDSHIFT_BPCHAR:
            case REDSHIFT_VARCHAR:
            case REDSHIFT_CHARACTER_VARYING:
            case REDSHIFT_NVARCHAR:
            case REDSHIFT_TEXT:
            case REDSHIFT_SUPER:
                return BasicType.STRING_TYPE;
            case REDSHIFT_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case REDSHIFT_GEOMETRY:
                return PrimitiveByteArrayType.INSTANCE;
            case REDSHIFT_TIME:
            case REDSHIFT_TIME_WITH_TIME_ZONE:
            case REDSHIFT_TIMETZ:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case REDSHIFT_TIMESTAMP:
            case REDSHIFT_TIMESTAMP_WITH_OUT_TIME_ZONE:
            case REDSHIFT_TIMESTAMPTZ:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            default:
                throw new UnsupportedOperationException(String.format("Doesn't support REDSHIFT type '%s''  yet.", connectorDataType));
        }
    }

    @Override
    public String toConnectorType(SeaTunnelDataType<?> seaTunnelDataType,
                                  Map<String, Object> dataTypeProperties) throws DataTypeConvertException {
        checkNotNull(seaTunnelDataType, "seaTunnelDataType cannot be null");
        SqlType sqlType = seaTunnelDataType.getSqlType();
        switch (sqlType) {
            case TINYINT:
            case SMALLINT:
                return REDSHIFT_SMALLINT;
            case INT:
                return REDSHIFT_INTEGER;
            case BIGINT:
                return REDSHIFT_BIGINT;
            case DECIMAL:
                return REDSHIFT_DECIMAL;
            case FLOAT:
                return REDSHIFT_FLOAT4;
            case DOUBLE:
                return REDSHIFT_DOUBLE_PRECISION;
            case BOOLEAN:
                return REDSHIFT_BOOLEAN;
            case STRING:
                return REDSHIFT_TEXT;
            case DATE:
                return REDSHIFT_DATE;
            case BYTES:
                return REDSHIFT_GEOMETRY;
            case TIME:
                return REDSHIFT_TIME;
            case TIMESTAMP:
                return REDSHIFT_TIMESTAMP;
            default:
                throw new UnsupportedOperationException(String.format("Doesn't support SeaTunnel type '%s''  yet.", seaTunnelDataType));
        }
    }

    @Override
    public String getIdentity() {
        return "REDSHIFT";
    }
}
