package org.apache.seatunnel.connectors.dolphindb.datatyle;

import org.apache.seatunnel.api.table.catalog.DataTypeConvertException;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig;

import com.google.auto.service.AutoService;
import com.xxdb.data.Entity;

import java.util.Map;

@AutoService(DataTypeConvertor.class)
public class DolphinDBDataTypeConvertor implements DataTypeConvertor<Entity.DATA_TYPE> {

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String connectorDataType) {
        // The connectorDataType is not used in Sink, we will get the schema from the database
        if (Entity.DATA_TYPE.DT_VOID.name().equals(connectorDataType)) {
            return BasicType.VOID_TYPE;
        }
        if (Entity.DATA_TYPE.DT_BOOL.name().equals(connectorDataType)) {
            return BasicType.BOOLEAN_TYPE;
        }
        if (Entity.DATA_TYPE.DT_BYTE.name().equals(connectorDataType)) {
            return BasicType.BYTE_TYPE;
        }
        if (Entity.DATA_TYPE.DT_SHORT.name().equals(connectorDataType)) {
            return BasicType.SHORT_TYPE;
        }
        if (Entity.DATA_TYPE.DT_INT.name().equals(connectorDataType)) {
            return BasicType.INT_TYPE;
        }
        if (Entity.DATA_TYPE.DT_LONG.name().equals(connectorDataType)) {
            return BasicType.LONG_TYPE;
        }
        // todo: How to deal with DT_DATE, DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND
        if (Entity.DATA_TYPE.DT_DATE.name().equals(connectorDataType)
                || Entity.DATA_TYPE.DT_MONTH.name().equals(connectorDataType)
                || Entity.DATA_TYPE.DT_TIME.name().equals(connectorDataType)
                || Entity.DATA_TYPE.DT_MINUTE.name().equals(connectorDataType)
                || Entity.DATA_TYPE.DT_SECOND.name().equals(connectorDataType)) {
            return LocalTimeType.LOCAL_DATE_TIME_TYPE;
        }
        return BasicType.STRING_TYPE;
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            Entity.DATA_TYPE connectorDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        return null;
    }

    @Override
    public Entity.DATA_TYPE toConnectorType(
            SeaTunnelDataType<?> seaTunnelDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        SqlType sqlType = seaTunnelDataType.getSqlType();
        switch (sqlType) {
            case ARRAY:
                ArrayType arrayType = (ArrayType) seaTunnelDataType;
                BasicType elementType = arrayType.getElementType();
                switch (elementType.getSqlType()) {
                    case STRING:
                        return Entity.DATA_TYPE.DT_STRING_ARRAY;
                    case BOOLEAN:
                        return Entity.DATA_TYPE.DT_BOOL_ARRAY;
                    case TINYINT:
                        return Entity.DATA_TYPE.DT_SHORT_ARRAY;
                    case SMALLINT:
                        return Entity.DATA_TYPE.DT_SHORT_ARRAY;
                    case INT:
                        return Entity.DATA_TYPE.DT_INT_ARRAY;
                    case BIGINT:
                        return Entity.DATA_TYPE.DT_LONG_ARRAY;
                    case FLOAT:
                        return Entity.DATA_TYPE.DT_FLOAT_ARRAY;
                    case DOUBLE:
                        return Entity.DATA_TYPE.DT_DOUBLE_ARRAY;
                    case DECIMAL:
                        return Entity.DATA_TYPE.DT_DOUBLE_ARRAY;
                    case BYTES:
                        return Entity.DATA_TYPE.DT_BYTE_ARRAY;
                    case DATE:
                        return Entity.DATA_TYPE.DT_DATE_ARRAY;
                    case TIME:
                        return Entity.DATA_TYPE.DT_TIME_ARRAY;
                    case TIMESTAMP:
                        return Entity.DATA_TYPE.DT_TIMESTAMP_ARRAY;
                    default:
                        return Entity.DATA_TYPE.DT_STRING_ARRAY;
                }
            case MAP:
                return Entity.DATA_TYPE.DT_DICTIONARY;
            case STRING:
                return Entity.DATA_TYPE.DT_STRING;
            case BOOLEAN:
                return Entity.DATA_TYPE.DT_BOOL;
            case TINYINT:
                return Entity.DATA_TYPE.DT_SHORT;
            case SMALLINT:
                return Entity.DATA_TYPE.DT_SHORT;
            case INT:
                return Entity.DATA_TYPE.DT_INT;
            case BIGINT:
                return Entity.DATA_TYPE.DT_LONG;
            case FLOAT:
                return Entity.DATA_TYPE.DT_FLOAT;
            case DOUBLE:
                return Entity.DATA_TYPE.DT_DOUBLE;
            case DECIMAL:
                return Entity.DATA_TYPE.DT_DOUBLE;
            case NULL:
                return Entity.DATA_TYPE.DT_VOID;
            case BYTES:
                return Entity.DATA_TYPE.DT_BYTE;
            case DATE:
                return Entity.DATA_TYPE.DT_DATE;
            case TIME:
                return Entity.DATA_TYPE.DT_TIME;
            case TIMESTAMP:
                return Entity.DATA_TYPE.DT_TIMESTAMP;
            case ROW:
                return Entity.DATA_TYPE.DT_STRING;
            case MULTIPLE_ROW:
                return Entity.DATA_TYPE.DT_STRING;
            default:
                return Entity.DATA_TYPE.DT_STRING;
        }
    }

    @Override
    public String getIdentity() {
        return DolphinDBConfig.PLUGIN_NAME;
    }
}
