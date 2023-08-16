package org.apache.seatunnel.connectors.dolphindb.datatyle;

import org.apache.seatunnel.api.table.catalog.DataTypeConvertException;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig;

import com.google.auto.service.AutoService;
import com.xxdb.data.AbstractEntity;
import com.xxdb.data.Entity;

import java.util.Map;

@AutoService(DataTypeConvertor.class)
public class DolphinDBDataTypeConvertor implements DataTypeConvertor<AbstractEntity> {

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
            AbstractEntity connectorDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        return null;
    }

    @Override
    public AbstractEntity toConnectorType(
            SeaTunnelDataType<?> seaTunnelDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        // todo:
        return null;
    }

    @Override
    public String getIdentity() {
        return DolphinDBConfig.PLUGIN_NAME;
    }
}
