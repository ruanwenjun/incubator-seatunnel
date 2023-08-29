package org.apache.seatunnel.connectors.dolphindb.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum DolphinDBErrorCode implements SeaTunnelErrorCode {
    WRITE_DATA_ERROR("DolphinDB-0001", "Write data error"),
    SOURCE_ALREADY_HAS_DATA("DolphinDB-0002", "SaveMode error"),
    EXECUTE_CUSTOMER_SCRIPT_ERROR("DolphinDB-0003", "Execute customer sql error"),
    CHECK_DATA_ERROR("DolphinDB-0004", "Check data exist error"),
    UNSUPPORTED_OPERATION("DolphinDB-0005", "Upsupported operation"),
    DELETE_DATA_ERROR("DolphinDB-0006", "Delete data error"),
    ;
    private final String code;
    private final String description;

    DolphinDBErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
