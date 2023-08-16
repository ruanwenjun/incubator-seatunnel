package org.apache.seatunnel.connectors.dolphindb.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum DolphinDBErrorCode implements SeaTunnelErrorCode {
    WRITE_DATA_ERROR("DolphinDB-0001", "Write data error"),
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
