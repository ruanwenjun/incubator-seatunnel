package org.apache.seatunnel.connectors.dolphindb.exception;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

public class DolphinDBConnectorException extends SeaTunnelRuntimeException {
    public DolphinDBConnectorException(DolphinDBErrorCode dolphinDBErrorCode, String errorMessage) {
        super(dolphinDBErrorCode, errorMessage);
    }

    public DolphinDBConnectorException(DolphinDBErrorCode dolphinDBErrorCode, Throwable throwable) {
        super(dolphinDBErrorCode, throwable);
    }

    public DolphinDBConnectorException(
            DolphinDBErrorCode dolphinDBErrorCode, String errorMessage, Throwable cause) {
        super(dolphinDBErrorCode, errorMessage, cause);
    }
}
