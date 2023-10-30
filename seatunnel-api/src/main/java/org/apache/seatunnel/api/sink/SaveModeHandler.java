package org.apache.seatunnel.api.sink;

public interface SaveModeHandler extends AutoCloseable {

    void handleSchemaSaveMode();

    void handleDataSaveMode();

    void handleSchemaSaveModeWithRestore();

    default void handleSaveMode() {
        handleSchemaSaveMode();
        handleDataSaveMode();
    }

    SchemaSaveMode getSchemaSaveMode();

    DataSaveMode getDataSaveMode();
}
