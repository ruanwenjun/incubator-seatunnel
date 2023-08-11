package org.apache.seatunnel.api.sink;

/**
 * This interface is implemented by sink (synchronization task target) to achieve automatic table creation
 */
public interface SupportAutoCreateTable {
    /**
     * The method execution must occur before handleSaveMode
     */
    void handleAutoCreateTable();
}
