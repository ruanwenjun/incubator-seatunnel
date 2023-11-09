package io.debezium.connector.oracle.oracleAgent;

public interface OracleAgentDmlEntry {

    /** @return object array that contains the before state, values from WHERE clause. */
    Object[] getOldValues();

    /**
     * @return object array that contains the after state, values from an insert's values list or
     *     the values in the SET clause of an update statement.
     */
    Object[] getNewValues();

    /** @return LogMiner event operation type */
    Operation getOperation();
}
