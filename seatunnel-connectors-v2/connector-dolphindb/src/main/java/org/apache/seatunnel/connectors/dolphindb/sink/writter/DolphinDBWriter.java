package org.apache.seatunnel.connectors.dolphindb.sink.writter;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.Optional;

public interface DolphinDBWriter extends AutoCloseable {

    void write(SeaTunnelRow seaTunnelRow);

    Optional<Void> prepareCommit() throws Exception;
}
