package org.apache.seatunnel.connectors.dws.guassdb.sink.state;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class DwsGaussDBSinkState implements Serializable {

    private String snapshotId;
}
