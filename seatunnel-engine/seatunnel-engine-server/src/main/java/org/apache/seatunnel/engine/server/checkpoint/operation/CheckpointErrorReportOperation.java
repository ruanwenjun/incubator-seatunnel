package org.apache.seatunnel.engine.server.checkpoint.operation;

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.serializable.CheckpointDataSerializerHook;
import org.apache.seatunnel.engine.server.task.operation.TaskOperation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import lombok.NoArgsConstructor;

import java.io.IOException;

@NoArgsConstructor
public class CheckpointErrorReportOperation extends TaskOperation {

    private String errorMsg;

    public CheckpointErrorReportOperation(TaskLocation taskLocation, Throwable e) {
        super(taskLocation);
        this.errorMsg = ExceptionUtils.getMessage(e);
    }

    @Override
    public void run() throws Exception {
        SeaTunnelServer server = getService();
        server.getCoordinatorService()
                .getJobMaster(taskLocation.getJobId())
                .getCheckpointManager()
                .reportCheckpointErrorFromTask(taskLocation, errorMsg);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeString(errorMsg);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        errorMsg = in.readString();
    }

    @Override
    public int getFactoryId() {
        return CheckpointDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return CheckpointDataSerializerHook.CHECKPOINT_ERROR_REPORT_OPERATOR;
    }
}
