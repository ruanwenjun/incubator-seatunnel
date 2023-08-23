/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.redshift.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.AbstractWriteStrategy;

import lombok.NonNull;

import java.util.Arrays;
import java.util.Collection;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.DEFAULT_FILE_NAME_EXPRESSION;
import static org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat.GZIP;
import static org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat.NONE;
import static org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat.SNAPPY;
import static org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat.ORC;

public class S3RedshiftChangelogWriteStrategy extends AbstractWriteStrategy {
    private final AbstractWriteStrategy delegate;

    public S3RedshiftChangelogWriteStrategy(AbstractWriteStrategy delegate) {
        super(validation(delegate.getFileSinkConfig()));
        this.delegate = delegate;
    }

    @Override
    public void write(@NonNull SeaTunnelRow seaTunnelRow) {
        throw new UnsupportedOperationException("Non-batch writes are not supported for changelog");
    }

    public synchronized void write(Collection<SeaTunnelRow> batch) {
        delegate.newFilePart();
        for (SeaTunnelRow row : batch) {
            delegate.write(row);
        }
        delegate.finishAndCloseFile();
    }

    @Override
    public void finishAndCloseFile() {
        throw new UnsupportedOperationException();
    }

    private static FileSinkConfig validation(FileSinkConfig fileSinkConfig) {
        checkArgument(
                fileSinkConfig.getFileNameExpression().contains(DEFAULT_FILE_NAME_EXPRESSION),
                "File name expression must contain '%s'",
                DEFAULT_FILE_NAME_EXPRESSION);
        checkArgument(fileSinkConfig.isEnableTransaction(), "Transaction must be enabled");
        checkArgument(
                Arrays.asList(ORC).contains(fileSinkConfig.getFileFormat()),
                "File format must be ORC");
        checkArgument(
                Arrays.asList(NONE, GZIP, SNAPPY).contains(fileSinkConfig.getCompressFormat()),
                "Compress format must be NONE, GZIP or SNAPPY");
        return fileSinkConfig;
    }
}
