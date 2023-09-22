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

package org.apache.seatunnel.connectors.seatunnel.redshift.resource;

import org.apache.seatunnel.connectors.seatunnel.redshift.RedshiftJdbcClient;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConf;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
@Getter
@AllArgsConstructor
public class CommitterResource implements AutoCloseable {
    private final RedshiftJdbcClient redshiftJdbcClient;
    private final ExecutorService commitWorker;

    @Override
    public void close() {
        if (commitWorker != null) {
            commitWorker.shutdownNow();
        }
        if (redshiftJdbcClient != null) {
            redshiftJdbcClient.close();
        }
    }

    public static CommitterResource createResource(S3RedshiftConf conf) {
        log.info(
                "Create committer resource with worker size: {}",
                conf.getRedshiftS3FileCommitWorkerSize());
        return new CommitterResource(
                RedshiftJdbcClient.newConnectionPool(
                        conf, conf.getRedshiftS3FileCommitWorkerSize()),
                createCommitWorker(conf.getRedshiftS3FileCommitWorkerSize()));
    }

    private static ExecutorService createCommitWorker(int workerSize) {
        ThreadPoolExecutor executor =
                new ThreadPoolExecutor(
                        workerSize,
                        workerSize,
                        60L,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(),
                        new ThreadFactoryBuilder()
                                .setNameFormat("s3-redshift-commit-worker-%d")
                                .build());
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }
}
