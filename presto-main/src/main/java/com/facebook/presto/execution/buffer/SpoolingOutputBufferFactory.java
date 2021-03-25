/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.execution.buffer;

import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.spiller.LocalTempStorage;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.storage.TempStorageManager;
import com.facebook.presto.testing.TestingTempStorageManager;
import com.facebook.presto.util.FinalizerService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class SpoolingOutputBufferFactory
{
    private ListeningExecutorService executor;

    private final FeaturesConfig featuresConfig;
    private final TempStorageManager tempStorageManager;
    private final FinalizerService finalizerService;

    private final Closer closer = Closer.create();

    private final ExecutorService coreExecutor = newCachedThreadPool(daemonThreadsNamed("spooling-outputbuffer-%s"));

    @Inject
    public SpoolingOutputBufferFactory(FeaturesConfig featuresConfig, TempStorageManager tempStorageManager, FinalizerService finalizerService)
    {
        this.featuresConfig = requireNonNull(featuresConfig, "featuresConfig is null");
        this.tempStorageManager = requireNonNull(tempStorageManager, "tempStorageManger is null");
        this.finalizerService = requireNonNull(finalizerService, "finalizerService is null");
    }

    @VisibleForTesting
    public SpoolingOutputBufferFactory(FeaturesConfig featuresConfig)
    {
        this.featuresConfig = requireNonNull(featuresConfig);
        tempStorageManager = new TestingTempStorageManager();
        finalizerService = new FinalizerService();

        initialize();
    }

    @PostConstruct
    public void initialize()
    {
        closer.register(coreExecutor::shutdownNow);
        executor = listeningDecorator(coreExecutor);
    }

    @PreDestroy
    public void shutdown()
            throws IOException
    {
        closer.close();
    }

    public SpoolingOutputBuffer createSpoolingOutputBuffer(
            TaskId taskId,
            String taskInstanceId,
            OutputBuffers outputBuffers,
            StateMachine<BufferState> state)
    {
        return new SpoolingOutputBuffer(
                taskId,
                taskInstanceId,
                outputBuffers,
                state,
                tempStorageManager.getTempStorage(LocalTempStorage.NAME),
                featuresConfig.getSpoolingOutputBufferThreshold(),
                executor,
                finalizerService);
    }
}
