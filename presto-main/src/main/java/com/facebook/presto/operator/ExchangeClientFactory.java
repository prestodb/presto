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
package com.facebook.presto.operator;

import com.facebook.airlift.concurrent.ThreadPoolExecutorMBean;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.server.ForAsyncHttp;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Provider;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class ExchangeClientFactory
        implements ExchangeClientSupplier
{
    private final DataSize maxBufferedBytes;
    private final int concurrentRequestMultiplier;
    private final Duration maxErrorDuration;
    private final HttpClient httpClient;
    private final DataSize maxResponseSize;
    private final boolean acknowledgePages;
    private final double responseSizeExponentialMovingAverageDecayingAlpha;
    private final ScheduledExecutorService scheduler;
    private final ThreadPoolExecutorMBean executorMBean;
    private final ExecutorService pageBufferClientCallbackExecutor;
    private final Provider<TaskManager> taskManagerProvider;
    private final ScheduledExecutorService timeoutExecutor;
    private final InternalNodeManager nodeManager;
    private final boolean bypassHttpForLocal;

    public ExchangeClientFactory(
            ExchangeClientConfig config,
            TaskManagerConfig taskManagerConfig,
            @ForExchange HttpClient httpClient,
            @ForExchange ScheduledExecutorService scheduler)
    {
        this(config, taskManagerConfig, httpClient, scheduler, null, null, null);
    }

    @Inject
    public ExchangeClientFactory(
            ExchangeClientConfig config,
            TaskManagerConfig taskManagerConfig,
            @ForExchange HttpClient httpClient,
            @ForExchange ScheduledExecutorService scheduler,
            @ForAsyncHttp ScheduledExecutorService timeoutExecutor,
            InternalNodeManager nodeManager,
            Provider<TaskManager> taskManagerProvider)
    {
        this(
                config.getMaxBufferSize(),
                config.getMaxResponseSize(),
                config.getConcurrentRequestMultiplier(),
                config.getMaxErrorDuration(),
                config.isAcknowledgePages(),
                config.getPageBufferClientMaxCallbackThreads(),
                config.getResponseSizeExponentialMovingAverageDecayingAlpha(),
                httpClient,
                nodeManager,
                scheduler,
                taskManagerConfig,
                taskManagerProvider,
                timeoutExecutor);
    }

    public ExchangeClientFactory(
            DataSize maxBufferedBytes,
            DataSize maxResponseSize,
            int concurrentRequestMultiplier,
            Duration maxErrorDuration,
            boolean acknowledgePages,
            int pageBufferClientMaxCallbackThreads,
            double responseSizeExponentialMovingAverageDecayingAlpha,
            HttpClient httpClient,
            InternalNodeManager nodeManager,
            ScheduledExecutorService scheduler,
            TaskManagerConfig taskManagerConfig,
            Provider<TaskManager> taskManagerProvider,
            ScheduledExecutorService timeoutExecutor)
    {
        this.maxBufferedBytes = requireNonNull(maxBufferedBytes, "maxBufferedBytes is null");
        this.concurrentRequestMultiplier = concurrentRequestMultiplier;
        this.maxErrorDuration = requireNonNull(maxErrorDuration, "maxErrorDuration is null");
        this.acknowledgePages = acknowledgePages;
        this.httpClient = requireNonNull(httpClient, "httpClient is null");

        // Use only 0.75 of the maxResponseSize to leave room for additional bytes from the encoding
        // TODO figure out a better way to compute the size of data that will be transferred over the network
        requireNonNull(maxResponseSize, "maxResponseSize is null");
        long maxResponseSizeBytes = (long) (Math.min(httpClient.getMaxContentLength(), maxResponseSize.toBytes()) * 0.75);
        this.maxResponseSize = new DataSize(maxResponseSizeBytes, BYTE);

        this.scheduler = requireNonNull(scheduler, "scheduler is null");

        this.pageBufferClientCallbackExecutor = newFixedThreadPool(pageBufferClientMaxCallbackThreads, daemonThreadsNamed("page-buffer-client-callback-%s"));
        this.executorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) pageBufferClientCallbackExecutor);
        this.timeoutExecutor = timeoutExecutor;
        this.taskManagerProvider = taskManagerProvider;
        this.nodeManager = nodeManager;
        this.bypassHttpForLocal = taskManagerConfig.isBypassHttpForLocal();

        this.responseSizeExponentialMovingAverageDecayingAlpha = responseSizeExponentialMovingAverageDecayingAlpha;

        checkArgument(maxBufferedBytes.toBytes() > 0, "maxBufferSize must be at least 1 byte: %s", maxBufferedBytes);
        checkArgument(maxResponseSize.toBytes() > 0, "maxResponseSize must be at least 1 byte: %s", maxResponseSize);
        checkArgument(concurrentRequestMultiplier > 0, "concurrentRequestMultiplier must be at least 1: %s", concurrentRequestMultiplier);
        checkArgument(responseSizeExponentialMovingAverageDecayingAlpha >= 0.0 && responseSizeExponentialMovingAverageDecayingAlpha <= 1.0, "responseSizeExponentialMovingAverageDecayingAlpha must be between 0 and 1: %s", responseSizeExponentialMovingAverageDecayingAlpha);
    }

    @PreDestroy
    public void stop()
    {
        pageBufferClientCallbackExecutor.shutdownNow();
    }

    @Managed
    @Nested
    public ThreadPoolExecutorMBean getExecutor()
    {
        return executorMBean;
    }

    @Override
    public ExchangeClient get(LocalMemoryContext systemMemoryContext)
    {
        return new ExchangeClient(
                maxBufferedBytes,
                maxResponseSize,
                concurrentRequestMultiplier,
                maxErrorDuration,
                acknowledgePages,
                responseSizeExponentialMovingAverageDecayingAlpha,
                httpClient,
                scheduler,
                systemMemoryContext,
                pageBufferClientCallbackExecutor,
                nodeManager,
                taskManagerProvider,
                bypassHttpForLocal,
                timeoutExecutor);
    }
}
