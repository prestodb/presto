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
package com.facebook.presto.dispatcher;

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.memory.ForRemoteDispatch;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.InternalCommunicationConfig;
import com.facebook.presto.server.smile.Codec;
import com.facebook.presto.server.smile.SmileCodec;
import com.facebook.presto.spi.QueryId;
import io.airlift.units.Duration;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.server.BasicQueryInfo.dispatchedQueryInfo;
import static com.facebook.presto.server.smile.JsonCodecWrapper.wrapJsonCodec;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class QueryInfoFetcherFactory
{
    private final Codec<BasicQueryInfo> queryInfoCodec;

    private final Duration updateInterval;
    private final Duration queryInfoRefreshMaxWait;
    private final Duration maxErrorDuration;

    private final HttpClient httpClient;
    private final ExecutorService coreExecutor;
    private final Executor executor;
    private final ScheduledExecutorService updateScheduledExecutor;
    private final ScheduledExecutorService errorScheduledExecutor;
    private final RemoteQueryStats remoteQueryStats;
    private final LocationFactory locationFactory;
    private final boolean isBinaryTransportEnabled;

    @Inject
    public QueryInfoFetcherFactory(
            DispatcherConfig config,
            InternalCommunicationConfig communicationConfig,
            @ForRemoteDispatch HttpClient httpClient,
            JsonCodec<BasicQueryInfo> jsonQueryInfoCodec,
            SmileCodec<BasicQueryInfo> smileQueryInfoCodec,
            RemoteQueryStats remoteQueryStats,
            LocationFactory locationFactory)
    {
        this.queryInfoRefreshMaxWait = config.getRemoteQueryInfoRefreshMaxWait();
        this.updateInterval = config.getRemoteQueryInfoUpdateInterval();
        this.maxErrorDuration = config.getRemoteQueryInfoMaxErrorDuration();
        this.isBinaryTransportEnabled = communicationConfig.isBinaryTransportEnabled();

        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        requireNonNull(jsonQueryInfoCodec, "jsonQueryInfoCodec is null");
        requireNonNull(smileQueryInfoCodec, "smileQueryInfoCodec is null");
        this.queryInfoCodec = communicationConfig.isBinaryTransportEnabled() ? smileQueryInfoCodec : wrapJsonCodec(jsonQueryInfoCodec);
        this.remoteQueryStats = requireNonNull(remoteQueryStats, "remoteQueryStats is null");
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");

        this.coreExecutor = newCachedThreadPool(daemonThreadsNamed("remote-task-callback-%s"));
        this.executor = new BoundedExecutor(coreExecutor, config.getRemoteQueryInfoMaxCallbackThreads());
        this.updateScheduledExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("query-info-update-scheduler-%s"));
        this.errorScheduledExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("remote-query-error-delay-%s"));
    }

    @PreDestroy
    public void stop()
    {
        coreExecutor.shutdownNow();
        updateScheduledExecutor.shutdownNow();
        errorScheduledExecutor.shutdownNow();
    }

    public QueryInfoFetcher create(Consumer<Throwable> onError, QueryId queryId, String query, Session session)
    {
        return new QueryInfoFetcher(
                onError,
                queryId,
                httpClient,
                dispatchedQueryInfo(queryId, query, session),
                queryInfoRefreshMaxWait,
                updateInterval,
                queryInfoCodec,
                maxErrorDuration,
                executor,
                updateScheduledExecutor,
                errorScheduledExecutor,
                remoteQueryStats,
                locationFactory,
                isBinaryTransportEnabled);
    }
}
