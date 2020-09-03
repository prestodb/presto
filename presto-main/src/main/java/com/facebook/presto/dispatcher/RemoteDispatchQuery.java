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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.StatusResponseHandler;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.execution.ClusterSizeMonitor;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.airlift.concurrent.MoreFutures.addSuccessCallback;
import static com.facebook.airlift.http.client.HttpStatus.OK;
import static com.facebook.airlift.http.client.Request.Builder.prepareDelete;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.TERMINAL_QUERY_STATES;
import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.shuffle;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class RemoteDispatchQuery
        implements DispatchQuery
{
    private static final Logger log = Logger.get(RemoteDispatchQuery.class);

    private final QueryId queryId;
    private final String user;
    private final String query;
    private final String slug;

    private final StateMachine<QueryState> queryState;
    @SuppressWarnings("UnstableApiUsage")
    private final HttpClient httpClient;
    private final SessionPropertyManager sessionPropertyManager;
    private final SettableFuture<?> dispatchedFuture = SettableFuture.create();
    private final QueryInfoFetcher queryInfoFetcher;

    private final LocationFactory httpLocationFactory;
    private final InternalNodeManager internalNodeManager;
    private final ClusterSizeMonitor clusterSizeMonitor;

    @GuardedBy("this")
    private volatile DateTime executionStartTime;

    @GuardedBy("this")
    private InternalNode chosenCoordinator;

    private final DateTime createTime = DateTime.now();
    private final long createNanos = System.nanoTime();
    private final AtomicLong lastHeartbeatNanos;

    @SuppressWarnings("UnstableApiUsage")
    public RemoteDispatchQuery(
            Session session,
            String query,
            String slug,
            HttpClient httpClient,
            SessionPropertyManager sessionPropertyManager,
            Executor executor,
            LocationFactory httpLocationFactory,
            InternalNodeManager internalNodeManager,
            ClusterSizeMonitor clusterSizeMonitor,
            QueryInfoFetcherFactory queryInfoFetcherFactory)
    {
        this.queryId = session.getQueryId();
        this.user = session.getUser();
        this.query = query;
        this.slug = slug;
        this.httpLocationFactory = httpLocationFactory;
        this.internalNodeManager = internalNodeManager;
        this.clusterSizeMonitor = clusterSizeMonitor;

        this.queryState = new StateMachine<>("query " + queryId, executor, QUEUED, TERMINAL_QUERY_STATES);
        this.httpClient = httpClient;
        this.sessionPropertyManager = sessionPropertyManager;
        this.queryInfoFetcher = queryInfoFetcherFactory.create(this::fail, queryId, query, session);

        this.lastHeartbeatNanos = new AtomicLong(System.nanoTime());
    }

    @Override
    public void recordHeartbeat()
    {
        lastHeartbeatNanos.set(System.nanoTime());
    }

    @Override
    public ListenableFuture<?> getDispatchedFuture()
    {
        return dispatchedFuture;
    }

    @Override
    public synchronized DispatchInfo getDispatchInfo()
    {
        // observe submitted before getting the state, to ensure a failed query stat is visible
        boolean dispatched = dispatchedFuture.isDone();
        BasicQueryInfo queryInfo = queryInfoFetcher.getQueryInfo();

        if (queryInfo == null) {
            return DispatchInfo.queued(Duration.valueOf("1s"), Duration.valueOf("1s"));
        }

        if (queryInfo.getState() == FAILED) {
            requireNonNull(queryInfo.getErrorCode(), "Failure info was null");
            ExecutionFailureInfo failureInfo = toFailure(new PrestoException(queryInfo::getErrorCode, queryInfo.getFailureInfo().getMessage()));
            return DispatchInfo.failed(failureInfo, queryInfo.getQueryStats().getElapsedTime(), queryInfo.getQueryStats().getQueuedTime());
        }
        if (dispatched && chosenCoordinator != null) {
            return DispatchInfo.dispatched(getCoordinatorLocation(chosenCoordinator), queryInfo.getQueryStats().getElapsedTime(), queryInfo.getQueryStats().getQueuedTime());
        }
        return DispatchInfo.queued(queryInfo.getQueryStats().getElapsedTime(), queryInfo.getQueryStats().getQueuedTime());
    }

    private CoordinatorLocation getCoordinatorLocation(InternalNode coordinator)
    {
        return (uriInfo, xForwardedProto) -> {
            String scheme = isNullOrEmpty(xForwardedProto) ? uriInfo.getRequestUri().getScheme() : xForwardedProto;
            return uriInfo.getRequestUriBuilder()
                    .host(coordinator.getHostAndPort().getHostText())
                    .port(coordinator.getHostAndPort().getPort())
                    .scheme(scheme)
                    .replacePath("")
                    .replaceQuery("");
        };
    }

    @SuppressWarnings("UnstableApiUsage")
    @Override
    public synchronized void cancel()
    {
        URI queryUri = httpLocationFactory.createQueryLocation(chosenCoordinator, queryId);
        Request request = prepareDelete().setUri(queryUri).build();
        httpClient.execute(request, createStatusResponseHandler());
    }

    @Override
    public void startWaitingForResources()
    {
        ListenableFuture<?> minimumWorkerFuture = clusterSizeMonitor.waitForMinimumCoordinators();
        // when worker requirement is met, wait for query execution to finish construction and then start the execution
        addSuccessCallback(minimumWorkerFuture, this::remoteExecuteQuery);
//        addExceptionCallback(minimumWorkerFuture, throwable -> queryExecutor.execute(() -> queryState.transitionToFailed(throwable)));
    }

    @SuppressWarnings("UnstableApiUsage")
    private synchronized void remoteExecuteQuery()
    {
        List<InternalNode> coordinators = newArrayList(internalNodeManager.getCoordinators());
        checkState(!coordinators.isEmpty());
        shuffle(coordinators);
        for (InternalNode coordinator : coordinators) {
            URI uri = httpLocationFactory.createRemoteExecutingStatementLocation(coordinator, queryId, slug);
            Request request = preparePost()
                    .setUri(uri)
                    .addHeader(PRESTO_USER, user)
                    .setBodyGenerator(createStaticBodyGenerator(query.getBytes()))
                    .build();
            StatusResponseHandler.StatusResponse response = httpClient.execute(request, createStatusResponseHandler());
            if (response.getStatusCode() == OK.code()) {
                queryInfoFetcher.start(coordinator);
                chosenCoordinator = coordinator;
                executionStartTime = DateTime.now();
                dispatchedFuture.set(null);
                return;
            }
        }
    }

    @Override
    public void addStateChangeListener(StateMachine.StateChangeListener<QueryState> stateChangeListener)
    {
        queryState.addStateChangeListener(stateChangeListener);
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        return queryInfoFetcher.getQueryInfo().getQueryStats().getUserMemoryReservation();
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        return queryInfoFetcher.getQueryInfo().getQueryStats().getTotalMemoryReservation();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return queryInfoFetcher.getQueryInfo().getQueryStats().getTotalCpuTime();
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return queryInfoFetcher.getQueryInfo();
    }

    @Override
    public Optional<ErrorCode> getErrorCode()
    {
        return Optional.empty();
    }

    @Override
    public QueryId getQueryId()
    {
        return queryId;
    }

    @Override
    public boolean isDone()
    {
        return queryInfoFetcher.getQueryInfo().getState().isDone();
    }

    @Override
    public Session getSession()
    {
        return queryInfoFetcher.getQueryInfo().getSession().toSession(sessionPropertyManager);
    }

    @Override
    public DateTime getCreateTime()
    {
        return queryInfoFetcher.getQueryInfo().getQueryStats().getCreateTime();
    }

    @Override
    public Optional<DateTime> getExecutionStartTime()
    {
        return Optional.ofNullable(executionStartTime);
    }

    @Override
    public DateTime getLastHeartbeat()
    {
        long instantNanos = lastHeartbeatNanos.get();
        long millisSinceCreate = NANOSECONDS.toMillis(instantNanos - createNanos);
        return new DateTime(createTime.getMillis() + millisSinceCreate);
    }

    @Override
    public Optional<DateTime> getEndTime()
    {
        return Optional.ofNullable(queryInfoFetcher.getQueryInfo().getQueryStats().getEndTime());
    }

    @Override
    public void fail(Throwable cause)
    {
        log.error(cause);
        cancel();
    }

    @Override
    public void pruneInfo()
    {
        // NO OP
    }
}
