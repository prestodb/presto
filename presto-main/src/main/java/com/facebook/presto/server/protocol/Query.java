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
package com.facebook.presto.server.protocol;

import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StageStats;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.memory.context.SimpleLocalMemoryContext;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.operator.ExchangeClientSupplier;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.HashSet;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.addTimeout;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
abstract class Query
{
    private static final Logger log = Logger.get(Query.class);

    protected final QueryManager queryManager;
    protected final QueryId queryId;

    private final Executor resultsProcessorExecutor;
    protected final ScheduledExecutorService timeoutExecutor;

    private final AtomicLong resultId = new AtomicLong();

    @GuardedBy("this")
    private QueryResults lastResult;

    @GuardedBy("this")
    private String lastResultPath;

    public static Query createV1(
            SessionContext sessionContext,
            String query,
            QueryManager queryManager,
            SessionPropertyManager sessionPropertyManager,
            ExchangeClientSupplier exchangeClientSupplier,
            Executor dataProcessorExecutor,
            ScheduledExecutorService timeoutExecutor,
            BlockEncodingSerde blockEncodingSerde)
    {
        QueryInfo queryInfo = queryManager.createQuery(sessionContext, query);
        ExchangeClient exchangeClient = exchangeClientSupplier.get(new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext()));
        Query createdQuery = new QueryV1(queryInfo, queryManager, sessionPropertyManager, exchangeClient, dataProcessorExecutor, timeoutExecutor, blockEncodingSerde);
        createdQuery.queryManager.addOutputInfoListener(createdQuery.queryId, createdQuery::setQueryOutputInfo);
        return createdQuery;
    }

    public static Query createV2(
            SessionContext sessionContext,
            String query,
            QueryManager queryManager,
            Executor dataProcessorExecutor,
            ScheduledExecutorService timeoutExecutor)
    {
        QueryInfo queryInfo = queryManager.createQuery(sessionContext, query);
        Query createdQuery = new QueryV2(queryInfo, queryManager, dataProcessorExecutor, timeoutExecutor);
        createdQuery.queryManager.addOutputInfoListener(createdQuery.queryId, createdQuery::setQueryOutputInfo);
        return createdQuery;
    }

    protected Query(
            QueryInfo queryInfo,
            QueryManager queryManager,
            Executor resultsProcessorExecutor,
            ScheduledExecutorService timeoutExecutor)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.queryId = requireNonNull(queryInfo, "queryInfo is null").getQueryId();
        this.resultsProcessorExecutor = requireNonNull(resultsProcessorExecutor, "resultsProcessorExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
    }

    public abstract void dispose();

    protected abstract void setQueryOutputInfo(QueryExecution.QueryOutputInfo outputInfo);

    protected abstract QueryResults getNextQueryResults(QueryInfo queryInfo, UriInfo uriInfo);

    @Nullable
    protected abstract ListenableFuture<?> isStatusChanged();

    public void cancel()
    {
        queryManager.cancelQuery(queryId);
        dispose();
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public synchronized ListenableFuture<QueryResults> waitForResults(OptionalLong token, UriInfo uriInfo, Duration wait)
    {
        // before waiting, check if this request has already been processed and cached
        if (token.isPresent()) {
            Optional<QueryResults> cachedResult = getCachedResult(token.getAsLong(), uriInfo);
            if (cachedResult.isPresent()) {
                return immediateFuture(cachedResult.get());
            }
        }

        // wait for a results data or query to finish, up to the wait timeout
        ListenableFuture<?> futureStateChange = addTimeout(
                getFutureStateChange(),
                () -> null,
                wait,
                timeoutExecutor);

        // when state changes, fetch the next result
        return Futures.transform(futureStateChange, ignored -> getNextResult(token, uriInfo), resultsProcessorExecutor);
    }

    private synchronized ListenableFuture<?> getFutureStateChange()
    {
        // if the exchange client is open, wait for data
        ListenableFuture<?> statusChangedFuture = isStatusChanged();
        if (statusChangedFuture != null) {
            return statusChangedFuture;
        }

        // otherwise, wait for the query to finish
        queryManager.recordHeartbeat(queryId);
        return queryManager.getQueryState(queryId).map(this::queryDoneFuture)
                .orElse(immediateFuture(null));
    }

    private synchronized Optional<QueryResults> getCachedResult(long token, UriInfo uriInfo)
    {
        // is the a repeated request for the last results?
        String requestedPath = uriInfo.getAbsolutePath().getPath();
        if (lastResultPath != null && requestedPath.equals(lastResultPath)) {
            // tell query manager we are still interested in the query
            queryManager.getQueryInfo(queryId);
            queryManager.recordHeartbeat(queryId);
            return Optional.of(lastResult);
        }

        if (token < resultId.get()) {
            throw new WebApplicationException(Response.Status.GONE);
        }

        // if this is not a request for the next results, return not found
        if (lastResult.getNextUri() == null || !requestedPath.equals(lastResult.getNextUri().getPath())) {
            // unknown token
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }

        return Optional.empty();
    }

    private synchronized QueryResults getNextResult(OptionalLong token, UriInfo uriInfo)
    {
        // check if the result for the token have already been created
        if (token.isPresent()) {
            Optional<QueryResults> cachedResult = getCachedResult(token.getAsLong(), uriInfo);
            if (cachedResult.isPresent()) {
                return cachedResult.get();
            }
        }

        // get the query info before returning
        // force update if query manager is closed
        QueryInfo queryInfo = queryManager.getQueryInfo(queryId);
        queryManager.recordHeartbeat(queryId);

        QueryResults queryResults = getNextQueryResults(queryInfo, uriInfo);

        // cache the last results
        if (lastResult != null && lastResult.getNextUri() != null) {
            lastResultPath = lastResult.getNextUri().getPath();
        }
        else {
            lastResultPath = null;
        }
        lastResult = queryResults;
        return queryResults;
    }

    private ListenableFuture<?> queryDoneFuture(QueryState currentState)
    {
        if (currentState.isDone()) {
            return immediateFuture(null);
        }
        return Futures.transformAsync(queryManager.getStateChange(queryId, currentState), this::queryDoneFuture);
    }

    protected final URI createNextResultsUri(UriInfo uriInfo, String statementPath)
    {
        return uriInfo.getBaseUriBuilder().replacePath(statementPath).path(queryId.toString()).path(String.valueOf(resultId.incrementAndGet())).replaceQuery("").build();
    }

    static StatementStats toStatementStats(QueryInfo queryInfo)
    {
        QueryStats queryStats = queryInfo.getQueryStats();
        StageInfo outputStage = queryInfo.getOutputStage().orElse(null);

        return StatementStats.builder()
                .setState(queryInfo.getState().toString())
                .setQueued(queryInfo.getState() == QueryState.QUEUED)
                .setScheduled(queryInfo.isScheduled())
                .setNodes(globalUniqueNodes(outputStage).size())
                .setTotalSplits(queryStats.getTotalDrivers())
                .setQueuedSplits(queryStats.getQueuedDrivers())
                .setRunningSplits(queryStats.getRunningDrivers() + queryStats.getBlockedDrivers())
                .setCompletedSplits(queryStats.getCompletedDrivers())
                .setUserTimeMillis(queryStats.getTotalUserTime().toMillis())
                .setCpuTimeMillis(queryStats.getTotalCpuTime().toMillis())
                .setWallTimeMillis(queryStats.getTotalScheduledTime().toMillis())
                .setQueuedTimeMillis(queryStats.getQueuedTime().toMillis())
                .setElapsedTimeMillis(queryStats.getElapsedTime().toMillis())
                .setProcessedRows(queryStats.getRawInputPositions())
                .setProcessedBytes(queryStats.getRawInputDataSize().toBytes())
                .setPeakMemoryBytes(queryStats.getPeakUserMemoryReservation().toBytes())
                .setRootStage(toStageStats(outputStage))
                .build();
    }

    private static StageStats toStageStats(StageInfo stageInfo)
    {
        if (stageInfo == null) {
            return null;
        }

        com.facebook.presto.execution.StageStats stageStats = stageInfo.getStageStats();

        ImmutableList.Builder<StageStats> subStages = ImmutableList.builder();
        for (StageInfo subStage : stageInfo.getSubStages()) {
            subStages.add(toStageStats(subStage));
        }

        Set<String> uniqueNodes = new HashSet<>();
        for (TaskInfo task : stageInfo.getTasks()) {
            // todo add nodeId to TaskInfo
            URI uri = task.getTaskStatus().getSelf();
            uniqueNodes.add(uri.getHost() + ":" + uri.getPort());
        }

        return StageStats.builder()
                .setStageId(String.valueOf(stageInfo.getStageId().getId()))
                .setState(stageInfo.getState().toString())
                .setDone(stageInfo.getState().isDone())
                .setNodes(uniqueNodes.size())
                .setTotalSplits(stageStats.getTotalDrivers())
                .setQueuedSplits(stageStats.getQueuedDrivers())
                .setRunningSplits(stageStats.getRunningDrivers() + stageStats.getBlockedDrivers())
                .setCompletedSplits(stageStats.getCompletedDrivers())
                .setUserTimeMillis(stageStats.getTotalUserTime().toMillis())
                .setCpuTimeMillis(stageStats.getTotalCpuTime().toMillis())
                .setWallTimeMillis(stageStats.getTotalScheduledTime().toMillis())
                .setProcessedRows(stageStats.getRawInputPositions())
                .setProcessedBytes(stageStats.getRawInputDataSize().toBytes())
                .setSubStages(subStages.build())
                .build();
    }

    private static Set<String> globalUniqueNodes(StageInfo stageInfo)
    {
        if (stageInfo == null) {
            return ImmutableSet.of();
        }
        ImmutableSet.Builder<String> nodes = ImmutableSet.builder();
        for (TaskInfo task : stageInfo.getTasks()) {
            // todo add nodeId to TaskInfo
            URI uri = task.getTaskStatus().getSelf();
            nodes.add(uri.getHost() + ":" + uri.getPort());
        }

        for (StageInfo subStage : stageInfo.getSubStages()) {
            nodes.addAll(globalUniqueNodes(subStage));
        }
        return nodes.build();
    }

    static URI findCancelableLeafStage(QueryInfo queryInfo)
    {
        // if query is running, find the leaf-most running stage
        return queryInfo.getOutputStage().map(Query::findCancelableLeafStage).orElse(null);
    }

    private static URI findCancelableLeafStage(StageInfo stage)
    {
        // if this stage is already done, we can't cancel it
        if (stage.getState().isDone()) {
            return null;
        }

        // attempt to find a cancelable sub stage
        // check in reverse order since build side of a join will be later in the list
        for (StageInfo subStage : Lists.reverse(stage.getSubStages())) {
            URI leafStage = findCancelableLeafStage(subStage);
            if (leafStage != null) {
                return leafStage;
            }
        }

        // no matching sub stage, so return this stage
        return stage.getSelf();
    }

    static QueryError toQueryError(QueryInfo queryInfo)
    {
        FailureInfo failure = queryInfo.getFailureInfo();
        if (failure == null) {
            QueryState state = queryInfo.getState();
            if ((!state.isDone()) || (state == QueryState.FINISHED)) {
                return null;
            }
            log.warn("Query %s in state %s has no failure info", queryInfo.getQueryId(), state);
            failure = toFailure(new RuntimeException(format("Query is %s (reason unknown)", state))).toFailureInfo();
        }

        ErrorCode errorCode;
        if (queryInfo.getErrorCode() != null) {
            errorCode = queryInfo.getErrorCode();
        }
        else {
            errorCode = GENERIC_INTERNAL_ERROR.toErrorCode();
            log.warn("Failed query %s has no error code", queryInfo.getQueryId());
        }
        return new QueryError(
                failure.getMessage(),
                null,
                errorCode.getCode(),
                errorCode.getName(),
                errorCode.getType().toString(),
                failure.getErrorLocation(),
                failure);
    }
}
