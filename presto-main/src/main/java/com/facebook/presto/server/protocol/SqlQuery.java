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

import com.facebook.presto.Session;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StageStats;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryOutputInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.memory.context.SimpleLocalMemoryContext;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.operator.ExchangeClientSupplier;
import com.facebook.presto.server.ForStatementResource;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.SystemSessionProperties.isExchangeCompressionEnabled;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.addTimeout;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class SqlQuery
        extends AbstractQuery<QueryOutputInfo>
{
    private static final long DESIRED_RESULT_BYTES = new DataSize(1, MEGABYTE).toBytes();

    private final Executor resultsProcessorExecutor;
    private final ScheduledExecutorService timeoutExecutor;

    @GuardedBy("this")
    private final ExchangeClient exchangeClient;

    @GuardedBy("this")
    private final PagesSerde serde;

    private final AtomicLong resultId = new AtomicLong();

    @GuardedBy("this")
    private QueryResults lastResult;

    @GuardedBy("this")
    private String lastResultPath;

    @GuardedBy("this")
    private List<Column> columns;

    @GuardedBy("this")
    private List<Type> types;

    @GuardedBy("this")
    private Long updateCount;

    static SqlQuery create(
            QueryId queryId,
            SessionContext sessionContext,
            String query,
            QueryManager<QueryOutputInfo> queryManager,
            SessionPropertyManager sessionPropertyManager,
            ExchangeClient exchangeClient,
            Executor dataProcessorExecutor,
            ScheduledExecutorService timeoutExecutor,
            BlockEncodingSerde blockEncodingSerde)
    {
        requireNonNull(queryManager, "queryManager is null");
        requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");

        QueryInfo info = queryManager.createQuery(queryId, sessionContext, query);
        SqlQuery result = new SqlQuery(
                queryId,
                info.getSession().toSession(sessionPropertyManager),
                queryManager,
                exchangeClient,
                dataProcessorExecutor,
                timeoutExecutor,
                blockEncodingSerde);

        result.queryManager.addStateChangeListener(result.getQueryId(), state -> {
            if (state.isDone()) {
                QueryInfo queryInfo = queryManager.getQueryInfo(result.getQueryId());
                result.closeExchangeClientIfNecessary(queryInfo);
            }
        });

        return result;
    }

    private SqlQuery(
            QueryId queryId,
            Session session,
            QueryManager<QueryOutputInfo> queryManager,
            ExchangeClient exchangeClient,
            Executor resultsProcessorExecutor,
            ScheduledExecutorService timeoutExecutor,
            BlockEncodingSerde blockEncodingSerde)
    {
        super(queryManager, queryId, session);
        this.exchangeClient = requireNonNull(exchangeClient, "exchangeClient is null");
        this.resultsProcessorExecutor = requireNonNull(resultsProcessorExecutor, "resultsProcessorExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        this.serde = new PagesSerdeFactory(requireNonNull(blockEncodingSerde, "serde is null"), isExchangeCompressionEnabled(session)).createPagesSerde();

        queryManager.addOutputListener(getQueryId(), this::setOutput);
    }

    @Override
    public synchronized void dispose()
    {
        exchangeClient.close();
    }

    @Override
    public synchronized ListenableFuture<QueryResults> waitForResults(OptionalLong token, UriInfo uriInfo, Duration wait)
    {
        // before waiting, check if this request has already been processed and cached
        if (token.isPresent()) {
            Optional<QueryResults> cachedResult = getCachedResult(token.getAsLong(), uriInfo);
            if (cachedResult.isPresent()) {
                return immediateFuture(cachedResult.get());
            }
        }

        ListenableFuture<?> future;
        if (!exchangeClient.isClosed()) {
            future = exchangeClient.isBlocked();
        }
        else {
            future = getFutureStateChange();
        }

        // wait for a results data or query to finish, up to the wait timeout
        ListenableFuture<?> futureStateChange = addTimeout(
                future,
                () -> null,
                wait,
                timeoutExecutor);

        // when state changes, fetch the next result
        return Futures.transform(futureStateChange, ignored -> getNextResult(token, uriInfo), resultsProcessorExecutor);
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

        // Remove as many pages as possible from the exchange until just greater than DESIRED_RESULT_BYTES
        // NOTE: it is critical that query results are created for the pages removed from the exchange
        // client while holding the lock because the query may transition to the finished state when the
        // last page is removed.  If another thread observes this state before the response is cached
        // the pages will be lost.
        Iterable<List<Object>> data = null;
        try {
            ImmutableList.Builder<RowIterable> pages = ImmutableList.builder();
            long bytes = 0;
            long rows = 0;
            while (bytes < DESIRED_RESULT_BYTES) {
                SerializedPage serializedPage = exchangeClient.pollPage();
                if (serializedPage == null) {
                    break;
                }

                Page page = serde.deserialize(serializedPage);
                bytes += page.getSizeInBytes();
                rows += page.getPositionCount();
                pages.add(new RowIterable(session.toConnectorSession(), types, page));
            }
            if (rows > 0) {
                // client implementations do not properly handle empty list of data
                data = Iterables.concat(pages.build());
            }
        }
        catch (Throwable cause) {
            queryManager.failQuery(queryId, cause);
        }

        // get the query info before returning
        // force update if query manager is closed
        QueryInfo queryInfo = queryManager.getQueryInfo(queryId);
        queryManager.recordHeartbeat(queryId);

        // TODO: figure out a better way to do this
        // grab the update count for non-queries
        if ((data != null) && (queryInfo.getUpdateType() != null) && (updateCount == null) &&
                (columns.size() == 1) && (columns.get(0).getType().equals(StandardTypes.BIGINT))) {
            Iterator<List<Object>> iterator = data.iterator();
            if (iterator.hasNext()) {
                Number number = (Number) iterator.next().get(0);
                if (number != null) {
                    updateCount = number.longValue();
                }
            }
        }

        closeExchangeClientIfNecessary(queryInfo);

        // for queries with no output, return a fake result for clients that require it
        if ((queryInfo.getState() == QueryState.FINISHED) && !queryInfo.getOutputStage().isPresent()) {
            columns = ImmutableList.of(new Column("result", BooleanType.BOOLEAN));
            data = ImmutableSet.of(ImmutableList.of(true));
        }

        // only return a next if the query is not done or there is more data to send (due to buffering)
        URI nextResultsUri = null;
        if (!queryInfo.isFinalQueryInfo() || !exchangeClient.isClosed()) {
            nextResultsUri = createNextResultsUri(uriInfo);
        }

        updateInfo(queryInfo);

        // first time through, self is null
        QueryResults queryResults = new QueryResults(
                queryId.toString(),
                uriInfo.getRequestUriBuilder().replaceQuery(queryId.toString()).replacePath("query.html").build(),
                findCancelableLeafStage(queryInfo),
                nextResultsUri,
                columns,
                data,
                toStatementStats(queryInfo),
                toQueryError(queryInfo, QueryState.FINISHED),
                queryInfo.getUpdateType(),
                updateCount);

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

    private synchronized void closeExchangeClientIfNecessary(QueryInfo queryInfo)
    {
        // Close the exchange client if the query has failed, or if the query
        // is done and it does not have an output stage. The latter happens
        // for data definition executions, as those do not have output.
        if ((queryInfo.getState() == QueryState.FAILED) ||
                (queryInfo.getState().isDone() && !queryInfo.getOutputStage().isPresent())) {
            exchangeClient.close();
        }
    }

    @Override
    synchronized void setOutput(QueryOutputInfo outputInfo)
    {
        // if first callback, set column names
        if (columns == null) {
            List<String> columnNames = outputInfo.getColumnNames();
            List<Type> columnTypes = outputInfo.getColumnTypes();
            checkArgument(columnNames.size() == columnTypes.size(), "Column names and types size mismatch");

            ImmutableList.Builder<Column> list = ImmutableList.builder();
            for (int i = 0; i < columnNames.size(); i++) {
                list.add(new Column(columnNames.get(i), columnTypes.get(i)));
            }
            columns = list.build();
            types = outputInfo.getColumnTypes();
        }

        for (URI outputLocation : outputInfo.getBufferLocations()) {
            exchangeClient.addLocation(outputLocation);
        }
        if (outputInfo.isNoMoreBufferLocations()) {
            exchangeClient.noMoreLocations();
        }
    }

    private synchronized URI createNextResultsUri(UriInfo uriInfo)
    {
        return uriInfo.getBaseUriBuilder().replacePath("/v1/statement").path(queryId.toString()).path(String.valueOf(resultId.incrementAndGet())).replaceQuery("").build();
    }

    private static StatementStats toStatementStats(QueryInfo queryInfo)
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

    private static URI findCancelableLeafStage(QueryInfo queryInfo)
    {
        // if query is running, find the leaf-most running stage
        return queryInfo.getOutputStage().map(SqlQuery::findCancelableLeafStage).orElse(null);
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

    public static class SqlQueryFactory
            implements QueryFactory<SqlQuery>
    {
        private final QueryManager<QueryOutputInfo> queryManager;
        private final SessionPropertyManager sessionPropertyManager;
        private final ExchangeClientSupplier exchangeClientSupplier;
        private final Executor dataProcessorExecutor;
        private final ScheduledExecutorService timeoutExecutor;
        private final BlockEncodingSerde blockEncodingSerde;

        @Inject
        SqlQueryFactory(
                QueryManager<QueryOutputInfo> queryManager,
                SessionPropertyManager sessionPropertyManager,
                ExchangeClientSupplier exchangeClientSupplier,
                @ForStatementResource BoundedExecutor dataProcessorExecutor,
                @ForStatementResource ScheduledExecutorService timeoutExecutor,
                BlockEncodingSerde blockEncodingSerde)
        {
            this.queryManager = requireNonNull(queryManager, "queryManager is null");
            this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "queryManager is null");
            this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
            this.dataProcessorExecutor = requireNonNull(dataProcessorExecutor, "responseExecutor is null");
            this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
            this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        }

        @Override
        public SqlQuery create(QueryId queryId, SessionContext sessionContext, String query)
        {
            return SqlQuery.create(
                    queryId,
                    sessionContext,
                    query,
                    queryManager,
                    sessionPropertyManager,
                    exchangeClientSupplier.get(new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext())),
                    dataProcessorExecutor,
                    timeoutExecutor,
                    blockEncodingSerde);
        }
    }
}
