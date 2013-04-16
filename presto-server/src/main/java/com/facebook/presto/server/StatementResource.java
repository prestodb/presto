package com.facebook.presto.server;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StageStats;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.execution.BufferInfo;
import com.facebook.presto.execution.ExecutionStats;
import com.facebook.presto.execution.QueryId;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.StageState;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.operator.Page;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;
import java.io.Closeable;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.execution.QueryInfo.queryIdGetter;
import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.execution.StageInfo.globalExecutionStats;
import static com.facebook.presto.execution.StageInfo.stageOnlyExecutionStats;
import static com.facebook.presto.execution.StageInfo.stageStateGetter;
import static com.facebook.presto.util.Failures.toFailure;
import static com.facebook.presto.util.Threads.threadsNamed;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.lang.String.format;
import static java.util.Collections.newSetFromMap;

@Path("/v1/statement")
public class StatementResource
{
    private static final Logger log = Logger.get(StatementResource.class);

    private static final Duration MAX_WAIT_TIME = new Duration(1, TimeUnit.SECONDS);
    private static final Ordering<Comparable<Duration>> WAIT_ORDERING = Ordering.natural().nullsLast();

    private final QueryManager queryManager;
    private final AsyncHttpClient httpClient;

    private final ConcurrentMap<QueryId, Query> queries = new ConcurrentHashMap<>();
    private final ScheduledExecutorService queryPurger = Executors.newSingleThreadScheduledExecutor(threadsNamed("query-purger-%d"));

    @Inject
    public StatementResource(QueryManager queryManager, @ForExecute AsyncHttpClient httpClient)
    {
        this.queryManager = checkNotNull(queryManager, "queryManager is null");
        this.httpClient = checkNotNull(httpClient, "httpClient is null");

        queryPurger.scheduleWithFixedDelay(new PurgeQueriesRunnable(queries.keySet(), queryManager), 200, 200, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void stop()
    {
        queryPurger.shutdownNow();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response createQuery(
            String statement,
            @HeaderParam(PRESTO_USER) String user,
            @HeaderParam(PRESTO_CATALOG) String catalog,
            @HeaderParam(PRESTO_SCHEMA) String schema,
            @Context UriInfo uriInfo)
            throws InterruptedException
    {
        assertRequest(!isNullOrEmpty(statement), "SQL statement is empty");
        assertRequest(!isNullOrEmpty(user), "User (%s) is empty", PRESTO_USER);
        assertRequest(!isNullOrEmpty(catalog), "Catalog (%s) is empty", PRESTO_CATALOG);
        assertRequest(!isNullOrEmpty(schema), "Schema (%s) is empty", PRESTO_SCHEMA);

        Session session = new Session(user, catalog, schema);
        Query query = new Query(session, statement, queryManager, httpClient);
        queries.put(query.getQueryId(), query);
        return Response.ok(query.getNextResults(uriInfo, new Duration(100, TimeUnit.MILLISECONDS))).build();
    }

    static void assertRequest(boolean expression, String format, Object... args)
    {
        if (!expression) {
            Response request = Response
                    .status(Status.BAD_REQUEST)
                    .type(MediaType.TEXT_PLAIN)
                    .entity(format(format, args))
                    .build();
            throw new WebApplicationException(request);
        }
    }

    @GET
    @Path("{queryId}/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getQueryResults(
            @PathParam("queryId") QueryId queryId,
            @PathParam("token") long token,
            @QueryParam("maxWait") Duration maxWait,
            @Context UriInfo uriInfo)
            throws InterruptedException
    {
        Query query = queries.get(queryId);
        if (query == null) {
            return Response.status(Status.NOT_FOUND).build();
        }

        Duration wait = WAIT_ORDERING.min(MAX_WAIT_TIME, maxWait);
        return Response.ok(query.getResults(token, uriInfo, wait)).build();
    }

    @DELETE
    @Path("{queryId}/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response cancelQuery(@PathParam("queryId") QueryId queryId,
            @PathParam("token") long token)
    {
        Query query = queries.get(queryId);
        if (query == null) {
            return Response.status(Status.NOT_FOUND).build();
        }
        query.close();
        return Response.noContent().build();
    }

    @ThreadSafe
    public static class Query
            implements Closeable
    {
        private final QueryManager queryManager;
        private final QueryId queryId;
        private final ExchangeClient exchangeClient;

        private final Set<URI> locations = newSetFromMap(new ConcurrentHashMap<URI, Boolean>());
        private final AtomicBoolean noMoreLocations = new AtomicBoolean();

        private final AtomicLong resultId = new AtomicLong();

        @GuardedBy("this")
        private QueryResults lastResult;

        @GuardedBy("this")
        private String lastResultPath;

        @GuardedBy("this")
        private List<Column> columns;

        public Query(Session session,
                String query,
                QueryManager queryManager,
                AsyncHttpClient httpClient)
        {
            checkNotNull(session, "session is null");
            checkNotNull(query, "query is null");
            checkNotNull(queryManager, "queryManager is null");

            this.queryManager = queryManager;

            QueryInfo queryInfo = queryManager.createQuery(session, query);
            queryId = queryInfo.getQueryId();
            exchangeClient = new ExchangeClient(100, 10, 3, httpClient, locations, noMoreLocations);
        }

        @Override
        public void close()
        {
            queryManager.cancelQuery(queryId);
        }

        public QueryId getQueryId()
        {
            return queryId;
        }

        public synchronized QueryResults getResults(long token, UriInfo uriInfo, Duration maxWaitTime)
                throws InterruptedException
        {
            // is the a repeated request for the last results?
            String requestedPath = uriInfo.getAbsolutePath().getPath();
            if (lastResultPath != null && requestedPath.equals(lastResultPath)) {
                // tell query manager we are still interested in the query
                queryManager.getQueryInfo(queryId, false);
                return lastResult;
            }

            if (token < resultId.get()) {
                throw new WebApplicationException(Status.GONE);
            }

            // if this is not a request for the next results, return not found
            if (lastResult.getNextUri() == null || !requestedPath.equals(lastResult.getNextUri().getPath())) {
                // unknown token
                throw new WebApplicationException(Status.NOT_FOUND);
            }

            return getNextResults(uriInfo, maxWaitTime);
        }

        public synchronized QueryResults getNextResults(UriInfo uriInfo, Duration maxWaitTime)
                throws InterruptedException
        {
            Iterable<List<Object>> data = getData(maxWaitTime);

            // get the query info before returning
            QueryInfo queryInfo = queryManager.getQueryInfo(queryId, false);

            // close exchange client if the query has failed
            if (queryInfo.getState().isDone() && (queryInfo.getState() != QueryState.FINISHED)) {
                exchangeClient.close();
            }

            // only return a next if the query is not done or there is more data to send (due to buffering)
            URI nextResultsUri = null;
            if ((!queryInfo.getState().isDone()) || (!exchangeClient.isClosed())) {
                nextResultsUri = createNextResultsUri(uriInfo);
            }

            // first time through, self is null
            QueryResults queryResults = new QueryResults(
                    queryId.toString(),
                    uriInfo.getRequestUriBuilder().replaceQuery("").replacePath(queryInfo.getSelf().getPath()).build(),
                    findCancelableLeafStage(queryInfo),
                    nextResultsUri,
                    columns,
                    data,
                    toStatementStats(queryInfo),
                    toQueryError(queryInfo));

            // cache the last results
            if (lastResult != null) {
                lastResultPath = lastResult.getNextUri().getPath();
            }
            else {
                lastResultPath = null;
            }
            lastResult = queryResults;
            return queryResults;
        }

        private synchronized Iterable<List<Object>> getData(Duration maxWaitTime)
                throws InterruptedException
        {
            QueryInfo queryInfo = queryManager.getQueryInfo(queryId, false);

            StageInfo outputStage = queryInfo.getOutputStage();
            if (outputStage == null) {
                // query hasn't finished planning
                return null;
            }

            if (columns == null) {
                columns = createColumnsList(queryInfo);
            }

            updateExchangeClient(outputStage);

            // fetch next page
            Page page = exchangeClient.getNextPage(maxWaitTime);
            if (page == null) {
                // no data this time
                return null;
            }

            return new RowIterable(page);
        }

        private synchronized void updateExchangeClient(StageInfo outputStage)
        {
            // update the exchange client with any additional locations
            for (TaskInfo taskInfo : outputStage.getTasks()) {
                List<BufferInfo> buffers = taskInfo.getOutputBuffers().getBuffers();
                Preconditions.checkState(buffers.size() == 1,
                        "Expected a single output buffer for task %s, but found %s",
                        taskInfo.getTaskId(),
                        buffers);

                String bufferId = Iterables.getOnlyElement(buffers).getBufferId();
                URI uri = uriBuilderFrom(taskInfo.getSelf()).appendPath("results").appendPath(bufferId).build();
                locations.add(uri);
            }
            if ((outputStage.getState() != StageState.PLANNED) && (outputStage.getState() != StageState.SCHEDULING)) {
                noMoreLocations.set(true);
            }
        }

        private synchronized URI createNextResultsUri(UriInfo uriInfo)
        {
            return uriInfo.getBaseUriBuilder().replacePath("/v1/statement").path(queryId.toString()).path(String.valueOf(resultId.incrementAndGet())).replaceQuery("").build();
        }

        private static List<Column> createColumnsList(QueryInfo queryInfo)
        {
            List<String> names = queryInfo.getFieldNames();
            ArrayList<Type> types = new ArrayList<>();
            for (TupleInfo tupleInfo : queryInfo.getOutputStage().getTupleInfos()) {
                types.addAll(tupleInfo.getTypes());
            }

            checkArgument(names.size() == types.size(), "names and types size mismatch");

            ImmutableList.Builder<Column> list = ImmutableList.builder();
            for (int i = 0; i < names.size(); i++) {
                String name = names.get(i);
                Type type = types.get(i);
                switch (type) {
                    case FIXED_INT_64:
                        list.add(new Column(name, "bigint"));
                        break;
                    case DOUBLE:
                        list.add(new Column(name, "double"));
                        break;
                    case VARIABLE_BINARY:
                        list.add(new Column(name, "varchar"));
                        break;
                    default:
                        throw new IllegalArgumentException("unhandled type: " + type);
                }
            }
            return list.build();
        }

        private static StatementStats toStatementStats(QueryInfo queryInfo)
        {
            ExecutionStats executionStats = globalExecutionStats(queryInfo.getOutputStage());

            return StatementStats.builder()
                    .setState(queryInfo.getState().toString())
                    .setScheduled(isScheduled(queryInfo))
                    .setNodes(globalUniqueNodes(queryInfo.getOutputStage()).size())
                    .setTotalSplits(executionStats.getSplits())
                    .setQueuedSplits(executionStats.getQueuedSplits())
                    .setRunningSplits(executionStats.getRunningSplits())
                    .setCompletedSplits(executionStats.getCompletedSplits())
                    .setUserTimeMillis((long) executionStats.getSplitUserTime().toMillis())
                    .setCpuTimeMillis((long) executionStats.getSplitCpuTime().toMillis())
                    .setWallTimeMillis((long) executionStats.getSplitWallTime().toMillis())
                    .setProcessedRows(executionStats.getCompletedPositionCount())
                    .setProcessedBytes(executionStats.getCompletedDataSize().toBytes())
                    .setRootStage(toStageStats(queryInfo.getOutputStage()))
                    .build();
        }

        private static StageStats toStageStats(StageInfo stageInfo)
        {
            if (stageInfo == null) {
                return null;
            }

            ExecutionStats executionStats = stageOnlyExecutionStats(stageInfo);

            ImmutableList.Builder<StageStats> subStages = ImmutableList.builder();
            for (StageInfo subStage : stageInfo.getSubStages()) {
                subStages.add(toStageStats(subStage));
            }

            Set<String> uniqueNodes = new HashSet<>();
            for (TaskInfo task : stageInfo.getTasks()) {
                // todo add nodeId to TaskInfo
                URI uri = task.getSelf();
                uniqueNodes.add(uri.getHost() + ":" + uri.getPort());
            }

            return StageStats.builder()
                    .setStageId(String.valueOf(stageInfo.getStageId().getId()))
                    .setState(stageInfo.getState().toString())
                    .setDone(stageInfo.getState().isDone())
                    .setNodes(uniqueNodes.size())
                    .setTotalSplits(executionStats.getSplits())
                    .setQueuedSplits(executionStats.getQueuedSplits())
                    .setRunningSplits(executionStats.getRunningSplits())
                    .setCompletedSplits(executionStats.getCompletedSplits())
                    .setUserTimeMillis((long) executionStats.getSplitUserTime().toMillis())
                    .setCpuTimeMillis((long) executionStats.getSplitCpuTime().toMillis())
                    .setWallTimeMillis((long) executionStats.getSplitWallTime().toMillis())
                    .setProcessedRows(executionStats.getCompletedPositionCount())
                    .setProcessedBytes(executionStats.getCompletedDataSize().toBytes())
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
                URI uri = task.getSelf();
                nodes.add(uri.getHost() + ":" + uri.getPort());
            }

            for (StageInfo subStage : stageInfo.getSubStages()) {
                nodes.addAll(globalUniqueNodes(subStage));
            }
            return nodes.build();
        }

        private static boolean isScheduled(QueryInfo queryInfo)
        {
            StageInfo stage = queryInfo.getOutputStage();
            if (stage == null) {
                return false;
            }
            return IterableTransformer.on(getAllStages(stage))
                    .transform(stageStateGetter())
                    .all(isStageRunningOrDone());
        }

        private static Predicate<StageState> isStageRunningOrDone()
        {
            return new Predicate<StageState>()
            {
                @Override
                public boolean apply(StageState state)
                {
                    return (state == StageState.RUNNING) || state.isDone();
                }
            };
        }

        private static URI findCancelableLeafStage(QueryInfo queryInfo)
        {
            if (queryInfo.getOutputStage() == null) {
                // query is not running yet, cannot cancel leaf stage
                return null;
            }

            // query is running, find the leaf-most running stage
            return findCancelableLeafStage(queryInfo.getOutputStage());
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

        private static QueryError toQueryError(QueryInfo queryInfo)
        {
            FailureInfo failure = queryInfo.getFailureInfo();
            if (failure == null) {
                QueryState state = queryInfo.getState();
                if ((!state.isDone()) || (state == QueryState.FINISHED)) {
                    return null;
                }
                log.warn("Query %s in state %s has no failure info", queryInfo.getQueryId(), state);
                failure = toFailure(new RuntimeException(format("Query is %s (reason unknown)", state)));
            }
            return new QueryError(failure.getMessage(), null, 0, failure.getErrorLocation(), failure);
        }

        private static class RowIterable
                implements Iterable<List<Object>>
        {
            private final Page page;

            private RowIterable(Page page)
            {
                this.page = checkNotNull(page, "page is null");
            }

            @Override
            public Iterator<List<Object>> iterator()
            {
                return new RowIterator(page);
            }
        }

        private static class RowIterator
                extends AbstractIterator<List<Object>>
        {
            private final BlockCursor[] cursors;
            private final int columnCount;

            private RowIterator(Page page)
            {
                int columnCount = 0;
                cursors = new BlockCursor[page.getChannelCount()];
                for (int channel = 0; channel < cursors.length; channel++) {
                    cursors[channel] = page.getBlock(channel).cursor();
                    columnCount = cursors[channel].getTupleInfo().getFieldCount();
                }
                this.columnCount = columnCount;
            }

            @Override
            protected List<Object> computeNext()
            {
                List<Object> row = new ArrayList<>(columnCount);
                for (BlockCursor cursor : cursors) {
                    if (!cursor.advanceNextPosition()) {
                        Preconditions.checkState(row.isEmpty(), "Page is unaligned");
                        return endOfData();
                    }

                    row.addAll(cursor.getTuple().toValues());
                }
                return row;
            }
        }
    }

    private static class PurgeQueriesRunnable
            implements Runnable
    {
        private final Set<QueryId> queryIds;
        private final QueryManager queryManager;

        public PurgeQueriesRunnable(Set<QueryId> queryIds, QueryManager queryManager)
        {
            this.queryIds = queryIds;
            this.queryManager = queryManager;
        }

        @Override
        public void run()
        {
            try {
                // Queries are added to the query manager before being recorded in queryIds set.
                // Therefore, we take a snapshot if queryIds before getting the live queries
                // from the query manager.  Then we remove only the queries in the snapshot and
                // not live queries set.  If we did this in the other order, a query could be
                // registered between fetching the live queries and inspecting the queryIds set.

                Set<QueryId> queryIdsSnapshot = ImmutableSet.copyOf(queryIds);
                // do not call queryManager.getQueryInfo() since it updates the heartbeat time
                Set<QueryId> liveQueries = ImmutableSet.copyOf(transform(queryManager.getAllQueryInfo(), queryIdGetter()));

                Set<QueryId> deadQueries = Sets.difference(queryIdsSnapshot, liveQueries);
                for (QueryId deadQuery : deadQueries) {
                    queryIds.remove(deadQuery);
                    log.debug("Removed expired query %s", deadQuery);
                }
            }
            catch (Throwable e) {
                log.warn(e, "Error removing old queries");
            }
        }
    }
}
