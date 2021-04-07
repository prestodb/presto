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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionInfo;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.MoreFutures.addTimeout;
import static com.facebook.presto.SystemSessionProperties.getQueryRetryLimit;
import static com.facebook.presto.SystemSessionProperties.getQueryRetryMaxExecutionTime;
import static com.facebook.presto.SystemSessionProperties.getTargetResultSize;
import static com.facebook.presto.SystemSessionProperties.isExchangeChecksumEnabled;
import static com.facebook.presto.SystemSessionProperties.isExchangeCompressionEnabled;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.WAITING_FOR_PREREQUISITES;
import static com.facebook.presto.server.protocol.QueryResourceUtil.toStatementStats;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
class Query
{
    private static final Logger log = Logger.get(Query.class);

    private final QueryManager queryManager;
    private final TransactionManager transactionManager;
    private final QueryId queryId;
    private final Session session;
    private final String slug;

    @GuardedBy("this")
    private final ExchangeClient exchangeClient;

    private final Executor resultsProcessorExecutor;
    private final ScheduledExecutorService timeoutExecutor;

    private final PagesSerde serde;
    private final RetryCircuitBreaker retryCircuitBreaker;

    @GuardedBy("this")
    private OptionalLong nextToken = OptionalLong.of(0);

    @GuardedBy("this")
    private QueryResults lastResult;

    @GuardedBy("this")
    private long lastToken = -1;

    @GuardedBy("this")
    private List<Column> columns;

    @GuardedBy("this")
    private List<Type> types;

    @GuardedBy("this")
    private Optional<String> setCatalog = Optional.empty();

    @GuardedBy("this")
    private Optional<String> setSchema = Optional.empty();

    @GuardedBy("this")
    private Map<String, String> setSessionProperties = ImmutableMap.of();

    @GuardedBy("this")
    private Set<String> resetSessionProperties = ImmutableSet.of();

    @GuardedBy("this")
    private Map<String, SelectedRole> setRoles = ImmutableMap.of();

    @GuardedBy("this")
    private Map<String, String> addedPreparedStatements = ImmutableMap.of();

    @GuardedBy("this")
    private Set<String> deallocatedPreparedStatements = ImmutableSet.of();

    @GuardedBy("this")
    private Optional<TransactionId> startedTransactionId = Optional.empty();

    @GuardedBy("this")
    private boolean clearTransactionId;

    @GuardedBy("this")
    private Long updateCount;

    @GuardedBy("this")
    private boolean hasProducedResult;

    @GuardedBy("this")
    private Map<SqlFunctionId, SqlInvokedFunction> addedSessionFunctions = ImmutableMap.of();

    @GuardedBy("this")
    private Set<SqlFunctionId> removedSessionFunctions = ImmutableSet.of();

    public static Query create(
            Session session,
            String slug,
            QueryManager queryManager,
            TransactionManager transactionManager,
            ExchangeClient exchangeClient,
            Executor dataProcessorExecutor,
            ScheduledExecutorService timeoutExecutor,
            BlockEncodingSerde blockEncodingSerde,
            RetryCircuitBreaker retryCircuitBreaker)
    {
        Query result = new Query(session, slug, queryManager, transactionManager, exchangeClient, dataProcessorExecutor, timeoutExecutor, blockEncodingSerde, retryCircuitBreaker);

        result.queryManager.addOutputInfoListener(result.getQueryId(), result::setQueryOutputInfo);

        result.queryManager.addStateChangeListener(result.getQueryId(), state -> {
            if (state.isDone()) {
                QueryInfo queryInfo = queryManager.getFullQueryInfo(result.getQueryId());
                result.closeExchangeClientIfNecessary(queryInfo);
            }
        });

        return result;
    }

    private Query(
            Session session,
            String slug,
            QueryManager queryManager,
            TransactionManager transactionManager,
            ExchangeClient exchangeClient,
            Executor resultsProcessorExecutor,
            ScheduledExecutorService timeoutExecutor,
            BlockEncodingSerde blockEncodingSerde,
            RetryCircuitBreaker retryCircuitBreaker)
    {
        requireNonNull(session, "session is null");
        requireNonNull(slug, "slug is null");
        requireNonNull(queryManager, "queryManager is null");
        requireNonNull(transactionManager, "transactionManager is null");
        requireNonNull(exchangeClient, "exchangeClient is null");
        requireNonNull(resultsProcessorExecutor, "resultsProcessorExecutor is null");
        requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        requireNonNull(blockEncodingSerde, "serde is null");
        requireNonNull(retryCircuitBreaker, "retryCircuitBreaker is null");

        this.queryManager = queryManager;
        this.transactionManager = transactionManager;

        this.queryId = session.getQueryId();
        this.session = session;
        this.slug = slug;
        this.exchangeClient = exchangeClient;
        this.resultsProcessorExecutor = resultsProcessorExecutor;
        this.timeoutExecutor = timeoutExecutor;

        this.serde = new PagesSerdeFactory(blockEncodingSerde, isExchangeCompressionEnabled(session), isExchangeChecksumEnabled(session)).createPagesSerde();
        this.retryCircuitBreaker = retryCircuitBreaker;
    }

    public void cancel()
    {
        queryManager.cancelQuery(queryId);
        dispose();
    }

    public synchronized void dispose()
    {
        exchangeClient.close();
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public boolean isSlugValid(String slug)
    {
        return this.slug.equals(slug);
    }

    public synchronized Optional<String> getSetCatalog()
    {
        return setCatalog;
    }

    public synchronized Optional<String> getSetSchema()
    {
        return setSchema;
    }

    public synchronized Map<String, String> getSetSessionProperties()
    {
        return setSessionProperties;
    }

    public synchronized Set<String> getResetSessionProperties()
    {
        return resetSessionProperties;
    }

    public synchronized Map<String, SelectedRole> getSetRoles()
    {
        return setRoles;
    }

    public synchronized Map<String, String> getAddedPreparedStatements()
    {
        return addedPreparedStatements;
    }

    public synchronized Set<String> getDeallocatedPreparedStatements()
    {
        return deallocatedPreparedStatements;
    }

    public synchronized Optional<TransactionId> getStartedTransactionId()
    {
        return startedTransactionId;
    }

    public synchronized boolean isClearTransactionId()
    {
        return clearTransactionId;
    }

    public synchronized Map<SqlFunctionId, SqlInvokedFunction> getAddedSessionFunctions()
    {
        return addedSessionFunctions;
    }

    public synchronized Set<SqlFunctionId> getRemovedSessionFunctions()
    {
        return removedSessionFunctions;
    }

    public synchronized ListenableFuture<QueryResults> waitForResults(long token, UriInfo uriInfo, String scheme, Duration wait, DataSize targetResultSize)
    {
        // before waiting, check if this request has already been processed and cached
        Optional<QueryResults> cachedResult = getCachedResult(token);
        if (cachedResult.isPresent()) {
            return immediateFuture(cachedResult.get());
        }

        // wait for a results data or query to finish, up to the wait timeout
        ListenableFuture<?> futureStateChange = addTimeout(
                getFutureStateChange(),
                () -> null,
                wait,
                timeoutExecutor);

        // when state changes, fetch the next result
        return Futures.transform(futureStateChange, ignored -> getNextResultWithRetry(token, uriInfo, scheme, targetResultSize), resultsProcessorExecutor);
    }

    private synchronized ListenableFuture<?> getFutureStateChange()
    {
        // if the exchange client is open, wait for data
        if (!exchangeClient.isClosed()) {
            return exchangeClient.isBlocked();
        }

        // otherwise, wait for the query to finish
        queryManager.recordHeartbeat(queryId);
        try {
            return queryDoneFuture(queryManager.getQueryState(queryId));
        }
        catch (NoSuchElementException e) {
            return immediateFuture(null);
        }
    }

    private synchronized Optional<QueryResults> getCachedResult(long token)
    {
        // is this the first request?
        if (lastResult == null) {
            return Optional.empty();
        }

        // is the a repeated request for the last results?
        if (token == lastToken) {
            // tell query manager we are still interested in the query
            queryManager.recordHeartbeat(queryId);
            return Optional.of(lastResult);
        }

        // if this is a result before the lastResult, the data is gone
        if (token < lastToken) {
            throw new WebApplicationException(Response.Status.GONE);
        }

        // if this is a request for a result after the end of the stream, return not found
        if (!nextToken.isPresent()) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }

        // if this is not a request for the next results, return not found
        if (token != nextToken.getAsLong()) {
            // unknown token
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }

        return Optional.empty();
    }

    private synchronized QueryResults getNextResultWithRetry(long token, UriInfo uriInfo, String scheme, DataSize targetResultSize)
    {
        QueryResults queryResults = getNextResult(token, uriInfo, scheme, targetResultSize);
        if (queryResults.getError() == null || !queryResults.getError().isRetriable()) {
            return queryResults;
        }

        // check if we have exceeded the global limit
        retryCircuitBreaker.incrementFailure();
        if (!retryCircuitBreaker.isRetryAllowed() || hasProducedResult) {
            return queryResults;
        }

        // check if we have exceeded the local limit
        if (queryManager.getQueryRetryCount(queryId) >= getQueryRetryLimit(session) ||
                queryManager.getQueryInfo(queryId).getQueryStats().getExecutionTime().toMillis() > getQueryRetryMaxExecutionTime(session).toMillis()) {
            return queryResults;
        }

        // no support for transactions
        if (session.getTransactionId().isPresent() &&
                !transactionManager.getOptionalTransactionInfo(session.getRequiredTransactionId()).map(TransactionInfo::isAutoCommitContext).orElse(true)) {
            return queryResults;
        }

        // build a new query with next uri
        // we expect failed nodes have been removed from discovery server upon query failure
        return new QueryResults(
                queryId.toString(),
                queryResults.getInfoUri(),
                queryResults.getPartialCancelUri(),
                createRetryUri(scheme, uriInfo),
                queryResults.getColumns(),
                null,
                StatementStats.builder()
                        .setState(WAITING_FOR_PREREQUISITES.toString())
                        .setWaitingForPrerequisites(true)
                        .build(),
                null,
                ImmutableList.of(),
                queryResults.getUpdateType(),
                queryResults.getUpdateCount());
    }

    private synchronized QueryResults getNextResult(long token, UriInfo uriInfo, String scheme, DataSize targetResultSize)
    {
        // check if the result for the token have already been created
        Optional<QueryResults> cachedResult = getCachedResult(token);
        if (cachedResult.isPresent()) {
            return cachedResult.get();
        }

        verify(nextToken.isPresent(), "Can not generate next result when next token is not present");
        verify(token == nextToken.getAsLong(), "Expected token to equal next token");
        URI queryHtmlUri = uriInfo.getRequestUriBuilder()
                .scheme(scheme)
                .replacePath("ui/query.html")
                .replaceQuery(queryId.toString())
                .build();

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
            long targetResultBytes = targetResultSize.toBytes();
            while (bytes < targetResultBytes) {
                SerializedPage serializedPage = exchangeClient.pollPage();
                if (serializedPage == null) {
                    break;
                }

                Page page = serde.deserialize(serializedPage);
                bytes += page.getLogicalSizeInBytes();
                rows += page.getPositionCount();
                pages.add(new RowIterable(session.toConnectorSession(), types, page));
            }
            if (rows > 0) {
                // client implementations do not properly handle empty list of data
                data = Iterables.concat(pages.build());
                hasProducedResult = true;
            }
        }
        catch (Throwable cause) {
            queryManager.failQuery(queryId, cause);
        }

        // get the query info before returning
        // force update if query manager is closed
        QueryInfo queryInfo = queryManager.getFullQueryInfo(queryId);
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

        // advance next token
        // only return a next if
        // (1) the query is not done AND the query state is not FAILED
        //   OR
        // (2)there is more data to send (due to buffering)
        if ((!queryInfo.isFinalQueryInfo() && queryInfo.getState() != FAILED) || !exchangeClient.isClosed()) {
            nextToken = OptionalLong.of(token + 1);
        }
        else {
            nextToken = OptionalLong.empty();
        }

        URI nextResultsUri = null;
        if (nextToken.isPresent()) {
            nextResultsUri = createNextResultsUri(scheme, uriInfo, nextToken.getAsLong());
        }

        // update catalog, schema, and path
        setCatalog = queryInfo.getSetCatalog();
        setSchema = queryInfo.getSetSchema();

        // update setSessionProperties
        setSessionProperties = queryInfo.getSetSessionProperties();
        resetSessionProperties = queryInfo.getResetSessionProperties();

        // update setRoles
        setRoles = queryInfo.getSetRoles();

        // update preparedStatements
        addedPreparedStatements = queryInfo.getAddedPreparedStatements();
        deallocatedPreparedStatements = queryInfo.getDeallocatedPreparedStatements();

        // update startedTransactionId
        startedTransactionId = queryInfo.getStartedTransactionId();
        clearTransactionId = queryInfo.isClearTransactionId();

        // update sessionFunctions
        addedSessionFunctions = queryInfo.getAddedSessionFunctions();
        removedSessionFunctions = queryInfo.getRemovedSessionFunctions();

        // first time through, self is null
        QueryResults queryResults = new QueryResults(
                queryId.toString(),
                queryHtmlUri,
                findCancelableLeafStage(queryInfo),
                nextResultsUri,
                columns,
                data,
                toStatementStats(queryInfo),
                toQueryError(queryInfo),
                queryInfo.getWarnings(),
                queryInfo.getUpdateType(),
                updateCount);

        // cache the new result
        lastToken = token;
        lastResult = queryResults;

        return queryResults;
    }

    private synchronized void closeExchangeClientIfNecessary(QueryInfo queryInfo)
    {
        // Close the exchange client if the query has failed, or if the query
        // is done and it does not have an output stage. The latter happens
        // for data definition executions, as those do not have output.
        if ((queryInfo.getState() == FAILED) ||
                (queryInfo.getState().isDone() && !queryInfo.getOutputStage().isPresent())) {
            exchangeClient.close();
        }
    }

    private synchronized void setQueryOutputInfo(QueryExecution.QueryOutputInfo outputInfo)
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

        outputInfo.getBufferLocations().forEach(exchangeClient::addLocation);
        if (outputInfo.isNoMoreBufferLocations()) {
            exchangeClient.noMoreLocations();
        }
    }

    private ListenableFuture<?> queryDoneFuture(QueryState currentState)
    {
        if (currentState.isDone()) {
            return immediateFuture(null);
        }
        return Futures.transformAsync(queryManager.getStateChange(queryId, currentState), this::queryDoneFuture, directExecutor());
    }

    private synchronized URI createNextResultsUri(String scheme, UriInfo uriInfo, long nextToken)
    {
        UriBuilder uri = uriInfo.getBaseUriBuilder()
                .scheme(scheme)
                .replacePath("/v1/statement/executing")
                .path(queryId.toString())
                .path(String.valueOf(nextToken))
                .replaceQuery("")
                .queryParam("slug", this.slug);
        Optional<DataSize> targetResultSize = getTargetResultSize(session);
        if (targetResultSize.isPresent()) {
            uri = uri.queryParam("targetResultSize", targetResultSize.get());
        }
        return uri.build();
    }

    private synchronized URI createRetryUri(String scheme, UriInfo uriInfo)
    {
        UriBuilder uri = uriInfo.getBaseUriBuilder()
                .scheme(scheme)
                .replacePath("/v1/statement/queued/retry")
                .path(queryId.toString())
                .replaceQuery("");
        Optional<DataSize> targetResultSize = getTargetResultSize(session);
        if (targetResultSize.isPresent()) {
            uri = uri.queryParam("targetResultSize", targetResultSize.get());
        }
        return uri.build();
    }

    private static URI findCancelableLeafStage(QueryInfo queryInfo)
    {
        // if query is running, find the leaf-most running stage
        return queryInfo.getOutputStage().map(Query::findCancelableLeafStage).orElse(null);
    }

    private static URI findCancelableLeafStage(StageInfo stage)
    {
        // if this stage is already done, we can't cancel it
        if (stage.getLatestAttemptExecutionInfo().getState().isDone()) {
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
        QueryState state = queryInfo.getState();
        if (state != FAILED) {
            return null;
        }

        FailureInfo failure;
        if (queryInfo.getFailureInfo() != null) {
            failure = queryInfo.getFailureInfo().toFailureInfo();
        }
        else {
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
                firstNonNull(failure.getMessage(), "Internal error"),
                null,
                errorCode.getCode(),
                errorCode.getName(),
                errorCode.getType().toString(),
                errorCode.isRetriable(),
                failure.getErrorLocation(),
                failure);
    }
}
