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

import com.facebook.presto.client.ClientTypeSignature;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryActions;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.transaction.TransactionId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.GuardedBy;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;

class QueryV2
        extends Query
{
    private static final String STATEMENT_PATH_V2 = "/v2/statement";

    @GuardedBy("this")
    private List<Column> columns;

    @GuardedBy("this")
    private List<URI> dataUris;

    @GuardedBy("this")
    private SettableFuture<?> firstDataUrisBatchIsReady = SettableFuture.create();

    public QueryV2(
            QueryInfo queryInfo,
            QueryManager queryManager,
            Executor resultsProcessorExecutor,
            ScheduledExecutorService timeoutExecutor)
    {
        super(queryInfo, queryManager, resultsProcessorExecutor, timeoutExecutor);
    }

    @Override
    public synchronized void dispose()
    {
        if (firstDataUrisBatchIsReady != null && !firstDataUrisBatchIsReady.isDone()) {
            // complete in a separate thread to avoid callbacks while holding a lock
            timeoutExecutor.execute(() -> firstDataUrisBatchIsReady.set(null));
        }
    }

    @Override
    protected synchronized void setQueryOutputInfo(QueryExecution.QueryOutputInfo outputInfo)
    {
        // if first callback, set column names
        if (columns == null) {
            List<String> columnNames = outputInfo.getColumnNames();
            List<Type> columnTypes = outputInfo.getColumnTypes();
            checkArgument(columnNames.size() == columnTypes.size(), "Column names and types size mismatch");

            ImmutableList.Builder<Column> list = ImmutableList.builder();
            for (int i = 0; i < columnNames.size(); i++) {
                String name = columnNames.get(i);
                TypeSignature typeSignature = columnTypes.get(i).getTypeSignature();
                String type = typeSignature.toString();
                list.add(new Column(name, type, new ClientTypeSignature(typeSignature)));
            }
            columns = list.build();
        }

        if (!outputInfo.getBufferLocations().isEmpty()) {
            dataUris = outputInfo.getBufferLocations().stream()
                    .map(QueryV2::rewriteTaskUri)
                    .collect(toImmutableList());
            if (firstDataUrisBatchIsReady != null) {
                // complete in a separate thread to avoid callbacks while holding a lock
                timeoutExecutor.execute(() -> firstDataUrisBatchIsReady.set(null));
            }
        }
    }

    @Override
    protected synchronized ListenableFuture<?> isStatusChanged()
    {
        return firstDataUrisBatchIsReady == null ? null : nonCancellationPropagating(firstDataUrisBatchIsReady);
    }

    @Override
    protected synchronized QueryResults getNextQueryResults(QueryInfo queryInfo, UriInfo uriInfo)
    {
        if (columns == null && queryInfo.getUpdateType() != null && !queryInfo.getOutputStage().isPresent()) {
            // For simple executions (e.g. drop table), there will never be an output stage,
            // Return a single value for clients that require a result.
            // TODO: do we really need it? Currently task doesn't send "true" as before
            columns = ImmutableList.of(new Column("result", "boolean", new ClientTypeSignature(StandardTypes.BOOLEAN, ImmutableList.of())));
        }

        // only return a next if the query is not done
        URI nextResultsUri = null;
        if (!queryInfo.isFinalQueryInfo()) {
            nextResultsUri = createNextResultsUri(uriInfo, STATEMENT_PATH_V2);
        }

        QueryActions actions = QueryActions.createIfNecessary(
                queryInfo.getSetCatalog().orElse(null),
                queryInfo.getSetSchema().orElse(null),
                queryInfo.getSetSessionProperties(),
                queryInfo.getResetSessionProperties(),
                queryInfo.getAddedPreparedStatements(),
                queryInfo.getDeallocatedPreparedStatements(),
                queryInfo.getStartedTransactionId().map(TransactionId::toString),
                queryInfo.isClearTransactionId());

        if (firstDataUrisBatchIsReady != null && (firstDataUrisBatchIsReady.isDone() || queryInfo.getState() == QueryState.FAILED)) {
            // uris will be sent as part of the response or query failed and there will be no uris
            firstDataUrisBatchIsReady = null;
        }

        // send data uris once per update, so clear the field once current uris were sent
        List<URI> dataUrisLocal = dataUris;
        if (dataUris != null) {
            dataUris = null;
        }
        return new QueryResults(
                queryId.toString(),
                uriInfo.getRequestUriBuilder().replaceQuery(queryId.toString()).replacePath("query.html").build(),
                findCancelableLeafStage(queryInfo),
                nextResultsUri,
                columns,
                null,
                toStatementStats(queryInfo),
                toQueryError(queryInfo),
                queryInfo.getUpdateType(),
                null,
                actions,
                dataUrisLocal);
    }

    private static URI rewriteTaskUri(URI uri)
    {
        String path = uri.getPath();
        checkArgument(path.startsWith("/v1/task/"));
        // avoid 'replace()' which uses regex
        String newPath = "/v1/download/" + path.substring("/v1/task/".length());
        return UriBuilder.fromUri(uri).replacePath(newPath).path("0").build();
    }
}
