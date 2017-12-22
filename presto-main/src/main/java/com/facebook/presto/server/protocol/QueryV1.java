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
import com.facebook.presto.client.ClientTypeSignature;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryActions;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.transaction.TransactionId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.SystemSessionProperties.isExchangeCompressionEnabled;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

class QueryV1
        extends Query
{
    private static final String STATEMENT_PATH_V1 = "/v1/statement";
    private static final long DESIRED_RESULT_BYTES = new DataSize(1, MEGABYTE).toBytes();

    @GuardedBy("this")
    private final ExchangeClient exchangeClient;

    @GuardedBy("this")
    private final PagesSerde serde;

    private final ConnectorSession connectorSession;

    @GuardedBy("this")
    private List<Column> columns;

    @GuardedBy("this")
    private List<Type> types;

    @GuardedBy("this")
    private Long updateCount;

    public QueryV1(
            QueryInfo queryInfo,
            QueryManager queryManager,
            SessionPropertyManager sessionPropertyManager,
            ExchangeClient exchangeClient,
            Executor resultsProcessorExecutor,
            ScheduledExecutorService timeoutExecutor,
            BlockEncodingSerde blockEncodingSerde)
    {
        super(queryInfo, queryManager, resultsProcessorExecutor, timeoutExecutor);
        this.exchangeClient = requireNonNull(exchangeClient, "exchangeClient is null");
        requireNonNull(queryInfo, "queryInfo is null");
        Session session = queryInfo.getSession().toSession(sessionPropertyManager);
        this.connectorSession = session.toConnectorSession();
        requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.serde = new PagesSerdeFactory(blockEncodingSerde, isExchangeCompressionEnabled(session)).createPagesSerde();
    }

    @Override
    public synchronized void dispose()
    {
        exchangeClient.close();
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
            types = outputInfo.getColumnTypes();
        }

        for (URI outputLocation : outputInfo.getBufferLocations()) {
            exchangeClient.addLocation(outputLocation);
        }
        if (outputInfo.isNoMoreBufferLocations()) {
            exchangeClient.noMoreLocations();
        }
    }

    @Override
    protected synchronized ListenableFuture<?> isStatusChanged()
    {
        if (exchangeClient.isClosed()) {
            return null;
        }
        else {
            return exchangeClient.isBlocked();
        }
    }

    @Override
    protected synchronized QueryResults getNextQueryResults(QueryInfo queryInfo, UriInfo uriInfo)
    {
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
                pages.add(new RowIterable(connectorSession, types, page));
            }
            if (rows > 0) {
                // client implementations do not properly handle empty list of data
                data = Iterables.concat(pages.build());
            }
        }
        catch (Throwable cause) {
            queryManager.failQuery(queryId, cause);
        }

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

        // close exchange client if the query has failed
        if (queryInfo.getState().isDone()) {
            if (queryInfo.getState() == QueryState.FAILED) {
                exchangeClient.close();
            }
            else if (!queryInfo.getOutputStage().isPresent()) {
                // For simple executions (e.g. drop table), there will never be an output stage,
                // so close the exchange as soon as the query is done.
                exchangeClient.close();

                // Return a single value for clients that require a result.
                columns = ImmutableList.of(new Column("result", "boolean", new ClientTypeSignature(StandardTypes.BOOLEAN, ImmutableList.of())));
                data = ImmutableSet.of(ImmutableList.of(true));
            }
        }

        // only return a next if the query is not done or there is more data to send (due to buffering)
        URI nextResultsUri = null;
        if (!queryInfo.isFinalQueryInfo() || !exchangeClient.isClosed()) {
            nextResultsUri = createNextResultsUri(uriInfo, STATEMENT_PATH_V1);
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

        return new QueryResults(
                queryId.toString(),
                uriInfo.getRequestUriBuilder().replaceQuery(queryId.toString()).replacePath("query.html").build(),
                findCancelableLeafStage(queryInfo),
                nextResultsUri,
                columns,
                data,
                toStatementStats(queryInfo),
                toQueryError(queryInfo),
                queryInfo.getUpdateType(),
                updateCount,
                actions,
                null);
    }
}
