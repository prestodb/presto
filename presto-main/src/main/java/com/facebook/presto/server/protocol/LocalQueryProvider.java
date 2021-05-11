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

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.memory.context.SimpleLocalMemoryContext;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.operator.ExchangeClientSupplier;
import com.facebook.presto.server.ForStatementResource;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.transaction.TransactionManager;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;

public class LocalQueryProvider
{
    private static final Logger log = Logger.get(LocalQueryProvider.class);

    private final QueryManager queryManager;
    private final TransactionManager transactionManager;
    private final ExchangeClientSupplier exchangeClientSupplier;
    private final BlockEncodingSerde blockEncodingSerde;
    private final BoundedExecutor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;
    private final RetryCircuitBreaker retryCircuitBreaker;

    private final ConcurrentMap<QueryId, Query> queries = new ConcurrentHashMap<>();
    private final ScheduledExecutorService queryPurger = newSingleThreadScheduledExecutor(threadsNamed("execution-query-purger"));

    @Inject
    public LocalQueryProvider(
            QueryManager queryManager,
            TransactionManager transactionManager,
            ExchangeClientSupplier exchangeClientSupplier,
            BlockEncodingSerde blockEncodingSerde,
            @ForStatementResource BoundedExecutor responseExecutor,
            @ForStatementResource ScheduledExecutorService timeoutExecutor,
            RetryCircuitBreaker retryCircuitBreaker)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.responseExecutor = requireNonNull(responseExecutor, "responseExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        this.retryCircuitBreaker = requireNonNull(retryCircuitBreaker, "retryCircuitBreaker is null");
    }

    @PostConstruct
    public void start()
    {
        queryPurger.scheduleWithFixedDelay(
                () -> {
                    try {
                        for (Entry<QueryId, Query> entry : queries.entrySet()) {
                            // forget about this query if the query manager is no longer tracking it
                            try {
                                queryManager.getQueryState(entry.getKey());
                            }
                            catch (NoSuchElementException e) {
                                // query is no longer registered
                                queries.remove(entry.getKey());
                            }
                        }
                    }
                    catch (Throwable e) {
                        log.warn(e, "Error removing old queries");
                    }
                },
                200,
                200,
                MILLISECONDS);
    }

    @PreDestroy
    public void stop()
    {
        queryPurger.shutdownNow();
    }

    public Query getQuery(QueryId queryId, String slug)
    {
        Query query = queries.get(queryId);
        if (query != null) {
            if (!query.isSlugValid(slug)) {
                throw notFound("Query not found");
            }
            return query;
        }

        // this is the first time the query has been accessed on this coordinator
        Session session;
        try {
            if (!queryManager.isQuerySlugValid(queryId, slug)) {
                throw notFound("Query not found");
            }
            session = queryManager.getQuerySession(queryId);
        }
        catch (NoSuchElementException e) {
            throw notFound("Query not found");
        }

        query = queries.computeIfAbsent(queryId, id -> {
            ExchangeClient exchangeClient = exchangeClientSupplier.get(new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), LocalQueryProvider.class.getSimpleName()));
            return Query.create(
                    session,
                    slug,
                    queryManager,
                    transactionManager,
                    exchangeClient,
                    responseExecutor,
                    timeoutExecutor,
                    blockEncodingSerde,
                    retryCircuitBreaker);
        });
        return query;
    }

    public void cancel(QueryId queryId, String slug)
    {
        Query query = queries.get(queryId);
        if (query != null) {
            if (!query.isSlugValid(slug)) {
                throw notFound("Query not found");
            }
            query.cancel();
        }

        // cancel the query execution directly instead of creating the statement client
        try {
            if (!queryManager.isQuerySlugValid(queryId, slug)) {
                throw notFound("Query not found");
            }
            queryManager.cancelQuery(queryId);
        }
        catch (NoSuchElementException e) {
            throw notFound("Query not found");
        }
    }

    private static WebApplicationException notFound(String message)
    {
        throw new WebApplicationException(
                Response.status(Status.NOT_FOUND)
                        .type(TEXT_PLAIN_TYPE)
                        .entity(message)
                        .build());
    }
}
