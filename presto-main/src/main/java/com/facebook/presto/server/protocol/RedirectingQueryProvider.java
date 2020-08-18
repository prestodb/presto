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
import com.facebook.presto.dispatcher.DispatchManager;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.ImmutableSet;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;

public class RedirectingQueryProvider
        implements QueryProvider
{
    private static final Logger log = Logger.get(RedirectingQueryProvider.class);

    private final ConcurrentMap<QueryId, Query> queries = new ConcurrentHashMap<>();
    private final DispatchManager dispatchManager;
    private final ScheduledExecutorService queryPurger = newSingleThreadScheduledExecutor(threadsNamed("execution-query-purger"));

    @Inject
    public RedirectingQueryProvider(DispatchManager dispatchManager)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
    }

    @PostConstruct
    public void start()
    {
        queryPurger.scheduleWithFixedDelay(
                () -> {
                    try {
                        // snapshot the queries before checking states to avoid registration race
                        for (Map.Entry<QueryId, Query> entry : ImmutableSet.copyOf(queries.entrySet())) {
                            // forget about this query if the query manager is no longer tracking it
                            if (!dispatchManager.isQueryPresent(entry.getKey())) {
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

    @Override
    public Query getQuery(QueryId queryId, String slug)
    {
        Query query = queries.get(queryId);
        if (query != null) {
            if (!query.isSlugValid(slug)) {
                throw notFound("Query not found");
            }
            return query;
        }

        return queries.computeIfAbsent(queryId, id -> new RedirectingQuery(
                queryId,
                slug,
                dispatchManager));
    }

    @Override
    public void cancel(QueryId queryId, String slug)
    {
        Query query = queries.get(queryId);
        if (query != null) {
            if (!query.isSlugValid(slug)) {
                throw notFound("Query not found");
            }
            query.cancel();
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
