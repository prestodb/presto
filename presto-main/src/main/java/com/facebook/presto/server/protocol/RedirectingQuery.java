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

import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.dispatcher.CoordinatorLocation;
import com.facebook.presto.dispatcher.DispatchManager;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.transaction.TransactionId;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

@ThreadSafe
class RedirectingQuery
        implements Query
{
    private final DispatchManager queryManager;
    private final QueryId queryId;
    private final String slug;

    public RedirectingQuery(
            QueryId queryId,
            String slug,
            DispatchManager queryManager)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.slug = requireNonNull(slug, "slug is null");
    }

    @Override
    public void cancel()
    {
        queryManager.cancelQuery(queryId);
    }

    @Override
    public QueryId getQueryId()
    {
        return queryId;
    }

    @Override
    public boolean isSlugValid(String slug)
    {
        return this.slug.equals(slug);
    }

    @Override
    public synchronized Optional<String> getSetCatalog()
    {
        return Optional.empty();
    }

    @Override
    public synchronized Optional<String> getSetSchema()
    {
        return Optional.empty();
    }

    @Override
    public synchronized Map<String, String> getSetSessionProperties()
    {
        return ImmutableMap.of();
    }

    @Override
    public synchronized Set<String> getResetSessionProperties()
    {
        return ImmutableSet.of();
    }

    @Override
    public synchronized Map<String, SelectedRole> getSetRoles()
    {
        return ImmutableMap.of();
    }

    @Override
    public synchronized Map<String, String> getAddedPreparedStatements()
    {
        return ImmutableMap.of();
    }

    @Override
    public synchronized Set<String> getDeallocatedPreparedStatements()
    {
        return ImmutableSet.of();
    }

    @Override
    public synchronized Optional<TransactionId> getStartedTransactionId()
    {
        return Optional.empty();
    }

    @Override
    public synchronized boolean isClearTransactionId()
    {
        return false;
    }

    @Override
    public synchronized ListenableFuture<QueryResults> waitForResults(long token, UriInfo uriInfo, String scheme, Optional<CoordinatorLocation> nextCoordinatorLocation, Duration wait, DataSize targetResultSize)
    {
        checkArgument(token == 0);
        requireNonNull(nextCoordinatorLocation, "nextCoordinatorLocation is null");
        checkArgument(nextCoordinatorLocation.isPresent());
        URI queryHtmlUri = uriInfo.getRequestUriBuilder()
                .scheme(scheme)
                .replacePath("ui/query.html")
                .replaceQuery(queryId.toString())
                .build();

        // get the query info before returning
        // force update if query manager is closed
        BasicQueryInfo queryInfo = queryManager.getQueryInfo(queryId);
        queryManager.getDispatchInfo(queryId);

        QueryResults queryResults = new QueryResults(
                queryId.toString(),
                queryHtmlUri,
                null,
                createNextResultsUri(scheme, uriInfo, nextCoordinatorLocation.get()),
                null,
                null,
                StatementStats.builder().setState("QUEUED").build(),
                null,
                queryInfo.getWarnings(),
                null,
                0L);

        return immediateFuture(queryResults);
    }

    private synchronized URI createNextResultsUri(String scheme, UriInfo uriInfo, CoordinatorLocation coordinatorLocation)
    {
        return coordinatorLocation.getUriBuilder(uriInfo, scheme)
                .scheme(scheme)
                .replacePath("/v1/statement/executing")
                .path(queryId.toString())
                .path(String.valueOf(0))
                .replaceQuery("")
                .queryParam("slug", this.slug)
                .build();
    }
}
