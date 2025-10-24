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

import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.dispatcher.DispatchInfo;
import com.facebook.presto.spi.QueryId;
import com.google.common.util.concurrent.ListenableFuture;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.server.protocol.QueryResourceUtil.toResponse;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class LocalExecutingQueryResponseProvider
        implements ExecutingQueryResponseProvider
{
    private final LocalQueryProvider queryProvider;

    @Inject
    public LocalExecutingQueryResponseProvider(LocalQueryProvider queryProvider)
    {
        this.queryProvider = requireNonNull(queryProvider, "queryProvider is null");
    }

    @Override
    public Optional<ListenableFuture<Response>> waitForExecutingResponse(
            QueryId queryId,
            String slug,
            DispatchInfo dispatchInfo,
            UriInfo uriInfo,
            String xPrestoPrefixUrl,
            String scheme,
            Duration maxWait,
            DataSize targetResultSize,
            boolean compressionEnabled,
            boolean nestedDataSerializationEnabled,
            boolean binaryResults,
            long durationUntilExpirationMs,
            Optional<URI> retryUrl,
            OptionalLong retryExpirationEpochTime,
            boolean isRetryQuery)
    {
        Query query;
        try {
            query = queryProvider.getQuery(queryId, slug, retryUrl, retryExpirationEpochTime, isRetryQuery);
        }
        catch (WebApplicationException e) {
            return Optional.empty();
        }
        return Optional.of(transform(
                query.waitForResults(0, uriInfo, scheme, maxWait, targetResultSize, binaryResults),
                results -> toResponse(query, results, xPrestoPrefixUrl, compressionEnabled, nestedDataSerializationEnabled, durationUntilExpirationMs),
                directExecutor()));
    }
}
