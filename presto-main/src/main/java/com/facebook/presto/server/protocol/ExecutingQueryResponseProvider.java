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
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.Optional;
import java.util.OptionalLong;

public interface ExecutingQueryResponseProvider
{
    /**
     * Generally, the Presto protocol redirects the client from QueuedStatementResource
     * to ExecutingStatementResource once the query has been de-queued and begun execution.
     * But this redirect might add too much latency for certain very low latency use cases.
     * This interface allows for a response from ExecutingStatementResource to be wired into
     * QueuedStatementResource, so that the client can receive results directly from the
     * QueuedStatement endpoint, without having to be redirected to the ExecutingStatement endpoint.
     *
     * This interface is required for https://github.com/prestodb/presto/issues/23455
     *
     * @param queryId query id
     * @param slug nonce to protect the query
     * @param dispatchInfo information about state of the query
     * @param uriInfo endpoint URI
     * @param xPrestoPrefixUrl prefix URL, that is useful if a proxy is being used
     * @param scheme HTTP scheme
     * @param maxWait duration to wait for query results
     * @param targetResultSize target result size of first response
     * @param compressionEnabled enable compression
     * @param nestedDataSerializationEnabled enable nested data serialization
     * @param binaryResults generate results in binary format, rather than JSON
     * @param retryUrl optional retry URL for cross-cluster retry
     * @param retryExpirationEpochTime optional retry expiration time
     * @param isRetryQuery true if this query is already a retry query
     * @return the ExecutingStatement's Response, if available
     */
    Optional<ListenableFuture<Response>> waitForExecutingResponse(
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
            boolean isRetryQuery);
}
