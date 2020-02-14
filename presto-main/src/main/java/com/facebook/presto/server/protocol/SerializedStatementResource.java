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
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.operator.ExchangeClientSupplier;
import com.facebook.presto.server.ForStatementResource;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.ws.rs.Path;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.util.OptionalLong;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.http.server.AsyncResponseHandler.bindAsyncResponse;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

@Path("/v1/serializedstatement")
public class SerializedStatementResource
        extends StatementResource
{
    @Inject
    public SerializedStatementResource(
            QueryManager queryManager,
            SessionPropertyManager sessionPropertyManager,
            ExchangeClientSupplier exchangeClientSupplier,
            BlockEncodingSerde blockEncodingSerde,
            @ForStatementResource BoundedExecutor responseExecutor,
            @ForStatementResource ScheduledExecutorService timeoutExecutor)

    {
        super(queryManager, sessionPropertyManager, exchangeClientSupplier, blockEncodingSerde, responseExecutor, timeoutExecutor);
    }

    @Override
    protected String getAllocationTag()
    {
        return SerializedStatementResource.class.getSimpleName();
    }

    @Override
    protected Response getQueryResultsResponse(Query query, UriInfo uriInfo, String schema, DataSize targetResultSize)
    {
        QueryResults queryResults = query.getNextResultSerialized(OptionalLong.empty(), uriInfo, schema, targetResultSize);
        return toResponse(query, queryResults);
    }

    @Override
    protected void asyncQueryResults(Query query, OptionalLong token, Duration maxWait, DataSize targetResultSize, UriInfo uriInfo, String scheme, AsyncResponse asyncResponse)
    {
        Duration wait = WAIT_ORDERING.min(MAX_WAIT_TIME, maxWait);
        if (targetResultSize == null) {
            targetResultSize = DEFAULT_TARGET_RESULT_SIZE;
        }
        else {
            targetResultSize = Ordering.natural().min(targetResultSize, MAX_TARGET_RESULT_SIZE);
        }
        ListenableFuture<QueryResults> queryResultsFuture = query.waitForResultsSerialized(token, uriInfo, scheme, wait, targetResultSize);

        ListenableFuture<Response> response = Futures.transform(queryResultsFuture, queryResults -> toResponse(query, queryResults), directExecutor());

        bindAsyncResponse(asyncResponse, response, responseExecutor);
    }
}
