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
package com.facebook.presto.metadata;

import com.facebook.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpClient.HttpResponseFuture;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.spi.NodeStats;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.errorprone.annotations.ThreadSafe;
import jakarta.annotation.Nullable;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static com.facebook.airlift.http.client.HttpStatus.OK;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.units.Duration.nanosSince;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class HttpRemoteNodeStats
        implements RemoteNodeStats
{
    private static final Logger log = Logger.get(HttpRemoteNodeStats.class);

    private final HttpClient httpClient;
    private final URI stateInfoUri;
    private final long refreshIntervalMillis;
    private final AtomicReference<Optional<NodeStats>> nodeStats = new AtomicReference<>(Optional.empty());
    private final AtomicReference<Future<?>> future = new AtomicReference<>();
    private final AtomicLong lastUpdateNanos = new AtomicLong();
    private final AtomicLong lastWarningLogged = new AtomicLong();

    public HttpRemoteNodeStats(HttpClient httpClient, URI stateInfoUri, long refreshIntervalMillis)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.stateInfoUri = requireNonNull(stateInfoUri, "stateInfoUri is null");
        this.refreshIntervalMillis = refreshIntervalMillis;
    }

    @Override
    public Optional<NodeStats> getNodeStats()
    {
        return nodeStats.get();
    }

    @Override
    public synchronized void asyncRefresh()
    {
        Duration sinceUpdate = nanosSince(lastUpdateNanos.get());
        if (nanosSince(lastWarningLogged.get()).toMillis() > 1_000 &&
                sinceUpdate.toMillis() > 10_000 &&
                future.get() != null) {
            log.warn("Node state update request to %s has not returned in %s", stateInfoUri, sinceUpdate.toString(SECONDS));
            lastWarningLogged.set(System.nanoTime());
        }
        if (sinceUpdate.toMillis() > refreshIntervalMillis && future.get() == null) {
            Request request = prepareGet()
                    .setUri(stateInfoUri)
                    .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                    .build();
            HttpResponseFuture<JsonResponse<NodeStats>> responseFuture = httpClient.executeAsync(request, createFullJsonResponseHandler(jsonCodec(NodeStats.class)));
            future.compareAndSet(null, responseFuture);

            Futures.addCallback(responseFuture, new FutureCallback<JsonResponse<NodeStats>>()
            {
                @Override
                public void onSuccess(@Nullable JsonResponse<NodeStats> result)
                {
                    lastUpdateNanos.set(System.nanoTime());
                    future.compareAndSet(responseFuture, null);
                    if (result != null) {
                        if (result.hasValue()) {
                            nodeStats.set(Optional.ofNullable(result.getValue()));
                        }
                        if (result.getStatusCode() != OK.code()) {
                            log.warn("Error fetching node stats from %s returned status %d", stateInfoUri, result.getStatusCode());
                            return;
                        }
                    }
                    else {
                        log.warn("Node statistics endpoint %s returned null response, using cached statistics", stateInfoUri);
                    }
                }

                @Override
                public void onFailure(Throwable t)
                {
                    log.error("Error fetching node stats from %s: %s", stateInfoUri, t.getMessage());
                    lastUpdateNanos.set(System.nanoTime());
                    future.compareAndSet(responseFuture, null);
                }
            }, directExecutor());
        }
    }
}
