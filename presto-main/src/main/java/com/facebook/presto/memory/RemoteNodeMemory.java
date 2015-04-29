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
package com.facebook.presto.memory;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClient.HttpResponseFuture;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;

@ThreadSafe
public class RemoteNodeMemory
{
    private static final Logger log = Logger.get(RemoteNodeMemory.class);

    private final HttpClient httpClient;
    private final URI memoryInfoUri;
    private final JsonCodec<MemoryInfo> memoryInfoCodec;
    private final JsonCodec<MemoryPoolAssignmentsRequest> assignmentsRequestJsonCodec;
    private final AtomicReference<Optional<MemoryInfo>> memoryInfo = new AtomicReference<>(empty());
    private final AtomicReference<Future<?>> future = new AtomicReference<>();
    private final AtomicLong lastUpdateNanos = new AtomicLong();
    private final AtomicLong lastWarningLogged = new AtomicLong();
    private final AtomicLong currentAssignmentVersion = new AtomicLong(-1);

    public RemoteNodeMemory(HttpClient httpClient, JsonCodec<MemoryInfo> memoryInfoCodec, JsonCodec<MemoryPoolAssignmentsRequest> assignmentsRequestJsonCodec, URI memoryInfoUri)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.memoryInfoUri = requireNonNull(memoryInfoUri, "memoryInfoUri is null");
        this.memoryInfoCodec = requireNonNull(memoryInfoCodec, "memoryInfoCodec is null");
        this.assignmentsRequestJsonCodec = requireNonNull(assignmentsRequestJsonCodec, "assignmentsRequestJsonCodec is null");
    }

    public long getCurrentAssignmentVersion()
    {
        return currentAssignmentVersion.get();
    }

    public Optional<MemoryInfo> getInfo()
    {
        return memoryInfo.get();
    }

    public void asyncRefresh(MemoryPoolAssignmentsRequest assignments)
    {
        Duration sinceUpdate = nanosSince(lastUpdateNanos.get());
        if (nanosSince(lastWarningLogged.get()).toMillis() > 1_000 &&
                sinceUpdate.toMillis() > 10_000 &&
                future.get() != null) {
            log.warn("Memory info update request to %s has not returned in %s", memoryInfoUri, sinceUpdate.toString(SECONDS));
            lastWarningLogged.set(System.nanoTime());
        }
        if (sinceUpdate.toMillis() > 1_000 && future.get() == null) {
            Request request = preparePost()
                    .setUri(memoryInfoUri)
                    .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                    .setBodyGenerator(jsonBodyGenerator(assignmentsRequestJsonCodec, assignments))
                    .build();
            HttpResponseFuture<JsonResponse<MemoryInfo>> responseFuture = httpClient.executeAsync(request, createFullJsonResponseHandler(memoryInfoCodec));
            future.compareAndSet(null, responseFuture);

            Futures.addCallback(responseFuture, new FutureCallback<JsonResponse<MemoryInfo>>() {
                @Override
                public void onSuccess(@Nullable JsonResponse<MemoryInfo> result)

                {
                    lastUpdateNanos.set(System.nanoTime());
                    future.compareAndSet(responseFuture, null);
                    long version = currentAssignmentVersion.get();
                    if (result != null) {
                        if (result.hasValue()) {
                            memoryInfo.set(ofNullable(result.getValue()));
                        }
                        if (result.getStatusCode() != OK.code()) {
                            log.warn("Error fetching memory info from %s returned status %d: %s", memoryInfoUri, result.getStatusCode(), result.getStatusMessage());
                            return;
                        }
                    }
                    currentAssignmentVersion.compareAndSet(version, assignments.getVersion());
                }

                @Override
                public void onFailure(Throwable t)
                {
                    log.warn(t, "Error fetching memory info from %s", memoryInfoUri);
                    lastUpdateNanos.set(System.nanoTime());
                    future.compareAndSet(responseFuture, null);
                }
            });
        }
    }
}
