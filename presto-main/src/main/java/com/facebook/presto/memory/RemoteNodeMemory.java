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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpClient.HttpResponseFuture;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.http.client.StaticBodyGenerator;
import com.facebook.airlift.json.Codec;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.smile.SmileCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.server.smile.BaseResponse;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.airlift.http.client.HttpStatus.OK;
import static com.facebook.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.presto.server.RequestHelpers.setContentTypeHeaders;
import static com.facebook.presto.server.smile.AdaptingJsonResponseHandler.createAdaptingJsonResponseHandler;
import static com.facebook.presto.server.smile.FullSmileResponseHandler.createFullSmileResponseHandler;
import static com.facebook.presto.server.smile.SmileBodyGenerator.smileBodyGenerator;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class RemoteNodeMemory
{
    private static final Logger log = Logger.get(RemoteNodeMemory.class);

    private final InternalNode node;
    private final HttpClient httpClient;
    private final URI memoryInfoUri;
    private final Codec<MemoryInfo> memoryInfoCodec;
    private final Codec<MemoryPoolAssignmentsRequest> assignmentsRequestCodec;
    private final AtomicReference<Optional<MemoryInfo>> memoryInfo = new AtomicReference<>(Optional.empty());
    private final AtomicReference<Future<?>> future = new AtomicReference<>();
    private final AtomicLong lastUpdateNanos = new AtomicLong();
    private final AtomicLong lastWarningLogged = new AtomicLong();
    private final AtomicLong currentAssignmentVersion = new AtomicLong(-1);
    private final boolean isBinaryTransportEnabled;

    public RemoteNodeMemory(
            InternalNode node,
            HttpClient httpClient,
            Codec<MemoryInfo> memoryInfoCodec,
            Codec<MemoryPoolAssignmentsRequest> assignmentsRequestCodec,
            URI memoryInfoUri,
            boolean isBinaryTransportEnabled)
    {
        this.node = requireNonNull(node, "node is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.memoryInfoUri = requireNonNull(memoryInfoUri, "memoryInfoUri is null");
        this.memoryInfoCodec = requireNonNull(memoryInfoCodec, "memoryInfoCodec is null");
        this.assignmentsRequestCodec = requireNonNull(assignmentsRequestCodec, "assignmentsRequestCodec is null");
        this.isBinaryTransportEnabled = isBinaryTransportEnabled;
    }

    public long getCurrentAssignmentVersion()
    {
        return currentAssignmentVersion.get();
    }

    public Optional<MemoryInfo> getInfo()
    {
        return memoryInfo.get();
    }

    public InternalNode getNode()
    {
        return node;
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
            Request request = setContentTypeHeaders(isBinaryTransportEnabled, preparePost())
                    .setUri(memoryInfoUri)
                    .setBodyGenerator(createBodyGenerator(assignments))
                    .build();

            ResponseHandler responseHandler;
            if (isBinaryTransportEnabled) {
                responseHandler = createFullSmileResponseHandler((SmileCodec<MemoryInfo>) memoryInfoCodec);
            }
            else {
                responseHandler = createAdaptingJsonResponseHandler((JsonCodec<MemoryInfo>) memoryInfoCodec);
            }

            HttpResponseFuture<BaseResponse<MemoryInfo>> responseFuture = httpClient.executeAsync(request, responseHandler);
            future.compareAndSet(null, responseFuture);

            Futures.addCallback(responseFuture, new FutureCallback<BaseResponse<MemoryInfo>>()
            {
                @Override
                public void onSuccess(@Nullable BaseResponse<MemoryInfo> result)
                {
                    lastUpdateNanos.set(System.nanoTime());
                    future.compareAndSet(responseFuture, null);
                    long version = currentAssignmentVersion.get();
                    if (result != null) {
                        if (result.hasValue()) {
                            memoryInfo.set(Optional.ofNullable(result.getValue()));
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
                    log.warn("Error fetching memory info from %s: %s", memoryInfoUri, t.getMessage());
                    lastUpdateNanos.set(System.nanoTime());
                    future.compareAndSet(responseFuture, null);
                }
            }, directExecutor());
        }
    }

    private StaticBodyGenerator createBodyGenerator(MemoryPoolAssignmentsRequest assignments)
    {
        if (isBinaryTransportEnabled) {
            return smileBodyGenerator((SmileCodec<MemoryPoolAssignmentsRequest>) assignmentsRequestCodec, assignments);
        }
        return jsonBodyGenerator((JsonCodec<MemoryPoolAssignmentsRequest>) assignmentsRequestCodec, assignments);
    }
}
