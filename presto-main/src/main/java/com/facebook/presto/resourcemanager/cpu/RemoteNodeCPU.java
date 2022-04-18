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
package com.facebook.presto.resourcemanager.cpu;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.ResponseHandler;
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

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.airlift.http.client.HttpStatus.OK;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.presto.server.RequestHelpers.setContentTypeHeaders;
import static com.facebook.presto.server.smile.AdaptingJsonResponseHandler.createAdaptingJsonResponseHandler;
import static com.facebook.presto.server.smile.FullSmileResponseHandler.createFullSmileResponseHandler;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RemoteNodeCPU
{
    private static final Logger log = Logger.get(RemoteNodeCPU.class);

    private final InternalNode node;
    private final HttpClient httpClient;
    private final URI cpuInfoURI;
    private final Codec<CPUInfo> cpuInfoCodec;
    private final AtomicReference<Optional<CPUInfo>> cpuInfo = new AtomicReference<>(Optional.empty());
    private final AtomicReference<Future<?>> future = new AtomicReference<>();
    private final AtomicLong lastUpdateNanos = new AtomicLong();
    private final AtomicLong lastWarningLogged = new AtomicLong();
    private final boolean isBinaryTransportEnabled;

    public RemoteNodeCPU(
            InternalNode node,
            HttpClient httpClient,
            Codec<CPUInfo> cpuInfoCodec,
            URI cpuInfoURI,
            boolean isBinaryTransportEnabled)
    {
        this.node = requireNonNull(node, "node is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.cpuInfoURI = requireNonNull(cpuInfoURI, "cpuInfoURI is null");
        this.cpuInfoCodec = requireNonNull(cpuInfoCodec, "cpuInfoCodec is null");
        this.isBinaryTransportEnabled = isBinaryTransportEnabled;
    }

    public void asyncRefresh()
    {
        Duration sinceUpdate = nanosSince(lastUpdateNanos.get());
        if (nanosSince(lastWarningLogged.get()).toMillis() > 1_000 &&
                sinceUpdate.toMillis() > 10_000 &&
                future.get() != null) {
            log.warn("CPU info update request to %s has not returned in %s", cpuInfoURI, sinceUpdate.toString(SECONDS));
            lastWarningLogged.set(System.nanoTime());
        }
        if (sinceUpdate.toMillis() > 1_000 && future.get() == null) {
            Request request = setContentTypeHeaders(isBinaryTransportEnabled, preparePost())
                    .setUri(cpuInfoURI)
                    .build();

            ResponseHandler responseHandler;
            if (isBinaryTransportEnabled) {
                responseHandler = createFullSmileResponseHandler((SmileCodec<CPUInfo>) cpuInfoCodec);
            }
            else {
                responseHandler = createAdaptingJsonResponseHandler((JsonCodec<CPUInfo>) cpuInfoCodec);
            }

            HttpClient.HttpResponseFuture<BaseResponse<CPUInfo>> responseFuture = httpClient.executeAsync(request, responseHandler);
            future.compareAndSet(null, responseFuture);

            Futures.addCallback(responseFuture, new FutureCallback<BaseResponse<CPUInfo>>()
            {
                @Override
                public void onSuccess(@Nullable BaseResponse<CPUInfo> result)
                {
                    lastUpdateNanos.set(System.nanoTime());
                    future.compareAndSet(responseFuture, null);
                    if (result != null) {
                        if (result.hasValue()) {
                            cpuInfo.set(Optional.ofNullable(result.getValue()));
                        }
                        if (result.getStatusCode() != OK.code()) {
                            log.warn("Error fetching CPU info from %s returned status %d: %s", cpuInfoURI, result.getStatusCode(), result.getStatusMessage());
                            return;
                        }
                    }
                }

                @Override
                public void onFailure(Throwable t)
                {
                    log.warn("Error fetching CPU info from %s: %s", cpuInfoURI, t.getMessage());
                    lastUpdateNanos.set(System.nanoTime());
                    future.compareAndSet(responseFuture, null);
                }
            }, directExecutor());
        }
    }

    public InternalNode getNode()
    {
        return node;
    }

    public Optional<CPUInfo> getCPUInfo()
    {
        return cpuInfo.get();
    }
}
