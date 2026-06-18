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

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.Duration;
import com.facebook.drift.client.DriftClient;
import com.facebook.presto.server.thrift.ThriftServerInfoClient;
import com.facebook.presto.spi.NodeState;
import com.facebook.presto.spi.NodeStats;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import jakarta.annotation.Nullable;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.airlift.units.Duration.nanosSince;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class ThriftRemoteNodeStats
        implements RemoteNodeStats
{
    private static final Logger log = Logger.get(ThriftRemoteNodeStats.class);

    private final ThriftServerInfoClient thriftClient;
    private final long refreshIntervalMillis;
    private final AtomicReference<Optional<NodeStats>> nodeStats = new AtomicReference<>(Optional.empty());
    private final AtomicBoolean requestInflight = new AtomicBoolean();
    private final AtomicLong lastUpdateNanos = new AtomicLong();
    private final URI stateInfoUri;
    private final AtomicLong lastWarningLogged = new AtomicLong();

    public ThriftRemoteNodeStats(DriftClient<ThriftServerInfoClient> thriftClient, URI stateInfoUri, long refreshIntervalMillis)
    {
        requireNonNull(stateInfoUri, "stateInfoUri is null");
        checkArgument(stateInfoUri.getScheme().equals("thrift"), "unexpected scheme %s", stateInfoUri.getScheme());

        this.stateInfoUri = stateInfoUri;
        this.refreshIntervalMillis = refreshIntervalMillis;
        this.thriftClient = requireNonNull(thriftClient, "thriftClient is null").get(Optional.of(stateInfoUri.getAuthority()));
    }

    @Override
    public Optional<NodeStats> getNodeStats()
    {
        return nodeStats.get();
    }

    @Override
    public void asyncRefresh()
    {
        Duration sinceUpdate = nanosSince(lastUpdateNanos.get());
        if (nanosSince(lastWarningLogged.get()).toMillis() > 1_000 &&
                sinceUpdate.toMillis() > 10_000 &&
                requestInflight.get()) {
            log.warn("Node state update request to %s has not returned in %s", stateInfoUri, sinceUpdate.toString(SECONDS));
            lastWarningLogged.set(System.nanoTime());
        }

        if (sinceUpdate.toMillis() > refreshIntervalMillis && requestInflight.compareAndSet(false, true)) {
            ListenableFuture<Integer> responseFuture = thriftClient.getServerState();

            Futures.addCallback(responseFuture, new FutureCallback<Integer>()
            {
                @Override
                public void onSuccess(@Nullable Integer result)
                {
                    lastUpdateNanos.set(System.nanoTime());
                    requestInflight.compareAndSet(true, false);
                    if (result != null) {
                        NodeStats nodeStats1 = new NodeStats(NodeState.valueOf(result), null);
                        nodeStats.set(Optional.of(nodeStats1));
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
                    requestInflight.compareAndSet(true, false);
                }
            }, directExecutor());
        }
    }
}
