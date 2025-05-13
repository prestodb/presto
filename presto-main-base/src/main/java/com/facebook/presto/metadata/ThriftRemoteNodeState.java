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

import com.facebook.drift.client.DriftClient;
import com.facebook.presto.server.thrift.ThriftServerInfoClient;
import com.facebook.presto.spi.NodeState;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ThriftRemoteNodeState
        implements RemoteNodeState
{
    private final ThriftServerInfoClient thriftClient;
    private final AtomicReference<Optional<NodeState>> nodeState = new AtomicReference<>(Optional.empty());
    private final AtomicBoolean requestInflight = new AtomicBoolean();
    private final AtomicLong lastUpdateNanos = new AtomicLong();

    public ThriftRemoteNodeState(DriftClient<ThriftServerInfoClient> thriftClient, URI stateInfoUri)
    {
        requireNonNull(stateInfoUri, "stateInfoUri is null");
        checkState(stateInfoUri.getScheme().equals("thrift"), "unexpected scheme %s", stateInfoUri.getScheme());

        this.thriftClient = requireNonNull(thriftClient, "thriftClient is null").get(Optional.of(stateInfoUri.getAuthority()));
    }

    @Override
    public Optional<NodeState> getNodeState()
    {
        return nodeState.get();
    }

    @Override
    public void asyncRefresh()
    {
        Duration sinceUpdate = nanosSince(lastUpdateNanos.get());

        if (sinceUpdate.toMillis() > 1_000 && requestInflight.compareAndSet(false, true)) {
            ListenableFuture<Integer> responseFuture = thriftClient.getServerState();

            Futures.addCallback(responseFuture, new FutureCallback<Integer>()
            {
                @Override
                public void onSuccess(@Nullable Integer result)
                {
                    lastUpdateNanos.set(System.nanoTime());
                    requestInflight.compareAndSet(true, false);
                    if (result != null) {
                        nodeState.set(Optional.of(NodeState.valueOf(result)));
                    }
                }

                @Override
                public void onFailure(Throwable t)
                {
                    lastUpdateNanos.set(System.nanoTime());
                    requestInflight.compareAndSet(true, false);
                }
            }, directExecutor());
        }
    }
}
