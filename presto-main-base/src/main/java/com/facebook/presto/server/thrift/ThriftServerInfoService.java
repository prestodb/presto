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
package com.facebook.presto.server.thrift;

import com.facebook.drift.annotations.ThriftMethod;
import com.facebook.drift.annotations.ThriftService;
import com.facebook.presto.server.GracefulShutdownHandler;
import com.facebook.presto.server.ServerInfoResource;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import javax.inject.Inject;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.facebook.presto.spi.NodeState.SHUTTING_DOWN;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

/**
 * The corresponding Thrift implementation for {@link ServerInfoResource}.
 * Only /state endpoint has been implemented.
 */
@ThriftService(value = "presto-info", idlName = "ThriftServerInfoService")
public class ThriftServerInfoService
{
    private final GracefulShutdownHandler shutdownHandler;
    private final ListeningExecutorService executor = listeningDecorator(newSingleThreadExecutor(daemonThreadsNamed("server-info-executor")));

    @Inject
    public ThriftServerInfoService(GracefulShutdownHandler shutdownHandler)
    {
        this.shutdownHandler = requireNonNull(shutdownHandler, "shutdownHandler is null");
    }

    /**
     * Use integers to represent the ordinal of the enum.
     * Use NodeState::valueOf to recover the enum from an integer
     */
    @ThriftMethod
    public ListenableFuture<Integer> getServerState()
    {
        return executor.submit(() -> {
            if (shutdownHandler.isShutdownRequested()) {
                return SHUTTING_DOWN.getValue();
            }
            return ACTIVE.getValue();
        });
    }
}
