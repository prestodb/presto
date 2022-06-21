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
package com.facebook.presto.resourcemanager;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.impl.BaseStateMachine;

import javax.annotation.PostConstruct;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;

import static com.facebook.airlift.concurrent.MoreFutures.addExceptionCallback;
import static com.facebook.airlift.concurrent.MoreFutures.addSuccessCallback;
import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RatisServer
{
    private final InternalNodeManager internalNodeManager;
    private final String id;
    private final String groupId;
    private final int port;
    private final String storageDir;
    private final int peerMinCountActive;
    private int currentPeerCount;
    private final Duration timeoutForPeers;
    private RaftServer raftServer;
    private static final Logger log = Logger.get(RatisServer.class);
    private final ScheduledExecutorService executor;
    private final Consumer<AllNodes> listener = this::updatePeers;

    @GuardedBy("this")
    private final List<SettableFuture<?>> peerSizeFutures = new ArrayList<>();

    @Inject
    public RatisServer(InternalNodeManager internalNodeManager, RaftConfig raftConfig)
    {
        requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.internalNodeManager = internalNodeManager;
        this.id = internalNodeManager.getCurrentNode().getNodeIdentifier();
        requireNonNull(raftConfig, "raftConfig is null");
        this.groupId = raftConfig.getGroupId();
        this.port = raftConfig.getPort();
        this.storageDir = raftConfig.getStorageDir();
        this.peerMinCountActive = raftConfig.getRequiredPeersActive();
        this.timeoutForPeers = raftConfig.getTimeoutForPeers();
        this.executor = newSingleThreadScheduledExecutor(threadsNamed("raft-monitor-%s"));
    }

    private RaftPeer getRaftPeer(InternalNode resourceManager)
    {
        RaftPeer.Builder builder = RaftPeer.newBuilder();
        builder.setId(RaftPeerId.valueOf(resourceManager.getNodeIdentifier()))
                .setAddress(resourceManager.getHost() + ":" + resourceManager.getRaftPort().getAsInt());
        return builder.build();
    }

    private synchronized void updatePeers(AllNodes allNodes)
    {
        currentPeerCount = allNodes.getActiveResourceManagers().size();
        if (currentPeerCount >= peerMinCountActive) {
            List<SettableFuture<?>> listeners = ImmutableList.copyOf(peerSizeFutures);
            peerSizeFutures.clear();
            executor.submit(() -> listeners.forEach(listener -> listener.set(null)));
        }
    }

    public RaftPeer[] getPeers()
    {
        return internalNodeManager.getResourceManagers().stream()
                .map(this::getRaftPeer)
                .toArray(RaftPeer[]::new);
    }

    @PostConstruct
    public void start()
            throws Exception
    {
        internalNodeManager.addNodeChangeListener(listener);
        updatePeers(internalNodeManager.getAllNodes());
        run();
    }

    public void run()
    {
        RaftProperties properties = new RaftProperties();
        GrpcConfigKeys.Server.setPort(properties, port);
        File storage = new File(storageDir + "/" + id);
        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storage));

        ListenableFuture<?> minPeerFuture = waitForMinimumPeers();
        addExceptionCallback(minPeerFuture,
                throwable -> log.error("Exception in waiting for minimum peers for raft server " + throwable.getMessage()));
        addSuccessCallback(minPeerFuture, () -> {
            final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(UUID.nameUUIDFromBytes(groupId.getBytes())), getPeers());
            try {
                raftServer = RaftServer.newBuilder()
                            .setServerId(RaftPeerId.valueOf(id))
                            .setProperties(properties)
                            .setGroup(raftGroup)
                            .setStateMachine(new BaseStateMachine())
                            .build();
                raftServer.start();
            }
            catch (IOException e) {
                log.error("Error starting raft server " + e.getMessage());
            }
        });
    }

    public void stop() throws IOException
    {
        raftServer.close();
        internalNodeManager.removeNodeChangeListener(listener);
    }

    public synchronized ListenableFuture<?> waitForMinimumPeers()
    {
        if (currentPeerCount >= peerMinCountActive) {
            return immediateFuture(null);
        }

        SettableFuture<?> future = SettableFuture.create();
        peerSizeFutures.add(future);

        ScheduledFuture<?> timeoutTask = executor.schedule(
                () -> {
                    synchronized (this) {
                        future.setException(new PrestoException(
                                GENERIC_INSUFFICIENT_RESOURCES,
                                format("Timeout: Insufficient active peer nodes")));
                    }
                },
                timeoutForPeers.toMillis(),
                MILLISECONDS);

        future.addListener(() -> {
            timeoutTask.cancel(true);
            removePeerFuture(future);
        }, executor);

        return future;
    }

    private synchronized void removePeerFuture(SettableFuture<?> future)
    {
        peerSizeFutures.remove(future);
    }
}
