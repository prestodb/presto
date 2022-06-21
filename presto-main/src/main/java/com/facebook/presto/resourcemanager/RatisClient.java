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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

import javax.annotation.PostConstruct;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
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

public class RatisClient
{
    private final InternalNodeManager internalNodeManager;
    private final String id;
    private final String groupId;
    private RaftClient client;
    private int currentPeerCount;
    private int currentRMCount;
    private final int peerMinCountActive;
    private final Duration timeoutForPeers;
    private static final Logger log = Logger.get(RatisClient.class);
    private final ScheduledExecutorService executor;
    private final Consumer<AllNodes> peerListener = this::updatePeers;
    private final Consumer<AllNodes> groupListener = this::updateRaftGroup;

    @GuardedBy("this")
    private final List<SettableFuture<?>> peerSizeFutures = new ArrayList<>();

    @Inject
    public RatisClient(InternalNodeManager internalNodeManager, RaftConfig raftConfig)
    {
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.id = internalNodeManager.getCurrentNode().getNodeIdentifier();
        this.groupId = requireNonNull(raftConfig, "raftConfig is null").getGroupId();
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

    public RaftPeer[] getPeers()
    {
        return internalNodeManager.getResourceManagers().stream()
                .map(this::getRaftPeer)
                .toArray(RaftPeer[]::new);
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

    private synchronized void updateRaftGroup(AllNodes allNodes)
    {
        log.info("Updating Raft Group");
        if (currentRMCount != allNodes.getActiveResourceManagers().size() && internalNodeManager.getCurrentNode().isResourceManager()) {
            log.info("Found a different RM Count");
            currentRMCount = allNodes.getActiveResourceManagers().size();
            try {
                client.admin().setConfiguration(getPeers());
            }
            catch (IOException e) {
                log.error("Error in updating raft group " + e.getMessage());
            }
        }
    }

    @PostConstruct
    public void start()
    {
        internalNodeManager.addNodeChangeListener(peerListener);
        internalNodeManager.addNodeChangeListener(groupListener);
        updatePeers(internalNodeManager.getAllNodes());
        run();
    }

    public void run()
    {
        ClientId clientId = ClientId.valueOf(UUID.nameUUIDFromBytes(id.getBytes()));
        RaftProperties raftProperties = new RaftProperties();

        ListenableFuture<?> minPeerFuture = waitForMinimumPeers();
        addExceptionCallback(minPeerFuture,
                throwable -> log.error("Exception in waiting for minimum peers for raft client " + throwable.getMessage()));
        addSuccessCallback(minPeerFuture, () -> {
            final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(UUID.nameUUIDFromBytes(groupId.getBytes())), getPeers());

            client = RaftClient.newBuilder()
                    .setClientId(clientId)
                    .setProperties(raftProperties)
                    .setRaftGroup(raftGroup)
                    .setClientRpc(new GrpcFactory(new Parameters()).newRaftClientRpc(clientId, raftProperties))
                    .build();
        });
    }

    public String getLeader()
    {
        return client.getLeaderId().toString();
    }

    @VisibleForTesting
    protected void setClientWithLeader(String leaderId)
    {
        ClientId clientId = ClientId.valueOf(UUID.nameUUIDFromBytes(id.getBytes()));
        RaftProperties raftProperties = new RaftProperties();

        final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(UUID.nameUUIDFromBytes(groupId.getBytes())), getPeers());

        client = RaftClient.newBuilder()
                .setClientId(clientId)
                .setProperties(raftProperties)
                .setRaftGroup(raftGroup)
                .setClientRpc(new GrpcFactory(new Parameters()).newRaftClientRpc(clientId, raftProperties))
                .setLeaderId(RaftPeerId.valueOf(leaderId))
                .build();
    }

    public void sendMessage()
    {
        client.async().send(() -> null);
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
                                format("Insufficient active peer nodes")));
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
