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

import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
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
import javax.inject.Inject;

import java.io.File;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public class RatisServer
{
    private final InternalNodeManager internalNodeManager;
    private final String id;
    private final String groupId;
    private final int port;
    private final String storageDir;

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
    }

    public RaftPeer[] getPeers()
    {
        Set<InternalNode> resourceManagers = internalNodeManager.getResourceManagers();
        return resourceManagers.stream()
                .map(resourceManager -> {
                    RaftPeer.Builder builder = RaftPeer.newBuilder();
                    builder.setId(RaftPeerId.valueOf(resourceManager.getNodeIdentifier()))
                        .setAddress(resourceManager.getHost() + ":" + resourceManager.getRaftPort().getAsInt());
                    return builder.build();
                }).toArray(RaftPeer[]::new);
    }

    @PostConstruct
    public void start()
            throws Exception
    {
        run();
    }

    public void run() throws Exception
    {
        RaftProperties properties = new RaftProperties();
        GrpcConfigKeys.Server.setPort(properties, port);
        File storage = new File(storageDir + "/" + id);
        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storage));
        final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(UUID.nameUUIDFromBytes(groupId.getBytes())), getPeers());

        RaftServer raftServer = RaftServer.newBuilder()
                .setServerId(RaftPeerId.valueOf(id))
                .setProperties(properties)
                .setGroup(raftGroup)
                .setStateMachine(new BaseStateMachine())
                .build();
        raftServer.start();
    }
}
