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
package com.facebook.presto.execution.executor;

import com.facebook.airlift.node.NodeInfo;
import com.facebook.presto.execution.TaskId;

import javax.inject.Inject;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

public class GracefulShutdownSplitTracker
{
    //private final NodePoolType nodePoolType;
    private final String nodeID;

    @Inject
    public GracefulShutdownSplitTracker(NodeInfo nodeInfo)
    {
        //this.nodePoolType = requireNonNull(serverConfig.getPoolType(), "pool type is null");
        this.nodeID = requireNonNull(nodeInfo, "nodeInfo is null").getNodeId();
    }

    private ConcurrentMap<TaskId, Set<Long>> pendingSplits = new ConcurrentHashMap<>();

    public ConcurrentMap<TaskId, Set<Long>> getPendingSplits()
    {
        return pendingSplits;
    }

    public String getNodeID()
    {
        return nodeID;
    }
}
