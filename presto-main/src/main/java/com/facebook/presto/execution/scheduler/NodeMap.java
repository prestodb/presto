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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.google.common.collect.SetMultimap;

import java.net.InetAddress;
import java.util.Set;

public class NodeMap
{
    private final SetMultimap<HostAddress, Node> nodesByHostAndPort;
    private final SetMultimap<InetAddress, Node> nodesByHost;
    private final SetMultimap<NetworkLocation, Node> workersByNetworkPath;
    private final Set<String> coordinatorNodeIds;

    public NodeMap(SetMultimap<HostAddress, Node> nodesByHostAndPort,
            SetMultimap<InetAddress, Node> nodesByHost,
            SetMultimap<NetworkLocation, Node> workersByNetworkPath,
            Set<String> coordinatorNodeIds)
    {
        this.nodesByHostAndPort = nodesByHostAndPort;
        this.nodesByHost = nodesByHost;
        this.workersByNetworkPath = workersByNetworkPath;
        this.coordinatorNodeIds = coordinatorNodeIds;
    }

    public SetMultimap<HostAddress, Node> getNodesByHostAndPort()
    {
        return nodesByHostAndPort;
    }

    public SetMultimap<InetAddress, Node> getNodesByHost()
    {
        return nodesByHost;
    }

    public SetMultimap<NetworkLocation, Node> getWorkersByNetworkPath()
    {
        return workersByNetworkPath;
    }

    public Set<String> getCoordinatorNodeIds()
    {
        return coordinatorNodeIds;
    }
}
