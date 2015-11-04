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

import com.facebook.presto.spi.Node;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class AllNodes
{
    private final Set<Node> activeNodes;
    private final Set<Node> inactiveNodes;
    private final Set<Node> shuttingDownNodes;

    public AllNodes(Set<Node> activeNodes, Set<Node> inactiveNodes, Set<Node> shuttingDownNodes)
    {
        this.activeNodes = ImmutableSet.copyOf(requireNonNull(activeNodes, "activeNodes is null"));
        this.inactiveNodes = ImmutableSet.copyOf(requireNonNull(inactiveNodes, "inactiveNodes is null"));
        this.shuttingDownNodes = ImmutableSet.copyOf(requireNonNull(shuttingDownNodes, "shuttingDownNodes is null"));
    }

    public Set<Node> getActiveNodes()
    {
        return activeNodes;
    }

    public Set<Node> getInactiveNodes()
    {
        return inactiveNodes;
    }

    public Set<Node> getShuttingDownNodes()
    {
        return shuttingDownNodes;
    }
}
