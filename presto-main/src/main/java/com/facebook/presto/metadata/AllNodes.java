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

import com.google.common.collect.ImmutableSet;

import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class AllNodes
{
    private final Set<InternalNode> activeNodes;
    private final Set<InternalNode> inactiveNodes;
    private final Set<InternalNode> shuttingDownNodes;
    private final Set<InternalNode> activeCoordinators;
    private final Set<InternalNode> activeResourceManagers;

    public AllNodes(Set<InternalNode> activeNodes, Set<InternalNode> inactiveNodes, Set<InternalNode> shuttingDownNodes, Set<InternalNode> activeCoordinators, Set<InternalNode> activeResourceManagers)
    {
        this.activeNodes = ImmutableSet.copyOf(requireNonNull(activeNodes, "activeNodes is null"));
        this.inactiveNodes = ImmutableSet.copyOf(requireNonNull(inactiveNodes, "inactiveNodes is null"));
        this.shuttingDownNodes = ImmutableSet.copyOf(requireNonNull(shuttingDownNodes, "shuttingDownNodes is null"));
        this.activeCoordinators = ImmutableSet.copyOf(requireNonNull(activeCoordinators, "activeCoordinators is null"));
        this.activeResourceManagers = ImmutableSet.copyOf(requireNonNull(activeResourceManagers, "activeResourceManagers is null"));
    }

    public Set<InternalNode> getActiveNodes()
    {
        return activeNodes;
    }

    public Set<InternalNode> getInactiveNodes()
    {
        return inactiveNodes;
    }

    public Set<InternalNode> getShuttingDownNodes()
    {
        return shuttingDownNodes;
    }

    public Set<InternalNode> getActiveCoordinators()
    {
        return activeCoordinators;
    }

    public Set<InternalNode> getActiveResourceManagers()
    {
        return activeResourceManagers;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AllNodes allNodes = (AllNodes) o;
        return Objects.equals(activeNodes, allNodes.activeNodes) &&
                Objects.equals(inactiveNodes, allNodes.inactiveNodes) &&
                Objects.equals(shuttingDownNodes, allNodes.shuttingDownNodes) &&
                Objects.equals(activeCoordinators, allNodes.activeCoordinators) &&
                Objects.equals(activeResourceManagers, allNodes.activeResourceManagers);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(activeNodes, inactiveNodes, shuttingDownNodes, activeCoordinators, activeResourceManagers);
    }
}
