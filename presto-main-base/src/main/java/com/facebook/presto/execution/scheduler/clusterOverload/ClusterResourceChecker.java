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
package com.facebook.presto.execution.scheduler.clusterOverload;

import com.facebook.presto.metadata.InternalNodeManager;

import javax.inject.Inject;
import javax.inject.Singleton;

import static java.util.Objects.requireNonNull;

/**
 * Provides methods to check if more queries can be run on the cluster
 * based on various resource constraints.
 */
@Singleton
public class ClusterResourceChecker
{
    private final NodeOverloadPolicy nodeOverloadPolicy;

    @Inject
    public ClusterResourceChecker(NodeOverloadPolicy nodeOverloadPolicy)
    {
        this.nodeOverloadPolicy = requireNonNull(nodeOverloadPolicy, "nodeOverloadPolicy is null");
    }

    /**
     * Checks if more queries can be run on the cluster based on overload.
     *
     * @param nodeManager The node manager to get cluster wide worker node information
     * @return true if more queries can be run, false otherwise
     */
    public boolean canRunMoreOnCluster(InternalNodeManager nodeManager)
    {
        return !nodeOverloadPolicy.isClusterOverloaded(nodeManager);
    }
}
