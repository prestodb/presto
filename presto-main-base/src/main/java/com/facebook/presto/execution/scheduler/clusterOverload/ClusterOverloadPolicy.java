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

/**
 * Interface for policies that determine if cluster is overloaded.
 * Implementations can check various metrics from NodeStats to determine
 * if a worker is overloaded and queries should be throttled.
 */
public interface ClusterOverloadPolicy
{
    /**
     * Checks if cluster is overloaded.
     *
     * @param nodeManager The node manager to get node information
     * @return true if cluster is overloaded, false otherwise
     */
    boolean isClusterOverloaded(InternalNodeManager nodeManager);

    /**
     * Gets the name of the policy.
     *
     * @return The name of the policy
     */
    String getName();
}
