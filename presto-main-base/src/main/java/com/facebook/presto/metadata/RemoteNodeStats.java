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

import com.facebook.presto.spi.NodeStats;

import java.util.Optional;

/**
 * Interface for retrieving statistics from remote nodes in a Presto cluster.
 * <p>
 * This interface provides a mechanism to asynchronously fetch and cache node statistics
 * from remote Presto worker nodes. Implementations handle the communication protocol
 * details (HTTP, Thrift, etc.) and provide a unified way to access node health and
 * performance metrics.
 * <p>
 * The interface supports lazy loading and caching of node statistics to minimize
 * network overhead while ensuring that cluster management components have access
 * to current node state information for scheduling and health monitoring decisions.
 */
public interface RemoteNodeStats
{
    /**
     * Returns the cached node statistics if available.
     * <p>
     * This method returns the most recently fetched statistics for the remote node.
     * If no statistics have been fetched yet or if the last fetch failed, this
     * method returns an empty Optional.
     *
     * @return an Optional containing the node statistics if available, empty otherwise
     */
    Optional<NodeStats> getNodeStats();

    /**
     * Triggers an asynchronous refresh of the node statistics.
     * <p>
     * This method initiates a background request to fetch the latest statistics
     * from the remote node. The operation is non-blocking and the results will
     * be available through subsequent calls to {@link #getNodeStats()}.
     * <p>
     * Implementations should handle network failures gracefully and avoid
     * overwhelming the remote node with excessive requests.
     */
    void asyncRefresh();
}
