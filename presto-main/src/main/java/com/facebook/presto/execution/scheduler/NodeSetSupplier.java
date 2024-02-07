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

import com.facebook.presto.execution.QueryState;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.QueryId;
import com.google.common.base.Supplier;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

public interface NodeSetSupplier
{
    public CompletableFuture<?> acquireNodes(QueryId queryId, int count, Supplier<QueryState> queryStateSupplier);

    public CompletableFuture<?> releaseNodes(QueryId queryId, int count);

    Supplier<NodeSet> createNodeSetSupplierForQuery(
            QueryId queryId,
            ConnectorId connectorId,
            Optional<Predicate<Node>> nodeFilterPredicate,
            NodeSelectionHashStrategy nodeSelectionHashStrategy,
            boolean useNetworkTopology,
            int minVirtualNodeCount,
            boolean includeCoordinator);
}
