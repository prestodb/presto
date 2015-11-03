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
package com.facebook.presto.raptor;

import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;

import javax.inject.Inject;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class RaptorNodeSupplier
        implements NodeSupplier
{
    private final NodeManager nodeManager;
    private final String connectorId;

    @Inject
    public RaptorNodeSupplier(NodeManager nodeManager, RaptorConnectorId connectorId)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    }

    @Override
    public Set<Node> getWorkerNodes()
    {
        return nodeManager.getActiveDatasourceNodes(connectorId);
    }
}
