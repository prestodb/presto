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

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeState;

import java.util.Set;

public interface InternalNodeManager
{
    Set<Node> getNodes(NodeState state);

    Set<Node> getActiveConnectorNodes(ConnectorId connectorId);

    Node getCurrentNode();

    Set<Node> getCoordinators();

    AllNodes getAllNodes();

    void refreshNodes();
}
