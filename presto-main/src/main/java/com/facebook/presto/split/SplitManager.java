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
package com.facebook.presto.split;

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class SplitManager
{
    private final ConcurrentMap<ConnectorId, ConnectorSplitManager> splitManagers = new ConcurrentHashMap<>();

    public void addConnectorSplitManager(ConnectorId connectorId, ConnectorSplitManager connectorSplitManager)
    {
        checkState(splitManagers.putIfAbsent(connectorId, connectorSplitManager) == null, "SplitManager for connector '%s' is already registered", connectorId);
    }

    public SplitSource getSplits(Session session, TableLayoutHandle layout)
    {
        ConnectorId connectorId = layout.getConnectorId();
        ConnectorSplitManager splitManager = getConnectorSplitManager(connectorId);

        ConnectorSession connectorSession = session.toConnectorSession(connectorId);

        ConnectorSplitSource source = splitManager.getSplits(layout.getTransactionHandle(), connectorSession, layout.getConnectorHandle());

        return new ConnectorAwareSplitSource(connectorId, layout.getTransactionHandle(), source);
    }

    private ConnectorSplitManager getConnectorSplitManager(ConnectorId connectorId)
    {
        ConnectorSplitManager result = splitManagers.get(connectorId);
        checkArgument(result != null, "No split manager for connector '%s'", connectorId);
        return result;
    }
}
