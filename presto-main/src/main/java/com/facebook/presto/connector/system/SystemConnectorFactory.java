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
package com.facebook.presto.connector.system;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SystemTable;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class
        SystemConnectorFactory
        implements ConnectorFactory
{
    private final NodeManager nodeManager;
    private final Set<SystemTable> tables;

    @Inject
    public SystemConnectorFactory(NodeManager nodeManager, Set<SystemTable> tables)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.tables = ImmutableSet.copyOf(requireNonNull(tables, "tables is null"));
    }

    @Override
    public String getName()
    {
        return SystemConnector.NAME;
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config)
    {
        return new SystemConnector(connectorId, nodeManager, tables);
    }
}
