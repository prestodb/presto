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

import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.function.table.ConnectorTableFunction;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.common.collect.ImmutableSet;
import jakarta.inject.Inject;

import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class GlobalSystemConnectorFactory
        implements ConnectorFactory
{
    private final Set<SystemTable> tables;
    private final Set<Procedure> procedures;
    private final Set<ConnectorTableFunction> tableFunctions;
    private final NodeManager nodeManager;
    private final FunctionAndTypeManager functionAndTypeManager;

    @Inject
    public GlobalSystemConnectorFactory(Set<SystemTable> tables, Set<Procedure> procedures, Set<ConnectorTableFunction> tableFunctions, NodeManager nodeManager, FunctionAndTypeManager functionAndTypeManager)
    {
        this.tables = ImmutableSet.copyOf(requireNonNull(tables, "tables is null"));
        this.procedures = ImmutableSet.copyOf(requireNonNull(procedures, "procedures is null"));
        this.tableFunctions = ImmutableSet.copyOf(requireNonNull(tableFunctions, "tableFunctions is null"));
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    @Override
    public String getName()
    {
        return GlobalSystemConnector.NAME;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new GlobalSystemHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return new GlobalSystemConnector(catalogName, tables, procedures, tableFunctions, nodeManager, functionAndTypeManager);
    }
}
