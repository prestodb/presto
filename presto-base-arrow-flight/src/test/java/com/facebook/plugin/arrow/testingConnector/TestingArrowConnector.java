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
package com.facebook.plugin.arrow.testingConnector;

import com.facebook.plugin.arrow.ArrowConnector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.function.table.ConnectorTableFunction;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.apache.arrow.memory.BufferAllocator;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class TestingArrowConnector
        extends ArrowConnector
{
    private final Set<ConnectorTableFunction> connectorTableFunctions;

    @Inject
    public TestingArrowConnector(ConnectorMetadata metadata, ConnectorSplitManager splitManager, ConnectorPageSourceProvider pageSourceProvider, Set<ConnectorTableFunction> connectorTableFunctions, BufferAllocator allocator)
    {
        super(metadata, splitManager, pageSourceProvider, allocator);
        this.connectorTableFunctions = ImmutableSet.copyOf(requireNonNull(connectorTableFunctions, "connectorTableFunctions is null"));
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return connectorTableFunctions;
    }
}
