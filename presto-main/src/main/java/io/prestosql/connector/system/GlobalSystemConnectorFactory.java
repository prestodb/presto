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
package io.prestosql.connector.system;

import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.procedure.Procedure;

import javax.inject.Inject;

import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class GlobalSystemConnectorFactory
        implements ConnectorFactory
{
    private final Set<SystemTable> tables;
    private final Set<Procedure> procedures;

    @Inject
    public GlobalSystemConnectorFactory(Set<SystemTable> tables, Set<Procedure> procedures)
    {
        this.tables = ImmutableSet.copyOf(requireNonNull(tables, "tables is null"));
        this.procedures = ImmutableSet.copyOf(requireNonNull(procedures, "procedures is null"));
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
        return new GlobalSystemConnector(catalogName, tables, procedures);
    }
}
