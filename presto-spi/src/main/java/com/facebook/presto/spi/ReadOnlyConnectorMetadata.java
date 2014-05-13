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
package com.facebook.presto.spi;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public abstract class ReadOnlyConnectorMetadata
        implements ConnectorMetadata
{
    @Override
    public final ConnectorTableHandle createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void dropTable(ConnectorTableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public final boolean canCreateSampledTables(ConnectorSession session)
    {
        return false;
    }

    @Override
    public final ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void commitCreateTable(ConnectorOutputTableHandle tableHandle, Collection<String> fragments)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return emptyList();
    }

    @Override
    public Map<SchemaTableName, String> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return emptyMap();
    }
}
