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

public abstract class ReadOnlySafeMetadata<TH extends ConnectorTableHandle, CH extends ConnectorColumnHandle>
        implements SafeMetadata<TH, CH, ConnectorOutputTableHandle, ConnectorInsertTableHandle>
{
    @Override
    public boolean canCreateSampledTables(ConnectorSession session)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TH createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(TH tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(TH tableHandle, SchemaTableName newTableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitCreateTable(ConnectorOutputTableHandle tableHandle, Collection<String> fragments)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, TH tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitInsert(ConnectorInsertTableHandle insertHandle, Collection<String> fragments)
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
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<SchemaTableName, String> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }
}
