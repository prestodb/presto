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
package com.facebook.presto.connector.adapter;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SafeMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class SafeMetadataAdapter<
        TH extends ConnectorTableHandle,
        CH extends ConnectorColumnHandle,
        OTH extends ConnectorOutputTableHandle,
        ITH extends ConnectorInsertTableHandle>
        implements ConnectorMetadata
{
    private final SafeMetadata<TH, CH, OTH, ITH> delegate;
    private final SafeTypeAdapter<TH, CH, ?, ?, OTH, ITH, ?> typeAdapter;

    public SafeMetadataAdapter(
            SafeMetadata<TH, CH, OTH, ITH> delegate,
            SafeTypeAdapter<TH, CH, ?, ?, OTH, ITH, ?> typeAdapter)
    {
        this.delegate = checkNotNull(delegate, "delegate is null");
        this.typeAdapter = checkNotNull(typeAdapter, "typeAdapter is null");
    }

    @Override
    public TH getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return delegate.getTableHandle(session, tableName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return delegate.listSchemaNames(session);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle table)
    {
        return delegate.getTableMetadata(typeAdapter.castTableHandle(table));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        return delegate.listTables(session, schemaNameOrNull);
    }

    @Override
    public CH getSampleWeightColumnHandle(ConnectorTableHandle tableHandle)
    {
        return delegate.getSampleWeightColumnHandle(typeAdapter.castTableHandle(tableHandle));
    }

    @Override
    public boolean canCreateSampledTables(ConnectorSession session)
    {
        return delegate.canCreateSampledTables(session);
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(ConnectorTableHandle tableHandle)
    {
        return typeAdapter.castColumnHandleMap(delegate.getColumnHandles(typeAdapter.castTableHandle(tableHandle)));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle, ConnectorColumnHandle columnHandle)
    {
        return delegate.getColumnMetadata(typeAdapter.castTableHandle(tableHandle), typeAdapter.castColumnHandle(columnHandle));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return delegate.listTableColumns(session, prefix);
    }

    @Override
    public TH createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return delegate.createTable(session, tableMetadata);
    }

    @Override
    public void dropTable(ConnectorTableHandle tableHandle)
    {
        delegate.dropTable(typeAdapter.castTableHandle(tableHandle));
    }

    @Override
    public void renameTable(ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        delegate.renameTable(typeAdapter.castTableHandle(tableHandle), newTableName);
    }

    @Override
    public OTH beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return delegate.beginCreateTable(session, tableMetadata);
    }

    @Override
    public void commitCreateTable(ConnectorOutputTableHandle tableHandle, Collection<String> fragments)
    {
        delegate.commitCreateTable(typeAdapter.castOutputTableHandle(tableHandle), fragments);
    }

    @Override
    public ITH beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return delegate.beginInsert(session, typeAdapter.castTableHandle(tableHandle));
    }

    @Override
    public void commitInsert(ConnectorInsertTableHandle insertHandle, Collection<String> fragments)
    {
        delegate.commitInsert(typeAdapter.castInsertTableHandle(insertHandle), fragments);
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        delegate.createView(session, viewName, viewData, replace);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        delegate.dropView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return delegate.listViews(session, schemaNameOrNull);
    }

    @Override
    public Map<SchemaTableName, String> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return delegate.getViews(session, prefix);
    }
}
