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

package com.facebook.presto.hive.auth;

import com.facebook.presto.hive.HiveMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.google.inject.Inject;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class HdfsAuthenticatingMetadata

        implements ConnectorMetadata
{
    private final HadoopKerberosAuthentication authentication;
    private final HiveMetadata targetMetadata;

    @Inject
    public HdfsAuthenticatingMetadata(HadoopKerberosAuthentication authentication, HiveMetadata targetMetadata)
    {
        this.authentication = requireNonNull(authentication, "authentication is null");
        this.targetMetadata = requireNonNull(targetMetadata, "targetMetadata is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return authentication.doAs(session.getUser(), () -> targetMetadata.listSchemaNames(session));
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return authentication.doAs(session.getUser(), () -> targetMetadata.getTableHandle(session, tableName));
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        return authentication.doAs(session.getUser(),
                () -> targetMetadata.getTableLayouts(session, table, constraint, desiredColumns));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return authentication.doAs(session.getUser(), () -> targetMetadata.getTableLayout(session, handle));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return authentication.doAs(session.getUser(), () -> targetMetadata.getTableMetadata(session, table));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        return authentication.doAs(session.getUser(), () -> targetMetadata.listTables(session, schemaNameOrNull));
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return authentication.doAs(session.getUser(), () -> targetMetadata.getSampleWeightColumnHandle(session, tableHandle));
    }

    @Override
    public boolean canCreateSampledTables(ConnectorSession session)
    {
        return authentication.doAs(session.getUser(), () -> targetMetadata.canCreateSampledTables(session));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return authentication.doAs(session.getUser(), () -> targetMetadata.getColumnHandles(session, tableHandle));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return authentication.doAs(session.getUser(), () -> targetMetadata.getColumnMetadata(session, tableHandle, columnHandle));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return authentication.doAs(session.getUser(), () -> targetMetadata.listTableColumns(session, prefix));
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        authentication.doAs(session.getUser(), () -> targetMetadata.createTable(session, tableMetadata));
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        authentication.doAs(session.getUser(), () -> targetMetadata.dropTable(session, tableHandle));
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        authentication.doAs(session.getUser(), () -> targetMetadata.renameTable(session, tableHandle, newTableName));
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        authentication.doAs(session.getUser(), () -> targetMetadata.addColumn(session, tableHandle, column));
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        authentication.doAs(session.getUser(), () -> targetMetadata.renameColumn(session, tableHandle, source, target));
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return authentication.doAs(session.getUser(), () -> targetMetadata.beginCreateTable(session, tableMetadata));
    }

    @Override
    public void commitCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        authentication.doAs(session.getUser(), () -> targetMetadata.commitCreateTable(session, tableHandle, fragments));
    }

    @Override
    public void rollbackCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle)
    {
        authentication.doAs(session.getUser(), () -> targetMetadata.rollbackCreateTable(session, tableHandle));
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return authentication.doAs(session.getUser(), () -> targetMetadata.beginInsert(session, tableHandle));
    }

    @Override
    public void commitInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        authentication.doAs(session.getUser(), () -> targetMetadata.commitInsert(session, insertHandle, fragments));
    }

    @Override
    public void rollbackInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle)
    {
        authentication.doAs(session.getUser(), () -> targetMetadata.rollbackInsert(session, insertHandle));
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return authentication.doAs(session.getUser(), () -> targetMetadata.getUpdateRowIdColumnHandle(session, tableHandle));
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return authentication.doAs(session.getUser(), () -> targetMetadata.beginDelete(session, tableHandle));
    }

    @Override
    public void commitDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        authentication.doAs(session.getUser(), () -> targetMetadata.commitDelete(session, tableHandle, fragments));
    }

    @Override
    public void rollbackDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        authentication.doAs(session.getUser(), () -> targetMetadata.rollbackDelete(session, tableHandle));
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        authentication.doAs(session.getUser(), () -> targetMetadata.createView(session, viewName, viewData, replace));
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        authentication.doAs(session.getUser(), () -> targetMetadata.dropView(session, viewName));
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return authentication.doAs(session.getUser(), () -> targetMetadata.listViews(session, schemaNameOrNull));
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return authentication.doAs(session.getUser(), () -> targetMetadata.getViews(session, prefix));
    }

    @Override
    public boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        return authentication.doAs(session.getUser(), () -> targetMetadata.supportsMetadataDelete(session, tableHandle, tableLayoutHandle));
    }

    @Override
    public OptionalLong metadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        return authentication.doAs(session.getUser(), () -> targetMetadata.metadataDelete(session, tableHandle, tableLayoutHandle));
    }
}
