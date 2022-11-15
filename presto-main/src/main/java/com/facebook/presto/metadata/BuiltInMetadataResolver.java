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

import com.facebook.presto.Session;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.ViewDefinition;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorMaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;
import com.facebook.presto.sql.analyzer.MetadataResolver;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class BuiltInMetadataResolver
        implements MetadataResolver
{
    private final Metadata metadata;
    private final Session session;

    public BuiltInMetadataResolver(Session session, Metadata metadata)
    {
        this.session = session;
        this.metadata = metadata;
    }

    @Override
    public boolean tableExists(QualifiedObjectName tableName)
    {
        return metadata.getTableHandle(session, tableName).isPresent();
    }

    @Override
    public List<ColumnMetadata> getColumns(QualifiedObjectName tableName)
    {
        Optional<TableHandle> tableHandle = getTableHandle(tableName);
        checkState(tableHandle.isPresent(), "Table: (%s) is not present!", tableName);

        return getTableMetadata(tableHandle.get()).getColumns();
    }

    @Override
    public boolean catalogExists(String catalogName)
    {
        return metadata.getCatalogHandle(session, catalogName).isPresent();
    }

    @Override
    public boolean schemaExists(CatalogSchemaName schema)
    {
        return metadata.schemaExists(session, schema);
    }

    //TODO: Make it private
    @Override
    public Optional<TableHandle> getTableHandle(QualifiedObjectName tableName)
    {
        return metadata.getTableHandle(session, tableName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        return metadata.getColumnHandles(session, tableHandle);
    }

    //TODO: Make it private
    @Override
    public TableMetadata getTableMetadata(TableHandle tableHandle)
    {
        return metadata.getTableMetadata(session, tableHandle);
    }

    @Override
    public Optional<ConnectorId> getCatalogHandle(String catalogName)
    {
        return metadata.getCatalogHandle(session, catalogName);
    }

    @Override
    public Optional<ViewDefinition> getView(QualifiedObjectName viewName)
    {
        return metadata.getView(session, viewName);
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(QualifiedObjectName viewName)
    {
        return metadata.getMaterializedView(session, viewName);
    }

    @Override
    public MaterializedViewStatus getMaterializedViewStatus(QualifiedObjectName materializedViewName, TupleDomain<String> baseQueryDomain)
    {
        return metadata.getMaterializedViewStatus(session, materializedViewName, baseQueryDomain);
    }
}
