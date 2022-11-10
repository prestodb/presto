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
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorMaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;
import com.facebook.presto.sql.analyzer.MetadataResolver;

import java.util.Map;
import java.util.Optional;

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
    public Optional<TableHandle> getTableHandle(QualifiedObjectName tableName)
    {
        return metadata.getTableHandle(session, tableName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        return metadata.getColumnHandles(session, tableHandle);
    }

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
    public boolean schemaExists(CatalogSchemaName schema)
    {
        return metadata.schemaExists(session, schema);
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
