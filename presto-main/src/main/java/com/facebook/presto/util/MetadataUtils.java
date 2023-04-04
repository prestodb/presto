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
package com.facebook.presto.util;

import com.facebook.presto.Session;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.sql.analyzer.MetadataHandle;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.analyzer.TableColumnMetadata;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.RuntimeMetricName.GET_COLUMN_HANDLE_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.GET_COLUMN_METADATA_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.GET_MATERIALIZED_VIEW_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.GET_TABLE_HANDLE_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.GET_VIEW_TIME_NANOS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_CATALOG;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_SCHEMA;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;

public class MetadataUtils
{
    private MetadataUtils()
    {
    }

    public static TableColumnMetadata getTableColumnsMetadata(Session session, MetadataResolver metadataResolver, MetadataHandle metadataHandle, QualifiedObjectName tableName)
    {
        if (metadataHandle.isPreProcessMetadataCalls()) {
            return metadataHandle.getTableColumnsMetadata(tableName);
        }

        return getTableColumnMetadata(session, metadataResolver, tableName);
    }

    public static Optional<ViewDefinition> getViewDefinition(Session session, MetadataResolver metadataResolver, MetadataHandle metadataHandle, QualifiedObjectName viewName)
    {
        if (metadataHandle.isPreProcessMetadataCalls()) {
            return metadataHandle.getViewDefinition(viewName);
        }

        return session.getRuntimeStats().profileNanos(
                GET_VIEW_TIME_NANOS,
                () -> metadataResolver.getView(viewName));
    }

    public static Optional<MaterializedViewDefinition> getMaterializedViewDefinition(Session session, MetadataResolver metadataResolver, MetadataHandle metadataHandle, QualifiedObjectName viewName)
    {
        if (metadataHandle.isPreProcessMetadataCalls()) {
            return metadataHandle.getMaterializedViewDefinition(viewName);
        }

        return session.getRuntimeStats().profileNanos(
                GET_MATERIALIZED_VIEW_TIME_NANOS,
                () -> metadataResolver.getMaterializedView(viewName));
    }

    public static TableColumnMetadata getTableColumnMetadata(Session session, MetadataResolver metadataResolver, QualifiedObjectName tableName)
    {
        Optional<TableHandle> tableHandle = session.getRuntimeStats().profileNanos(
                GET_TABLE_HANDLE_TIME_NANOS,
                () -> metadataResolver.getTableHandle(tableName));

        if (!tableHandle.isPresent()) {
            if (!metadataResolver.catalogExists(tableName.getCatalogName())) {
                throw new SemanticException(MISSING_CATALOG, "Catalog %s does not exist", tableName.getCatalogName());
            }
            if (!metadataResolver.schemaExists(new CatalogSchemaName(tableName.getCatalogName(), tableName.getSchemaName()))) {
                throw new SemanticException(MISSING_SCHEMA, "Schema %s does not exist", tableName.getSchemaName());
            }
            throw new SemanticException(MISSING_TABLE, "Table %s does not exist", tableName);
        }

        Map<String, ColumnHandle> columnHandles = session.getRuntimeStats().profileNanos(
                GET_COLUMN_HANDLE_TIME_NANOS,
                () -> metadataResolver.getColumnHandles(tableHandle.get()));

        List<ColumnMetadata> columnsMetadata = session.getRuntimeStats().profileNanos(
                GET_COLUMN_METADATA_TIME_NANOS,
                () -> metadataResolver.getColumns(tableHandle.get()));

        return new TableColumnMetadata(tableHandle, columnHandles, columnsMetadata);
    }
}
