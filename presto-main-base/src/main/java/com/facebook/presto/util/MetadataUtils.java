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

/**
 * Ways of asking for the columns of a table.
 *
 * Two things vary. What the table is identified by, which the name of each method says: a table
 * name, or a table handle, which a versioned read needs because only the handle carries the
 * version. And whether metadata prepared ahead of the query may be used, which the parameters
 * say: the overload taking a {@link MetadataHandle} returns that prepared metadata when
 * pre-processing is on, and the ones without it always ask the connector.
 *
 * An analyzer should call {@link #getTableColumnsMetadataByName} with the MetadataHandle, or
 * {@link #getTableColumnsMetadataByHandle} when it holds a versioned handle.
 */
public class MetadataUtils
{
    private MetadataUtils()
    {
    }

    /**
     * The columns of the table a name resolves to: the metadata prepared ahead of the query when
     * pre-processing is on, and the connector's answer otherwise.
     *
     * A name cannot say which version to read, and the prepared metadata is keyed by name and is
     * built before any version is resolved, so a versioned read has to ask
     * {@link #getTableColumnsMetadataByHandle} instead.
     */
    public static TableColumnMetadata getTableColumnsMetadataByName(Session session, MetadataResolver metadataResolver, MetadataHandle metadataHandle, QualifiedObjectName tableName)
    {
        if (metadataHandle.isPreProcessMetadataCalls()) {
            return metadataHandle.getTableColumnsMetadata(tableName);
        }

        return getTableColumnsMetadataByName(session, metadataResolver, tableName);
    }

    public static Optional<ViewDefinition> getViewDefinition(Session session, MetadataResolver metadataResolver, MetadataHandle metadataHandle, QualifiedObjectName viewName)
    {
        if (metadataHandle.isPreProcessMetadataCalls()) {
            return metadataHandle.getViewDefinition(viewName);
        }

        return session.getRuntimeStats().recordWallTime(
                GET_VIEW_TIME_NANOS,
                () -> metadataResolver.getView(viewName));
    }

    public static Optional<MaterializedViewDefinition> getMaterializedViewDefinition(Session session, MetadataResolver metadataResolver, MetadataHandle metadataHandle, QualifiedObjectName viewName)
    {
        if (metadataHandle.isPreProcessMetadataCalls()) {
            return metadataHandle.getMaterializedViewDefinition(viewName);
        }

        return session.getRuntimeStats().recordWallTime(
                GET_MATERIALIZED_VIEW_TIME_NANOS,
                () -> metadataResolver.getMaterializedView(viewName));
    }

    /**
     * The columns of the table a name resolves to, always asked of the connector. Pre-processing
     * runs this ahead of the query, and the overload taking a {@link MetadataHandle} falls back to
     * it when pre-processing is off.
     *
     * Unlike {@link #getTableColumnsMetadataByHandle} it starts from a name, so it is also what
     * reports a missing catalog, schema or table, and the columns it returns are the current ones
     * rather than those of a version.
     */
    public static TableColumnMetadata getTableColumnsMetadataByName(Session session, MetadataResolver metadataResolver, QualifiedObjectName tableName)
    {
        Optional<TableHandle> tableHandle = session.getRuntimeStats().recordWallTime(
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

        Map<String, ColumnHandle> columnHandles = session.getRuntimeStats().recordWallTime(
                GET_COLUMN_HANDLE_TIME_NANOS,
                () -> metadataResolver.getColumnHandles(tableHandle.get()));

        List<ColumnMetadata> columnsMetadata = session.getRuntimeStats().recordWallTime(
                GET_COLUMN_METADATA_TIME_NANOS,
                () -> metadataResolver.getColumns(tableHandle.get()));

        return new TableColumnMetadata(tableHandle, columnHandles, columnsMetadata);
    }

    /**
     * The column handles and the column metadata of a table handle.
     *
     * Use this when the caller already holds a handle and the name is not enough. A handle for a
     * versioned read carries the version, and the columns of that version can differ from the
     * current ones.
     *
     * Pre-processed metadata cannot answer this. It is keyed by table name, and it is prepared
     * before any version is resolved, so the resolver is asked directly here. Both calls are timed
     * the same way as the ones made by name.
     */
    public static TableColumnMetadata getTableColumnsMetadataByHandle(Session session, MetadataResolver metadataResolver, TableHandle tableHandle)
    {
        Map<String, ColumnHandle> columnHandles = session.getRuntimeStats().recordWallTime(
                GET_COLUMN_HANDLE_TIME_NANOS,
                () -> metadataResolver.getColumnHandles(tableHandle));

        List<ColumnMetadata> columnsMetadata = session.getRuntimeStats().recordWallTime(
                GET_COLUMN_METADATA_TIME_NANOS,
                () -> metadataResolver.getColumns(tableHandle));

        return new TableColumnMetadata(Optional.of(tableHandle), columnHandles, columnsMetadata);
    }
}
