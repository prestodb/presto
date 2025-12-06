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
package com.facebook.presto.spi.analyzer;

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.TableHandle;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Metadata resolver provides information about catalog, schema, tables, views, types, and functions required for analyzer functionality.
 */
public interface MetadataResolver
{
    /**
     * Returns if the catalog with the given name is available in metadata.
     */
    boolean catalogExists(String catalogName);

    /**
     * Returns true if the schema exist in the metadata.
     *
     * @param schemaName represents the catalog and schema name.
     */
    boolean schemaExists(CatalogSchemaName schemaName);

    /**
     * Returns true if the table exist in the metadata.
     *
     * @param tableName the fully qualified name (catalog, schema and table) of the table
     */
    default boolean tableExists(QualifiedObjectName tableName)
    {
        return getTableHandle(tableName).isPresent();
    }

    /**
     * Returns tableHandle for provided tableName
     * @param tableName the fully qualified name (catalog, schema and table) of the table
     */
    Optional<TableHandle> getTableHandle(QualifiedObjectName tableName);

    /**
     * Returns the list of column metadata for the provided catalog, schema and table name.
     *
     * @param tableHandle of the table
     */
    List<ColumnMetadata> getColumns(TableHandle tableHandle);

    /**
     * Returns the map of columnName to ColumnHandle for the provided tableHandle.
     *
     * @param tableHandle of the table
     */
    Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle);

    /**
     * Returns view metadata for a given view.
     *
     * @param viewName the fully qualified name (catalog, schema, and view) of the view.
     */
    Optional<ViewDefinition> getView(QualifiedObjectName viewName);

    /**
     * Returns true if provided object is a view.
     *
     * @param viewName the fully qualified name (catalog, schema, and view) of the view.
     */
    default boolean isView(QualifiedObjectName viewName)
    {
        return getView(viewName).isPresent();
    }

    /**
     * Returns materialized view metadata for a given materialized view name
     *
     * @param viewName the fully qualified name (catalog, schema, and view) of the view.
     */
    Optional<MaterializedViewDefinition> getMaterializedView(QualifiedObjectName viewName);

    /**
     * Returns true if provided catalog, schema and object name is a materialized view.
     *
     * @param viewName the fully qualified name (catalog, schema, and view) of the view.
     */
    default boolean isMaterializedView(QualifiedObjectName viewName)
    {
        return getMaterializedView(viewName).isPresent();
    }

    /**
     * Get the materialized view status to inform the engine how much data has been materialized in the view
     * @param materializedViewName materialized view name
     * @param baseQueryDomain The domain from which to consider missing partitions.
     */
    default MaterializedViewStatus getMaterializedViewStatus(QualifiedObjectName materializedViewName, TupleDomain<String> baseQueryDomain)
    {
        throw new UnsupportedOperationException("getMaterializedViewStatus is not supported");
    }
}
