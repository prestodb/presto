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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.TableHandle;

import java.util.List;
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
    boolean tableExists(QualifiedObjectName tableName);

    /**
     * Returns tableHandle for provided tableName
     * @param tableName the fully qualified name (catalog, schema and table) of the table
     */
    Optional<TableHandle> getTableHandle(QualifiedObjectName tableName);
    /**
     * Returns the list of column metadata for the provided catalog, schema and table name.
     *
     * @param tableName the fully qualified name (catalog, schema and table) of the table
     * @throws SemanticException if the table does not exist
     */
    Optional<List<ColumnMetadata>> getColumns(QualifiedObjectName tableName);

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
}
