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
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.SqlFunction;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;

/**
 * Metadata resolver provides information about catalog, schema, tables, views, types, and functions required for analyzer functionality.
 */
public interface MetadataResolver
{
    /**
     * Returns if the catalog with the given name is available in metadata.
     */
    default boolean catalogExists(String catalogName)
    {
        return false;
    }

    /**
     * Returns true if the schema exist in the metadata.
     *
     * @param schemaName represents the catalog and schema name.
     */
    default boolean schemaExists(CatalogSchemaName schemaName)
    {
        return false;
    }

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
    default Optional<TableHandle> getTableHandle(QualifiedObjectName tableName)
    {
        return Optional.empty();
    }

    /**
     * Returns the list of column metadata for the provided catalog, schema and table name.
     *
     * @param tableName the fully qualified name (catalog, schema and table) of the table
     * @throws SemanticException if the table does not exist
     */
    default Optional<List<ColumnMetadata>> getColumns(QualifiedObjectName tableName)
    {
        return Optional.empty();
    }

    /**
     * Returns view metadata for a given view.
     *
     * @param viewName the fully qualified name (catalog, schema, and view) of the view.
     */
    default Optional<ViewDefinition> getView(QualifiedObjectName viewName)
    {
        return Optional.empty();
    }

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
    default Optional<MaterializedViewDefinition> getMaterializedView(QualifiedObjectName viewName)
    {
        return Optional.empty();
    }

    /**
     * Returns true if provided catalog, schema and object name is a materialized view.
     *
     * @param viewName the fully qualified name (catalog, schema, and view) of the view.
     */
    default boolean isMaterializedView(QualifiedObjectName viewName)
    {
        return getMaterializedView(viewName).isPresent();
    }

    default Type getType(TypeSignature signature)
    {
        throw new UnsupportedOperationException("getType is not supported");
    }

    default Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
    {
        throw new UnsupportedOperationException("getParameterizedType is not supported");
    }

    /**
     * Returns list of registered types.
     */
    default List<Type> getTypes()
    {
        return emptyList();
    }

    /**
     * Determines if a value of a given actual type can be cast to an expected type.
     *
     * @param actualType The actual type of the value.
     * @param expectedType The expected type of the value.
     */
    default boolean canCoerce(Type actualType, Type expectedType)
    {
        return false;
    }

    /**
     * Returns a boolean value indicating whether the coercion from the actual type to the expected type is only a type coercion
     *
     * @param actualType the actual type of the value
     * @param expectedType the expected type of the value
     */
    default boolean isTypeOnlyCoercion(Type actualType, Type expectedType)
    {
        return false;
    }

    /**
     * Returns a common super type for given two types
     * @param firstType the first type
     * @param secondType the second type
     */
    default Optional<Type> getCommonSuperType(Type firstType, Type secondType)
    {
        return Optional.empty();
    }

    /**
     * List all built-in functions
     */
    default Collection<SqlFunction> listBuiltInFunctions()
    {
        return Collections.emptyList();
    }

    default FunctionHandle resolveOperator(OperatorType operatorType, List<TypeSignatureProvider> argumentTypes)
    {
        throw new UnsupportedOperationException("resolveOperator is not supported!");
    }

    default FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle)
    {
        throw new UnsupportedOperationException("getFunctionMetadata is not supported!");
    }
}
