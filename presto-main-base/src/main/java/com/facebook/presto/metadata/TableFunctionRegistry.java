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
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.function.CatalogSchemaFunctionName;
import com.facebook.presto.spi.function.SchemaFunctionName;
import com.facebook.presto.spi.function.table.ArgumentSpecification;
import com.facebook.presto.spi.function.table.ConnectorTableFunction;
import com.facebook.presto.spi.function.table.DescribedTableReturnTypeSpecification;
import com.facebook.presto.spi.function.table.TableArgumentSpecification;
import com.facebook.presto.spi.function.table.TableFunctionMetadata;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.ThreadSafe;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.SESSION_CATALOG_NOT_SET;
import static com.facebook.presto.spi.function.table.Preconditions.checkArgument;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.CATALOG_NOT_SPECIFIED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.SCHEMA_NOT_SPECIFIED;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class TableFunctionRegistry
{
    // catalog name in the original case; schema and function name in lowercase
    private final Map<ConnectorId, Map<SchemaFunctionName, TableFunctionMetadata>> tableFunctions = new ConcurrentHashMap<>();
    private final AtomicBoolean tvfProviderFunctionsLoaded = new AtomicBoolean(false);

    public void addTableFunctions(ConnectorId catalogName, Collection<ConnectorTableFunction> functions)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(functions, "functions is null");
        checkState(!tableFunctions.containsKey(catalogName), "Table functions already registered for catalog: " + catalogName);

        functions.stream()
                .forEach(TableFunctionRegistry::validateTableFunction);

        ImmutableMap.Builder<SchemaFunctionName, TableFunctionMetadata> builder = ImmutableMap.builder();
        for (ConnectorTableFunction function : functions) {
            builder.put(
                    new SchemaFunctionName(
                            function.getSchema().toLowerCase(ENGLISH),
                            function.getName().toLowerCase(ENGLISH)),
                    new TableFunctionMetadata(catalogName, function));
        }
        tableFunctions.putIfAbsent(catalogName, builder.buildOrThrow());
    }

    public void removeTableFunctions(ConnectorId catalogName)
    {
        tableFunctions.remove(catalogName);
    }

    public boolean areTvfProviderFunctionsLoaded()
    {
        return tvfProviderFunctionsLoaded.get();
    }

    public void updateTvfProviderFunctionsLoaded()
    {
        tvfProviderFunctionsLoaded.compareAndSet(false, true);
    }

    public static List<CatalogSchemaFunctionName> toPath(Session session, QualifiedName name)
    {
        List<String> parts = name.getParts();
        if (parts.size() > 3) {
            throw new PrestoException(StandardErrorCode.FUNCTION_NOT_FOUND, "Invalid function name: " + name);
        }
        if (parts.size() == 3) {
            return ImmutableList.of(new CatalogSchemaFunctionName(parts.get(0), parts.get(1), parts.get(2)));
        }

        if (parts.size() == 2) {
            String currentCatalog = session.getCatalog()
                    .orElseThrow(() -> new PrestoException(SESSION_CATALOG_NOT_SET, "Session default catalog must be set to resolve a partial function name: " + name));
            return ImmutableList.of(new CatalogSchemaFunctionName(currentCatalog, parts.get(0), parts.get(1)));
        }

        ImmutableList.Builder<CatalogSchemaFunctionName> names = ImmutableList.builder();

        String currentCatalog = session.getCatalog()
                .orElseThrow(() -> new SemanticException(CATALOG_NOT_SPECIFIED, "Catalog must be specified when session catalog is not set"));
        String currentSchema = session.getSchema()
                .orElseThrow(() -> new SemanticException(SCHEMA_NOT_SPECIFIED, "Schema must be specified when session schema is not set"));

        // add resolved path items
        names.add(new CatalogSchemaFunctionName(currentCatalog, currentSchema, parts.get(0)));

        // add builtin path items
        names.add(new CatalogSchemaFunctionName("system", "builtin", parts.get(0)));
        return names.build();
    }

    /**
     * Resolve table function with given qualified name.
     * Table functions are resolved case-insensitive for consistency with existing scalar function resolution.
     */
    public TableFunctionMetadata resolve(Session session, QualifiedName qualifiedName)
    {
        for (CatalogSchemaFunctionName name : toPath(session, qualifiedName)) {
            ConnectorId connectorId = new ConnectorId(name.getCatalogName());
            Map<SchemaFunctionName, TableFunctionMetadata> catalogFunctions = tableFunctions.get(connectorId);
            if (catalogFunctions != null) {
                String lowercasedSchemaName = name.getSchemaFunctionName().getSchemaName().toLowerCase(ENGLISH);
                String lowercasedFunctionName = name.getSchemaFunctionName().getFunctionName().toLowerCase(ENGLISH);
                TableFunctionMetadata function = catalogFunctions.get(new SchemaFunctionName(lowercasedSchemaName, lowercasedFunctionName));
                if (function != null) {
                    return function;
                }
            }
        }

        return null;
    }

    public TableFunctionMetadata resolve(ConnectorId connectorId, CatalogSchemaFunctionName name)
    {
        Map<SchemaFunctionName, TableFunctionMetadata> catalogFunctions = tableFunctions.get(connectorId);
        if (catalogFunctions != null) {
            String lowercasedSchemaName = name.getSchemaFunctionName().getSchemaName().toLowerCase(ENGLISH);
            String lowercasedFunctionName = name.getSchemaFunctionName().getFunctionName().toLowerCase(ENGLISH);
            TableFunctionMetadata function = catalogFunctions.get(new SchemaFunctionName(lowercasedSchemaName, lowercasedFunctionName));
            if (function != null) {
                return function;
            }
        }

        throw new PrestoException(GENERIC_USER_ERROR, format("Table functions for catalog %s could not be resolved.", connectorId.getCatalogName()));
    }

    private static void validateTableFunction(ConnectorTableFunction tableFunction)
    {
        requireNonNull(tableFunction, "tableFunction is null");
        requireNonNull(tableFunction.getName(), "table function name is null");
        requireNonNull(tableFunction.getSchema(), "table function schema name is null");
        requireNonNull(tableFunction.getArguments(), "table function arguments is null");
        requireNonNull(tableFunction.getReturnTypeSpecification(), "table function returnTypeSpecification is null");

        checkArgument(!tableFunction.getName().isEmpty(), "table function name is empty");
        checkArgument(!tableFunction.getSchema().isEmpty(), "table function schema name is empty");

        Set<String> argumentNames = new HashSet<>();
        int tableArgumentsWithRowSemantics = 0;
        for (ArgumentSpecification specification : tableFunction.getArguments()) {
            if (!argumentNames.add(specification.getName())) {
                throw new IllegalArgumentException("duplicate argument name: " + specification.getName());
            }

            if (specification instanceof TableArgumentSpecification &&
                    ((TableArgumentSpecification) specification).isRowSemantics()) {
                tableArgumentsWithRowSemantics++;
            }
        }
        checkArgument(tableArgumentsWithRowSemantics <= 1, "more than one table argument with row semantics");
        // The 'keep when empty' or 'prune when empty' property must not be explicitly specified for a table argument with row semantics.
        // Such a table argument is implicitly 'prune when empty'. The TableArgumentSpecification.Builder enforces the 'prune when empty' property
        // for a table argument with row semantics.

        if (tableFunction.getReturnTypeSpecification() instanceof DescribedTableReturnTypeSpecification) {
            DescribedTableReturnTypeSpecification describedTable = (DescribedTableReturnTypeSpecification) tableFunction.getReturnTypeSpecification();
            checkArgument(describedTable.getDescriptor().isTyped(), "field types missing in returned type specification");
        }
    }
}
