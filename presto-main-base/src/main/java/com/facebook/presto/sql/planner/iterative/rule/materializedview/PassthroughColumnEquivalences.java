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
package com.facebook.presto.sql.planner.iterative.rule.materializedview;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewDefinition.TableColumn;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.util.DisjointSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

/**
 * Captures column equivalence information for passthrough (direct-mapped) columns.
 *
 * <p>Only columns marked as {@code isDirectMapped=true} are included in equivalence classes.
 * This is a safety constraint: passthrough columns contain exactly the same data
 * as the base table columns, allowing predicates to be safely translated between them.
 *
 * <p>Columns that are transformed (e.g., {@code COALESCE(dt, '2024-01-01')}) are NOT
 * included because predicate translation through transformations could produce incorrect
 * results. For example, a base table row with {@code dt=NULL} would not match
 * {@code dt='2024-01-01'}, but the MV row would match after the COALESCE transformation.
 *
 * <p>Each equivalence class contains columns that are equal due to:
 * <ul>
 *   <li>Direct passthrough from base table to MV output</li>
 *   <li>Join conditions where both sides are passthrough (e.g., A.dt = B.dt both map to mv.dt)</li>
 * </ul>
 */
public class PassthroughColumnEquivalences
{
    // Maps each column to its equivalence class (the set of all equivalent columns)
    private final Map<TableColumn, Set<TableColumn>> columnToEquivalenceClass;
    // All known tables (base tables + data table) for validation
    private final Set<SchemaTableName> knownTables;

    public PassthroughColumnEquivalences(MaterializedViewDefinition materializedViewDefinition, SchemaTableName dataTable)
    {
        requireNonNull(materializedViewDefinition, "materializedViewDefinition is null");
        requireNonNull(dataTable, "dataTable is null");

        DisjointSet<TableColumn> equivalences = new DisjointSet<>();

        for (MaterializedViewDefinition.ColumnMapping mapping : materializedViewDefinition.getColumnMappings()) {
            TableColumn dataColumn = new TableColumn(dataTable, mapping.getViewColumn().getColumnName());

            for (TableColumn baseColumn : mapping.getBaseTableColumns()) {
                if (baseColumn.isDirectMapped().orElse(true)) {
                    equivalences.findAndUnion(dataColumn, baseColumn);
                }
            }
        }

        // Build the column-to-equivalence-class map from DisjointSet
        ImmutableMap.Builder<TableColumn, Set<TableColumn>> builder = ImmutableMap.builder();
        for (Set<TableColumn> equivalenceClass : equivalences.getEquivalentClasses()) {
            if (equivalenceClass.size() > 1) {
                ImmutableSet<TableColumn> immutableClass = ImmutableSet.copyOf(equivalenceClass);
                for (TableColumn column : equivalenceClass) {
                    builder.put(column, immutableClass);
                }
            }
        }
        this.columnToEquivalenceClass = builder.build();

        this.knownTables = ImmutableSet.<SchemaTableName>builder()
                .add(dataTable)
                .addAll(materializedViewDefinition.getBaseTables())
                .build();
    }

    public boolean hasEquivalence(TableColumn column)
    {
        return columnToEquivalenceClass.containsKey(column);
    }

    /**
     * Returns equivalent predicates for other tables based on column equivalences.
     * Given a predicate on the source table, returns a map from each equivalent table
     * to its corresponding predicate with column names mapped through equivalences.
     */
    public Map<SchemaTableName, TupleDomain<String>> getEquivalentPredicates(
            SchemaTableName sourceTable,
            TupleDomain<String> predicate)
    {
        requireNonNull(sourceTable, "sourceTable is null");
        requireNonNull(predicate, "predicate is null");
        checkState(knownTables.contains(sourceTable),
                "Unknown table: %s. Expected one of: %s", sourceTable, knownTables);

        if (!predicate.getDomains().isPresent()) {
            return ImmutableMap.of();
        }

        Map<SchemaTableName, Map<String, Domain>> domainsByTargetTable = new HashMap<>();

        for (Map.Entry<String, Domain> entry : predicate.getDomains().get().entrySet()) {
            String columnName = entry.getKey();
            Domain domain = entry.getValue();

            TableColumn sourceColumn = new TableColumn(sourceTable, columnName);
            Set<TableColumn> equivalents = columnToEquivalenceClass.get(sourceColumn);
            if (equivalents == null) {
                continue;
            }

            for (TableColumn equivalent : equivalents) {
                if (!equivalent.equals(sourceColumn)) {
                    domainsByTargetTable
                            .computeIfAbsent(equivalent.getTableName(), table -> new HashMap<>())
                            .put(equivalent.getColumnName(), domain);
                }
            }
        }

        return domainsByTargetTable.entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        tableToDomains -> TupleDomain.withColumnDomains(ImmutableMap.copyOf(tableToDomains.getValue()))));
    }

    /**
     * Translates stale predicates from a source table to RowExpressions referencing target variables.
     * Uses column equivalences to map predicates to equivalent tables, then binds to actual variables.
     *
     * @param sourceTable The table with stale predicates
     * @param stalePredicates List of stale partition predicates (disjuncts)
     * @param columnToVariable Mapping from (table, column) to plan variables
     * @param translator Converts TupleDomain to RowExpression
     * @return List of RowExpressions representing the stale predicates bound to variables
     * @throws UnsupportedOperationException if predicates exist but none can be mapped
     */
    public List<RowExpression> translatePredicatesToVariables(
            SchemaTableName sourceTable,
            List<TupleDomain<String>> stalePredicates,
            Map<TableColumn, VariableReferenceExpression> columnToVariable,
            RowExpressionDomainTranslator translator)
    {
        if (stalePredicates.isEmpty()) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<RowExpression> result = ImmutableList.builder();
        for (TupleDomain<String> stalePredicate : stalePredicates) {
            Map<SchemaTableName, TupleDomain<String>> equivalentPredicates =
                    getEquivalentPredicates(sourceTable, stalePredicate);

            for (Map.Entry<SchemaTableName, TupleDomain<String>> entry : equivalentPredicates.entrySet()) {
                SchemaTableName targetTable = entry.getKey();
                TupleDomain<String> targetPredicate = entry.getValue();

                // Convert column names to variables
                TupleDomain<VariableReferenceExpression> variablePredicate = targetPredicate.transform(
                        col -> columnToVariable.get(new TableColumn(targetTable, col)));

                // Only include if some columns could be mapped (not all dropped)
                if (!variablePredicate.isAll() && !variablePredicate.isNone()) {
                    result.add(translator.toPredicate(variablePredicate));
                }
            }
        }

        List<RowExpression> mappedPredicates = result.build();
        if (mappedPredicates.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Cannot map stale predicates from " + sourceTable + " to equivalent columns. " +
                            "Column equivalences may be missing or columns may not be directly mapped.");
        }

        return mappedPredicates;
    }
}
