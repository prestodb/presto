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
package com.facebook.presto.sql.planner;

import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.ResolvedField;
import com.facebook.presto.sql.planner.plan.TableFunctionNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SortItem;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class TableFunctionUtils
{
    private TableFunctionUtils() {}

    static Optional<OrderingScheme> getOrderingScheme(Analysis.TableArgumentAnalysis tableArgument, PlanBuilder sourcePlanBuilder, RelationPlan sourcePlan)
    {
        Optional<OrderingScheme> orderBy = Optional.empty();
        if (tableArgument.getOrderBy().isPresent()) {
            List<SortItem> sortItems = tableArgument.getOrderBy().get().getSortItems();

            // Ensure all ORDER BY columns can be translated (populate missing translations if needed)
            for (SortItem sortItem : sortItems) {
                Expression sortKey = sortItem.getSortKey();
                if (!sourcePlanBuilder.canTranslate(sortKey)) {
                    Optional<ResolvedField> resolvedField = sourcePlan.getScope().tryResolveField(sortKey);
                    resolvedField.ifPresent(field -> sourcePlanBuilder.getTranslations().put(
                            sortKey,
                            sourcePlan.getVariable(field.getRelationFieldIndex())));
                }
            }

            // The ordering symbols are coerced
            List<VariableReferenceExpression> coerced = sortItems.stream()
                    .map(SortItem::getSortKey)
                    .map(sourcePlanBuilder::translate)
                    .collect(toImmutableList());

            List<SortOrder> sortOrders = sortItems.stream()
                    .map(PlannerUtils::toSortOrder)
                    .collect(toImmutableList());

            orderBy = Optional.of(PlannerUtils.toOrderingScheme(coerced, sortOrders));
        }
        return orderBy;
    }

    /**
     * Adds pass-through columns from a table function argument to the output variables.
     * If a table argument has pass-through columns, all of its columns are passed on to the output, otherwise only the partitioning columns are passed.
     *
     * @param outputVariables builder for accumulating all output variables from the table function. Proper columns must be added before calling this method.
     * @param tableArgument the analysis result for the table argument.
     * @param sourcePlan the relation plan for the source table.
     * @param specification the optional data organization specification containing partition and ordering information.
     * @param passThroughColumns builder for accumulating {@code PassThroughColumn}.
     * @param sourcePlanBuilder the plan builder for the source, used to translate partition-by {@code Expression} to {@code VariableReferenceExpression}.
     */
    static void addPassthroughColumns(ImmutableList.Builder<VariableReferenceExpression> outputVariables,
                                      Analysis.TableArgumentAnalysis tableArgument,
                                      RelationPlan sourcePlan,
                                      Optional<DataOrganizationSpecification> specification,
                                      ImmutableList.Builder<TableFunctionNode.PassThroughColumn> passThroughColumns,
                                      PlanBuilder sourcePlanBuilder)
    {
        if (tableArgument.isPassThroughColumns()) {
            // the original output symbols from the source node, not coerced
            // note: hidden columns are included. They are present in sourcePlan.fieldMappings
            outputVariables.addAll(sourcePlan.getFieldMappings());
            Set<VariableReferenceExpression> partitionBy = specification
                    .map(DataOrganizationSpecification::getPartitionBy)
                    .map(ImmutableSet::copyOf)
                    .orElse(ImmutableSet.of());
            sourcePlan.getFieldMappings().stream()
                    .map(variable -> new TableFunctionNode.PassThroughColumn(variable, partitionBy.contains(variable)))
                    .forEach(passThroughColumns::add);
        }
        else if (tableArgument.getPartitionBy().isPresent()) {
            tableArgument.getPartitionBy().get().stream()
                    .map(sourcePlanBuilder::translate)
                    // the original symbols for partitioning columns, not coerced
                    .forEach(variable -> {
                        outputVariables.add(variable);
                        passThroughColumns.add(new TableFunctionNode.PassThroughColumn(variable, true));
                    });
        }
    }
}
