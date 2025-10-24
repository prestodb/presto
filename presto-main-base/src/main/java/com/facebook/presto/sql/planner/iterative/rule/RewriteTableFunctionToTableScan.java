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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.TableFunctionApplicationResult;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.TableFunctionNode;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.matching.Pattern.empty;
import static com.facebook.presto.sql.planner.plan.Patterns.sources;
import static com.facebook.presto.sql.planner.plan.Patterns.tableFunction;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/*
 * This process converts connector-resolvable TableFunctionNodes into equivalent
 * TableScanNodes by invoking the connector’s applyTableFunction() during planning.
 * It allows table-valued functions whose results can be expressed as a ConnectorTableHandle
 * to be treated like regular scans and benefit from normal scan optimizations.
 *
 * Example:
 * Before Transformation:
 *   TableFunction(my_function(arg1, arg2))
 *
 * After Transformation:
 *   TableScan(my_function(arg1, arg2)).applyTableFunction_tableHandle)
 *    assignments: {outputVar1 -> my_function(arg1, arg2)).applyTableFunction_colHandle1,
 *                  outputVar2 -> my_function(arg1, arg2)).applyTableFunction_colHandle2}
 */
public class RewriteTableFunctionToTableScan
        implements Rule<TableFunctionNode>
{
    private static final Pattern<TableFunctionNode> PATTERN = tableFunction()
            .with(empty(sources()));

    private final Metadata metadata;

    public RewriteTableFunctionToTableScan(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<TableFunctionNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TableFunctionNode tableFunctionNode, Captures captures, Context context)
    {
        Optional<TableFunctionApplicationResult<TableHandle>> result = metadata.applyTableFunction(context.getSession(), tableFunctionNode.getHandle());

        if (!result.isPresent()) {
            return Result.empty();
        }

        List<ColumnHandle> columnHandles = result.get().getColumnHandles();
        checkState(tableFunctionNode.getOutputVariables().size() == columnHandles.size(), "returned table does not match the node's output");
        ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> assignments = ImmutableMap.builder();
        for (int i = 0; i < columnHandles.size(); i++) {
            assignments.put(tableFunctionNode.getOutputVariables().get(i), columnHandles.get(i));
        }

        return Result.ofPlanNode(new TableScanNode(
                tableFunctionNode.getSourceLocation(),
                tableFunctionNode.getId(),
                result.get().getTableHandle(),
                tableFunctionNode.getOutputVariables(),
                assignments.buildOrThrow(),
                TupleDomain.all(),
                TupleDomain.all(), Optional.empty()));
    }
}
