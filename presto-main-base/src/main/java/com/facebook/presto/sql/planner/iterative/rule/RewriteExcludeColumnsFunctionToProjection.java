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

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.operator.table.ExcludeColumns.ExcludeColumnsFunctionHandle;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.TableFunctionProcessorNode;

import java.util.List;
import java.util.NoSuchElementException;

import static com.facebook.presto.sql.planner.plan.Patterns.tableFunctionProcessor;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterators.getOnlyElement;
/**
 * Rewrite a TableFunctionProcessorNode into a Project node if the table function is exclude_columns.
 * <pre>
 * - TableFunctionProcessorNode
 *   propperOutputs=[A, B]
 *   passthroughColumns=[C, D]
 *   - (input) plan which produces symbols [A, B, C, D]
 * </pre>
 * into
 * <pre>
 * - Project
 *   assignments={A, B, C, D}
 *   - (input) plan which produces symbols [A, B, C, D]
 * </pre>
 */
public class RewriteExcludeColumnsFunctionToProjection
        implements Rule<TableFunctionProcessorNode>
{
    private static final Pattern<TableFunctionProcessorNode> PATTERN = tableFunctionProcessor();

    @Override
    public Pattern<TableFunctionProcessorNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TableFunctionProcessorNode node, Captures captures, Context context)
    {
        if (!(node.getHandle().getFunctionHandle() instanceof ExcludeColumnsFunctionHandle)) {
            return Result.empty();
        }

        List<VariableReferenceExpression> inputSymbols = getOnlyElement(node.getRequiredVariables().iterator());
        List<VariableReferenceExpression> outputSymbols = node.getOutputVariables();

        checkState(inputSymbols.size() == outputSymbols.size(), "inputSymbols size differs from outputSymbols size");
        Assignments.Builder assignments = Assignments.builder();
        for (int i = 0; i < outputSymbols.size(); i++) {
            assignments.put(outputSymbols.get(i), inputSymbols.get(i));
        }

        return Result.ofPlanNode(new ProjectNode(
                node.getId(),
                node.getSource().orElseThrow(NoSuchElementException::new),
                assignments.build()));
    }
}
