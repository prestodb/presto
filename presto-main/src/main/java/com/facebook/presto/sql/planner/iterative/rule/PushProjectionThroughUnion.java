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

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.ExpressionVariableInliner;
import com.facebook.presto.sql.planner.RowExpressionVariableInliner;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.relational.OriginalExpressionUtils;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.union;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;

public class PushProjectionThroughUnion
        implements Rule<ProjectNode>
{
    private static final Capture<UnionNode> CHILD = newCapture();

    private static final Pattern<ProjectNode> PATTERN = project()
            .with(source().matching(union().capturedAs(CHILD)));

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ProjectNode parent, Captures captures, Context context)
    {
        UnionNode source = captures.get(CHILD);

        // OutputLayout of the resultant Union, will be same as the layout of the Project
        List<VariableReferenceExpression> outputLayout = parent.getOutputVariables();

        // Mapping from the output symbol to ordered list of symbols from each of the sources
        ImmutableListMultimap.Builder<VariableReferenceExpression, VariableReferenceExpression> mappings = ImmutableListMultimap.builder();

        // sources for the resultant UnionNode
        ImmutableList.Builder<PlanNode> outputSources = ImmutableList.builder();

        for (int i = 0; i < source.getSources().size(); i++) {
            Map<VariableReferenceExpression, SymbolReference> outputToInput = Maps.transformValues(source.sourceVariableMap(i), OriginalExpressionUtils::asSymbolReference);  // Map: output of union -> input of this source to the union
            Assignments.Builder assignments = Assignments.builder(); // assignments for the new ProjectNode

            // mapping from current ProjectNode to new ProjectNode, used to identify the output layout
            Map<VariableReferenceExpression, VariableReferenceExpression> projectVariableMapping = new HashMap<>();

            // Translate the assignments in the ProjectNode using symbols of the source of the UnionNode
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : parent.getAssignments().entrySet()) {
                RowExpression translatedExpression;
                Type type = entry.getKey().getType();
                VariableReferenceExpression variable;
                if (isExpression(entry.getValue())) {
                    translatedExpression = castToRowExpression(ExpressionVariableInliner.inlineVariables(outputToInput, castToExpression(entry.getValue()), context.getVariableAllocator().getTypes()));
                    variable = context.getVariableAllocator().newVariable(castToExpression(translatedExpression), type);
                }
                else {
                    translatedExpression = RowExpressionVariableInliner.inlineVariables(source.sourceVariableMap(i), entry.getValue());
                    variable = context.getVariableAllocator().newVariable(translatedExpression);
                }
                assignments.put(variable, translatedExpression);
                projectVariableMapping.put(entry.getKey(), variable);
            }
            outputSources.add(new ProjectNode(context.getIdAllocator().getNextId(), source.getSources().get(i), assignments.build()));
            outputLayout.forEach(variable -> mappings.put(variable, projectVariableMapping.get(variable)));
        }

        return Result.ofPlanNode(new UnionNode(parent.getId(), outputSources.build(), mappings.build()));
    }
}
