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
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.ExternalCallExpressionChecker;

import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static java.util.Objects.requireNonNull;

public class RewriteFilterWithExternalFunctionToProject
        implements Rule<FilterNode>
{
    private static final Pattern<FilterNode> PATTERN = filter();

    private final FunctionAndTypeManager functionAndTypeManager;

    public RewriteFilterWithExternalFunctionToProject(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode node, Captures captures, Context context)
    {
        if (!node.getPredicate().accept(new ExternalCallExpressionChecker(functionAndTypeManager), null)) {
            // No remote function in predicate
            return Result.empty();
        }
        VariableReferenceExpression predicateVariable = context.getVariableAllocator().newVariable(node.getPredicate());
        Assignments.Builder assignments = Assignments.builder();
        node.getOutputVariables().forEach(variable -> assignments.put(variable, variable));
        Assignments identityAssignments = assignments.build();
        assignments.put(predicateVariable, node.getPredicate());

        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new FilterNode(
                            context.getIdAllocator().getNextId(),
                            new ProjectNode(context.getIdAllocator().getNextId(), node.getSource(), assignments.build()),
                            predicateVariable),
                        identityAssignments,
                        LOCAL));
    }
}
