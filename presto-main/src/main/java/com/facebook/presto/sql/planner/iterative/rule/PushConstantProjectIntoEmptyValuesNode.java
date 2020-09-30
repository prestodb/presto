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

import com.facebook.presto.Session;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.SystemSessionProperties.isOptimizeUnionOverValues;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.values;

public class PushConstantProjectIntoEmptyValuesNode
        implements Rule<ProjectNode>
{
    private static final Capture<ValuesNode> CHILD = newCapture();
    private static final Pattern<ProjectNode> PATTERN = project()
            .matching(x -> x.getAssignments().getExpressions().stream().allMatch(y -> y instanceof ConstantExpression))
            .with(source().matching(values().capturedAs(CHILD)));

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isOptimizeUnionOverValues(session);
    }

    @Override
    public Result apply(ProjectNode node, Captures captures, Context context)
    {
        ValuesNode child = captures.get(CHILD);
        // Checking that ValuesNode has only one output row which is empty
        if (child.getRows().isEmpty() || !child.getRows().get(0).isEmpty()) {
            return Result.empty();
        }

        List<RowExpression> outputRow = new ArrayList<>(node.getOutputVariables().size());

        for (int i = 0; i < node.getOutputVariables().size(); i++) {
            outputRow.add(node.getAssignments().get(node.getOutputVariables().get(i)));
        }

        if (!child.getRows().isEmpty() && child.getRows().size() == 1 && child.getRows().get(0).isEmpty()) {
            return Result.ofPlanNode(
                    new ValuesNode(
                            context.getIdAllocator().getNextId(),
                            node.getOutputVariables(),
                            ImmutableList.of(outputRow)));
        }
        return Result.empty();
    }
}
