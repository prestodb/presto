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
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.google.common.collect.ImmutableList;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.iterative.rule.Util.pruneNestedFields;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;

public abstract class ProjectOffPushDownFieldRule<N extends PlanNode>
        implements Rule<ProjectNode>
{
    private final Capture<N> targetCapture = newCapture();

    private final Pattern<N> targetPattern;

    protected ProjectOffPushDownFieldRule(Pattern<N> targetPattern)
    {
        this.targetPattern = targetPattern;
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(targetPattern.capturedAs(targetCapture)));
    }

    @Override
    public Result apply(ProjectNode parent, Captures captures, Context context)
    {
        N targetNode = captures.get(targetCapture);

        return pruneNestedFields(targetNode.getOutputSymbols(), parent.getAssignments().getExpressions())
                .flatMap(prunedOutputs -> this.pushDownProjectOff(context.getIdAllocator(), targetNode, prunedOutputs))
                .map(newChild -> parent.replaceChildren(ImmutableList.of(newChild)))
                .map(Result::ofPlanNode)
                .orElse(Result.empty());
    }

    protected abstract Optional<PlanNode> pushDownProjectOff(PlanNodeIdAllocator idAllocator, N targetNode, Set<Symbol> referencedOutputs);
}
