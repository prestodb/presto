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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;

import java.util.Optional;
import java.util.Set;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.iterative.rule.Util.pruneInputs;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;

/**
 * @param <N> The node type to look for under the ProjectNode
 * Looks for a Project parent over a N child, such that the parent doesn't use all the output columns of the child.
 * Given that situation, invokes the pushDownProjectOff helper to possibly rewrite the child to produce fewer outputs.
 */
public abstract class ProjectOffPushDownRule<N extends PlanNode>
        implements Rule<ProjectNode>
{
    private final Capture<N> targetCapture = newCapture();

    private final Pattern<N> targetPattern;

    protected ProjectOffPushDownRule(Pattern<N> targetPattern)
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

        return pruneInputs(targetNode.getOutputSymbols(), parent.getAssignments().getExpressions())
                .flatMap(prunedOutputs -> this.pushDownProjectOff(context.getIdAllocator(), targetNode, prunedOutputs))
                .map(newChild -> parent.replaceChildren(ImmutableList.of(newChild)))
                .map(Result::ofPlanNode)
                .orElse(Result.empty());
    }

    protected abstract Optional<PlanNode> pushDownProjectOff(PlanNodeIdAllocator idAllocator, N targetNode, Set<Symbol> referencedOutputs);
}
