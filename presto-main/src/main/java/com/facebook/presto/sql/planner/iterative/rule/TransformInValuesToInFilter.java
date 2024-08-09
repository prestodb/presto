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
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.SystemSessionProperties.isTransformInValuesToInFilterEnabled;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IN;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.Patterns.filteringSource;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.semiJoin;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.values;
import static com.facebook.presto.sql.relational.Expressions.specialForm;

/**
 * This optimizer looks for SemiJoinNode whose filteringSource has only one column, then transform the SemijoinNode into ProjectNode with predicate variable for filtering.
 * <p/>
 * Plan before optimizer:
 * <pre>
 * SemiJoinNode (semiJoinOutput variable c):
 *   - source (sourceJoinVariable a)
 *   - filteringSource (one column variable b)
 *       - ProjectNode
 *            - ValuesNode
 * </pre>
 * <p/>
 * Plan after optimizer:
 * <pre>
 * ProjectNode:
 *   - source
 *   - assignments(identityAssignments of source output,c in b)
 * </pre>
 */
public class TransformInValuesToInFilter
        implements Rule<SemiJoinNode>
{
    private static final Pattern<SemiJoinNode> PATTERN = semiJoin().with(filteringSource()
            .matching(project().with(source()
                    .matching(values()))));

    @Override
    public Pattern<SemiJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isTransformInValuesToInFilterEnabled(session);
    }

    @Override
    public Result apply(SemiJoinNode semiJoinNode, Captures captures, Context context)
    {
        PlanNode source = semiJoinNode.getSource();
        return context.getLookup().resolveGroup(semiJoinNode.getFilteringSource()).findFirst()
                .flatMap(projectNode -> context.getLookup().resolveGroup(projectNode.getSources().get(0)).findFirst())
                .map(ValuesNode.class::cast)
                .map(ValuesNode::getRows)
                // check that all values are only a single row expression (no struct/row types)
                .filter(valuesRows -> valuesRows.stream().noneMatch(row -> row.size() > 1))
                .map(rows -> rows.stream().map(row -> row.get(0)).iterator())
                .map(args -> {
                    RowExpression predicate = specialForm(IN, BOOLEAN, ImmutableList.<RowExpression>builder().add(semiJoinNode.getSourceJoinVariable()).addAll(args).build());
                    Assignments.Builder builder = Assignments.builder();
                    builder.putAll(identityAssignments(source.getOutputVariables()))
                            .put(semiJoinNode.getSemiJoinOutput(), predicate);
                    ProjectNode projectNode = new ProjectNode(context.getIdAllocator().getNextId(), source, builder.build());
                    return Result.ofPlanNode(projectNode);
                }).orElse(Result.empty());
    }
}
