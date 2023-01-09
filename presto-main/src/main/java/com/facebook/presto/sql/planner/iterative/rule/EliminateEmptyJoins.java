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
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isEmptyJoinOptimization;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isAtMost;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.relational.Expressions.constantNull;

public class EliminateEmptyJoins
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join();

    // Build assignment list for the new Project node as: X=X if X is from non-empty child and X=null otherwise.
    public static Assignments buildAssignments(Collection<VariableReferenceExpression> variables, PlanNode nonEmptyChild)
    {
        Assignments.Builder builder = Assignments.builder();
        for (VariableReferenceExpression variable : variables) {
            if (nonEmptyChild.getOutputVariables().contains(variable)) {
                builder.put(variable, variable);
            }
            else {
                builder.put(variable, constantNull(variable.getSourceLocation(), variable.getType()));
            }
        }
        return builder.build();
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        if (!isEmptyJoinOptimization(context.getSession())) {
            return Result.empty();
        }

        boolean leftChildEmpty;
        boolean rightChildEmpty;
        leftChildEmpty = isAtMost(context.getLookup().resolve(joinNode.getLeft()), context.getLookup(), 0);
        rightChildEmpty = isAtMost(context.getLookup().resolve(joinNode.getRight()), context.getLookup(), 0);

        /*
        Prune joins with one or more empty sources in the following cases. The pruning is done by replacing the whole join by empty values node.
        1. Both left and right children are empty. This works for all type of joins including outer joins.
        2. One of the left and right are empty and join is inner.
        3. Left child empty and left outer join.
        4. Right child empty and right outer join.
         */
        if ((leftChildEmpty && rightChildEmpty) ||
                ((leftChildEmpty || rightChildEmpty) && joinNode.getType() == JoinNode.Type.INNER)
                || (leftChildEmpty && joinNode.getType() == JoinNode.Type.LEFT)
                || (rightChildEmpty && joinNode.getType() == JoinNode.Type.RIGHT)) {
            return Result.ofPlanNode(
                    new ValuesNode(joinNode.getSourceLocation(), joinNode.getId(), joinNode.getOutputVariables(), Collections.emptyList(), Optional.empty()));
        }

        /*
        This covers the cases where the whole join can not be pruned for outer join cases.
        In this case, we optimize the join using a projection over the non-empty child.
        The following are 4 scenarios:
        1. S1 left outer join S2 and S2 is empty. The join is rewritten as Projection over S1 with null values for fields of S2. For example,
           "select t1.X, dt.Y from t1 left outer (select * from t2 where 1=0) is rewritten as select t1.X, null as Y from t1
        2. S1 right outer join S2 and S1 is empty. Similar to #1.
        3. S1 full outer join S2 and S1 is empty. This is can be reduce to S2 left outer join S1 and S1 is empty. Same logic of #1 is used.
        4. S1 full outer join S2 and S2 is empty. Similar to #3 and full outer join is reduced to S1 left outer join S2. Same logic is #1.
         */
        if (leftChildEmpty || rightChildEmpty) {
            PlanNode nonEmptyChild;
            if (leftChildEmpty) {
                nonEmptyChild = joinNode.getRight();
            }
            else {
                nonEmptyChild = joinNode.getLeft();
            }
            Assignments.Builder newProjections = Assignments.builder()
                    .putAll(buildAssignments(joinNode.getOutputVariables(), nonEmptyChild));

            return Result.ofPlanNode(new ProjectNode(joinNode.getSourceLocation(), joinNode.getId(), nonEmptyChild, newProjections.build(), LOCAL));
        }
        return Result.empty();
    }
}
