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
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isEnableRemoveUnreferencedJoin;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.sql.planner.plan.Patterns.Join.type;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.distinctLimit;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.source;

/**
 * LEFT-JOIN by definition is "lossless", this optimization rule enforces JOIN operation to be removed if the project output is no columns
 * of the table are referenced in the output of the JOIN
 */
public class RemoveUnreferencedJoin
{
    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                new RemoveJoinFromDistinctLimit(),
                new RemoveJoinFromAggregation());
    }

    /**
     * DistinctLimit enforces the uniqueness of the output symbols. If there is no output columns from right table to reference in the output of the parent,
     * it is safe to remove the right table of the LEFT-JOIN
     */
    public static final class RemoveJoinFromDistinctLimit
            implements Rule<DistinctLimitNode>
    {
        private static final Capture<JoinNode> JOIN = Capture.newCapture();
        private static final Pattern<DistinctLimitNode> PATTERN =
                distinctLimit()
                        .with(source().matching(
                                join()
                                        .with(type().matching(type -> type == LEFT || type == RIGHT))
                                        .capturedAs(JOIN)));

        @Override
        public boolean isEnabled(Session session)
        {
            return isEnableRemoveUnreferencedJoin(session);
        }

        @Override
        public Pattern<DistinctLimitNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Result apply(DistinctLimitNode node, Captures captures, Context context)
        {
            JoinNode childJoin = captures.get(JOIN);

            if (childJoin.getType() == LEFT) {
                return pruneJoinFromDistinctLimit(
                        childJoin.getRight().getOutputVariables(),
                        childJoin.getLeft().getOutputVariables(),
                        node,
                        childJoin.getLeft(),
                        context);
            }
            else {
                return pruneJoinFromDistinctLimit(
                        childJoin.getLeft().getOutputVariables(),
                        childJoin.getRight().getOutputVariables(),
                        node,
                        childJoin.getRight(),
                        context);
            }
        }
    }

    private static Rule.Result pruneJoinFromDistinctLimit(
            List<VariableReferenceExpression> unreferencedJoinOutput,
            List<VariableReferenceExpression> referencedJoinOutput,
            DistinctLimitNode distinctLimitNode,
            PlanNode source,
            Rule.Context context)
    {
        Assignments.Builder newAssignments = Assignments.builder();
        if (!distinctLimitNode.getDistinctVariables().containsAll(unreferencedJoinOutput)) {
            for (VariableReferenceExpression outputVariable : referencedJoinOutput) {
                newAssignments.put(outputVariable, outputVariable);
            }

            return Rule.Result.ofPlanNode(
                    new DistinctLimitNode(
                            context.getIdAllocator().getNextId(),
                            new ProjectNode(
                                    context.getIdAllocator().getNextId(),
                                    source,
                                    newAssignments.build()),
                            distinctLimitNode.getLimit(),
                            distinctLimitNode.isPartial(),
                            distinctLimitNode.getDistinctVariables(),
                            distinctLimitNode.getHashVariable()));
        }
        return Rule.Result.empty();
    }

    /**
     * Aggregation enforces the uniqueness of the output symbols too, check to prune JOIN if possible
     */
    public static final class RemoveJoinFromAggregation
            implements Rule<AggregationNode>
    {
        private static final Capture<JoinNode> JOIN = Capture.newCapture();
        private static final Pattern<AggregationNode> PATTERN =
                aggregation()
                        .with(source().matching(
                                join()
                                        .with(type().matching(type -> type == LEFT || type == RIGHT))
                                        .capturedAs(JOIN)));

        @Override
        public boolean isEnabled(Session session)
        {
            return isEnableRemoveUnreferencedJoin(session);
        }

        @Override
        public Pattern<AggregationNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Result apply(AggregationNode node, Captures captures, Context context)
        {
            JoinNode childJoin = captures.get(JOIN);

            if (childJoin.getType() == LEFT) {
                return pruneJoinFromAggregation(
                        childJoin.getRight().getOutputVariables(),
                        childJoin.getLeft().getOutputVariables(),
                        node,
                        childJoin.getLeft(),
                        context);
            }
            else {
                return pruneJoinFromAggregation(
                        childJoin.getLeft().getOutputVariables(),
                        childJoin.getRight().getOutputVariables(),
                        node,
                        childJoin.getRight(),
                        context);
            }
        }

        private static Rule.Result pruneJoinFromAggregation(
                List<VariableReferenceExpression> unreferencedJoinOutput,
                List<VariableReferenceExpression> referencedJoinOutput,
                AggregationNode aggregationNode,
                PlanNode source,
                Rule.Context context)
        {
            Assignments.Builder newAssignments = Assignments.builder();
            if (checkDistinctColumnToPruneJoin(aggregationNode, unreferencedJoinOutput, referencedJoinOutput)) {
                for (VariableReferenceExpression outputVariable : referencedJoinOutput) {
                    newAssignments.put(outputVariable, outputVariable);
                }

                return Result.ofPlanNode(
                        new AggregationNode(
                                context.getIdAllocator().getNextId(),
                                new ProjectNode(context.getIdAllocator().getNextId(),
                                        source,
                                        newAssignments.build()),
                                aggregationNode.getAggregations(),
                                aggregationNode.getGroupingSets(),
                                aggregationNode.getPreGroupedVariables(),
                                aggregationNode.getStep(),
                                aggregationNode.getHashVariable(),
                                aggregationNode.getGroupIdVariable()));
            }
            return Rule.Result.empty();
        }

        /**
         * @param node - AggregationNode input
         * @param unreferencedOutput - List of unreferenced Output
         * @param referencedJoinOutput - List of referenced Output
         * @return - true if ok to prune Joinnode
         * - false if not qualified to prune JoinNode
         */
        private static boolean checkDistinctColumnToPruneJoin(
                AggregationNode node,
                List<VariableReferenceExpression> unreferencedOutput,
                List<VariableReferenceExpression> referencedJoinOutput)
        {
            if (node.getAggregations().isEmpty()) {
                if (!node.getOutputVariables().containsAll(unreferencedOutput)) {
                    return true;
                }
                else {
                    return false;
                }
            }
            else {
                if (node.getAggregations().values().stream().anyMatch(aggregation -> aggregation.getCall().getArguments().stream().anyMatch(arg -> unreferencedOutput.contains(arg))) ||
                        checkMultiOutputAgg(node.getGroupingKeys(), node.getOutputVariables(), unreferencedOutput, referencedJoinOutput)) {
                    return false;
                }
                else {
                    return true;
                }
            }
        }

        /**
         * @param groupingKeys
         * @param aggOutput
         * @param unreferencedOutput
         * @param referencedJoinOutput
         * @return true if not ok to prune Joinnode
         * false if qualified to prune JoinNode
         */
        private static boolean checkMultiOutputAgg(
                List<VariableReferenceExpression> groupingKeys,
                List<VariableReferenceExpression> aggOutput,
                List<VariableReferenceExpression> unreferencedOutput,
                List<VariableReferenceExpression> referencedJoinOutput)
        {
            // if no grouping key, uniqueness is not guaranteed, so cannot prune joinNode
            if (groupingKeys.isEmpty()) {
                if (aggOutput.stream().anyMatch(variable -> unreferencedOutput.contains(variable)) ||
                        !aggOutput.stream().anyMatch(variable -> referencedJoinOutput.contains(variable))) {
                    return true;
                }
                else {
                    return false;
                }
            }
            else {
                if (aggOutput.stream().anyMatch(variable -> groupingKeys.contains(variable)) &&
                        aggOutput.size() > groupingKeys.size()) {
                    return true;
                }
                else {
                    return false;
                }
            }
        }
    }
}
