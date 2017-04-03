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

import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.InPredicate;

import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;

/**
 * This optimizers looks for InPredicate expressions in ApplyNodes and replaces the nodes with SemiJoin nodes.
 * <p/>
 * Plan before optimizer:
 * <pre>
 * Filter(a IN b):
 *   Apply
 *     - correlation: []  // empty
 *     - input: some plan A producing symbol a
 *     - subquery: some plan B producing symbol b
 * </pre>
 * <p/>
 * Plan after optimizer:
 * <pre>
 * Filter(semijoinresult):
 *   SemiJoin
 *     - source: plan A
 *     - filteringSource: symbol a
 *     - sourceJoinSymbol: plan B
 *     - filteringSourceJoinSymbol: symbol b
 *     - semiJoinOutput: semijoinresult
 * </pre>
 */
public class TransformUncorrelatedInPredicateSubqueryToSemiJoin
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        if (!(node instanceof ApplyNode)) {
            return Optional.empty();
        }

        ApplyNode applyNode = (ApplyNode) node;

        if (!applyNode.getCorrelation().isEmpty()) {
            return Optional.empty();
        }

        if (applyNode.getSubqueryAssignments().size() != 1) {
            return Optional.empty();
        }

        Expression expression = getOnlyElement(applyNode.getSubqueryAssignments().getExpressions());
        if (!(expression instanceof InPredicate)) {
            return Optional.empty();
        }

        InPredicate inPredicate = (InPredicate) expression;
        Symbol semiJoinSymbol = getOnlyElement(applyNode.getSubqueryAssignments().getSymbols());

        SemiJoinNode replacement = new SemiJoinNode(idAllocator.getNextId(),
                applyNode.getInput(),
                applyNode.getSubquery(),
                Symbol.from(inPredicate.getValue()),
                Symbol.from(inPredicate.getValueList()),
                semiJoinSymbol,
                Optional.empty(),
                Optional.empty(),
                Optional.empty()
        );

        return Optional.of(replacement);
    }
}
