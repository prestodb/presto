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
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Pattern;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.Expression;

import java.util.Optional;

import static com.facebook.presto.sql.planner.optimizations.CanonicalizeExpressions.canonicalizeExpression;

public class CanonicalizeJoinExpressions
        implements Rule
{
    private static final Pattern PATTERN = Pattern.node(JoinNode.class);

    @Override
    public Pattern getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        JoinNode joinNode = (JoinNode) node;

        if (!joinNode.getFilter().isPresent()) {
            return Optional.empty();
        }

        Expression canonicalizedExpression = canonicalizeExpression(joinNode.getFilter().get());
        if (canonicalizedExpression.equals(joinNode.getFilter().get())) {
            return Optional.empty();
        }

        JoinNode replacement = new JoinNode(
                joinNode.getId(),
                joinNode.getType(),
                joinNode.getLeft(),
                joinNode.getRight(),
                joinNode.getCriteria(),
                joinNode.getOutputSymbols(),
                Optional.of(canonicalizedExpression),
                joinNode.getLeftHashSymbol(),
                joinNode.getRightHashSymbol(),
                joinNode.getDistributionType());

        return Optional.of(replacement);
    }
}
