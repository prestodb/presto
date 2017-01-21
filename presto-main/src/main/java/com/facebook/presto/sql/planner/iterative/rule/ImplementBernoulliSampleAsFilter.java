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
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

/**
 * Transforms:
 *
 * <pre>
 * - Sample(BERNOULLI, p)
 *     - X
 * </pre>
 *
 * Into:
 *
 * <pre>
 * - Filter (rand() < p)
 *     - X
 * </pre>
 */
public class ImplementBernoulliSampleAsFilter
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        if (!(node instanceof SampleNode)) {
            return Optional.empty();
        }

        SampleNode sample = (SampleNode) node;

        if (sample.getSampleType() != SampleNode.Type.BERNOULLI) {
            return Optional.empty();
        }

        return Optional.of(new FilterNode(
                node.getId(),
                sample.getSource(),
                new ComparisonExpression(
                        ComparisonExpressionType.LESS_THAN,
                        new FunctionCall(QualifiedName.of("rand"), ImmutableList.<Expression>of()),
                        new DoubleLiteral(Double.toString(sample.getSampleRatio())))));
    }
}
