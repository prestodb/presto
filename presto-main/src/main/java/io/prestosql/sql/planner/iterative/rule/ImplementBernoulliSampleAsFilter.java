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
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.SampleNode;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.DoubleLiteral;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.QualifiedName;

import static io.prestosql.sql.planner.plan.Patterns.Sample.sampleType;
import static io.prestosql.sql.planner.plan.Patterns.sample;
import static io.prestosql.sql.planner.plan.SampleNode.Type.BERNOULLI;

/**
 * Transforms:
 * <pre>
 * - Sample(BERNOULLI, p)
 *     - X
 * </pre>
 * Into:
 * <pre>
 * - Filter (rand() < p)
 *     - X
 * </pre>
 */
public class ImplementBernoulliSampleAsFilter
        implements Rule<SampleNode>
{
    private static final Pattern<SampleNode> PATTERN = sample()
            .with(sampleType().equalTo(BERNOULLI));

    @Override
    public Pattern<SampleNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(SampleNode sample, Captures captures, Context context)
    {
        return Result.ofPlanNode(new FilterNode(
                sample.getId(),
                sample.getSource(),
                new ComparisonExpression(
                        ComparisonExpression.Operator.LESS_THAN,
                        new FunctionCall(QualifiedName.of("rand"), ImmutableList.of()),
                        new DoubleLiteral(Double.toString(sample.getSampleRatio())))));
    }
}
