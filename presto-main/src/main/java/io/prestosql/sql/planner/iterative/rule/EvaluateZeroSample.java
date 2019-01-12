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
import io.prestosql.sql.planner.plan.SampleNode;
import io.prestosql.sql.planner.plan.ValuesNode;

import static io.prestosql.sql.planner.plan.Patterns.Sample.sampleRatio;
import static io.prestosql.sql.planner.plan.Patterns.sample;

/**
 * Replaces 0% sample node with empty values node.
 */
public class EvaluateZeroSample
        implements Rule<SampleNode>
{
    private static final Pattern<SampleNode> PATTERN = sample()
            .with(sampleRatio().equalTo(0.0));

    @Override
    public Pattern<SampleNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(SampleNode sample, Captures captures, Context context)
    {
        return Result.ofPlanNode(new ValuesNode(sample.getId(), sample.getOutputSymbols(), ImmutableList.of()));
    }
}
