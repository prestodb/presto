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
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.sql.planner.iterative.Rule;

import static com.facebook.presto.SystemSessionProperties.isBatchAggregationEvaluationEnabled;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static java.util.Objects.requireNonNull;

public class BatchAggregation
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation();
    private final FunctionAndTypeManager functionAndTypeManager;

    public BatchAggregation(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isBatchAggregationEvaluationEnabled(session);
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        return Result.empty();
    }
}
