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

import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

public class MergeFilters
        implements Rule<FilterNode>
{
    private static final Capture<FilterNode> CHILD = newCapture();

    private static final Pattern<FilterNode> PATTERN = filter()
            .with(source().matching(filter().capturedAs(CHILD)));

    private final LogicalRowExpressions logicalRowExpressions;

    public MergeFilters(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.logicalRowExpressions = new LogicalRowExpressions(
                new RowExpressionDeterminismEvaluator(functionAndTypeManager),
                new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver()),
                functionAndTypeManager);
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode parent, Captures captures, Context context)
    {
        FilterNode child = captures.get(CHILD);

        return Result.ofPlanNode(
                new FilterNode(
                        parent.getSourceLocation(),
                        parent.getId(),
                        child.getSource(),
                        logicalRowExpressions.combineConjuncts(child.getPredicate(), parent.getPredicate())));
    }
}
