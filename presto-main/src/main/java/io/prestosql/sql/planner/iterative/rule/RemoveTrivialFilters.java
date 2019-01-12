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

import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.ValuesNode;
import io.prestosql.sql.tree.Expression;

import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.Collections.emptyList;

public class RemoveTrivialFilters
        implements Rule<FilterNode>
{
    private static final Pattern<FilterNode> PATTERN = filter();

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode filterNode, Captures captures, Context context)
    {
        Expression predicate = filterNode.getPredicate();

        if (predicate.equals(TRUE_LITERAL)) {
            return Result.ofPlanNode(filterNode.getSource());
        }

        if (predicate.equals(FALSE_LITERAL)) {
            return Result.ofPlanNode(new ValuesNode(context.getIdAllocator().getNextId(), filterNode.getOutputSymbols(), emptyList()));
        }

        return Result.empty();
    }
}
