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
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.PlanNode;

import static io.prestosql.sql.planner.plan.Patterns.exchange;
import static java.util.Objects.requireNonNull;

public final class PreconditionRules
{
    private PreconditionRules() {}

    public static Rule<ExchangeNode> checkRulesAreFiredBeforeAddExchangesRule()
    {
        return checkNoPlanNodeMatches(exchange(), "Expected rules to be fired before 'AddExchanges' optimizer");
    }

    private static <T extends PlanNode> Rule<T> checkNoPlanNodeMatches(Pattern<T> pattern, String message)
    {
        return new CheckNoPlanNodeMatchesRule<>(pattern, message);
    }

    private static class CheckNoPlanNodeMatchesRule<T extends PlanNode>
            implements Rule<T>
    {
        private final Pattern<T> pattern;
        private final String message;

        public CheckNoPlanNodeMatchesRule(Pattern<T> pattern, String message)
        {
            this.pattern = requireNonNull(pattern, "pattern is null");
            this.message = requireNonNull(message, "message is null");
        }

        @Override
        public Pattern<T> getPattern()
        {
            return pattern;
        }

        @Override
        public Result apply(T node, Captures captures, Context context)
        {
            throw new IllegalStateException(message);
        }
    }
}
