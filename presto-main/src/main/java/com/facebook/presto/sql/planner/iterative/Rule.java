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
package com.facebook.presto.sql.planner.iterative;

import com.facebook.presto.Session;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Match;
import com.facebook.presto.matching.Matcher;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.Optional;
import java.util.function.BiFunction;

public interface Rule<T>
{
    /**
     * Returns a pattern to which plan nodes this rule applies.
     */
    Pattern<T> getPattern();

    default boolean isEnabled(Session session)
    {
        return true;
    }

    Optional<PlanNode> apply(T node, Captures captures, Context context);

    /**
     * This method is for working with Rules of unknown pattern result type ({@code Rule<?>}).
     * <p>
     * Given a {@code Rule<?>} it's impossible to produce a {@code Match<T>} instance that
     * could be passed to the Rule's apply method - because we don't know the Rule's {@code T}.
     * This method knows the Rule's {@code T}, so it can produce a compatible {@code Match<T>}.
     * <p>
     * This method creates a {@code Match<T>} for the Rule's pattern using the provided Matcher
     * and passes it - along with the rule - to the provided consumer. Because of this method's
     * type parameters, the Rule and the Match are guaranteed to be parametrized with the same
     * type, so the type system allows us to call {@code rule.apply(match.value(), ...)} within
     * the consumer's body.
     */
    default <R> R withMatch(Matcher matcher, PlanNode planNode, BiFunction<Rule<T>, Match<T>, R> consumer)
    {
        Match<T> match = matcher.match(getPattern(), planNode);
        return consumer.apply(this, match);
    }

    interface Context
    {
        Lookup getLookup();

        PlanNodeIdAllocator getIdAllocator();

        SymbolAllocator getSymbolAllocator();

        Session getSession();
    }
}
