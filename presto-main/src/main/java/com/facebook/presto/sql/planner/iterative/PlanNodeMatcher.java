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

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.DefaultMatcher;
import com.facebook.presto.matching.Match;
import com.facebook.presto.matching.pattern.WithPattern;

import java.util.Optional;
import java.util.function.Function;

public class PlanNodeMatcher
        extends DefaultMatcher
{
    private final Lookup lookup;

    public PlanNodeMatcher(Lookup lookup)
    {
        this.lookup = lookup;
    }

    @Override
    public <T> Match<T> matchWith(WithPattern<T> withPattern, Object object, Captures captures)
    {
        Function<? super T, Optional<?>> property = withPattern.getProperty().getFunction();
        Optional<?> propertyValue = property.apply((T) object);

        Optional<?> resolvedValue = propertyValue
                .map(value -> value instanceof GroupReference ? lookup.resolve(((GroupReference) value)) : value);

        Match<?> propertyMatch = resolvedValue
                .map(value -> match(withPattern.getPattern(), value, captures))
                .orElse(Match.empty());
        return propertyMatch.map(ignored -> (T) object);
    }
}
