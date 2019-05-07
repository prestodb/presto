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

import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class Pattern<T>
{
    private final Class<T> type;
    private final Optional<Predicate<T>> propertyMatcher;
    private final Optional<List<Pattern<? extends PlanNode>>> sourceMatcher;
    private final Optional<List<Pattern<? extends PlanNode>>> parentMatcher;

    private Pattern(
            Class<T> type,
            Optional<Predicate<T>> propertyMatcher,
            Optional<List<Pattern<? extends PlanNode>>> sourceMatcher,
            Optional<List<Pattern<? extends PlanNode>>> parentMatcher)
    {
        this.type = requireNonNull(type, "type is null");
        this.propertyMatcher = requireNonNull(propertyMatcher, "propertyMatcher is null");
        this.sourceMatcher = requireNonNull(sourceMatcher, "sourceMatcher is null");
        this.parentMatcher = requireNonNull(parentMatcher, "parentMatcher is null");
    }

    public Class<T> getType()
    {
        return type;
    }

    public static <U> Pattern<U> typeOf(Class<U> type)
    {
        return new Pattern<>(type, Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static Pattern<PlanNode> any()
    {
        return typeOf(PlanNode.class);
    }

    public Pattern<T> with(Predicate<T> propertyMatcher)
    {
        Predicate<T> newPropertyMatcher = this.propertyMatcher.map(existing -> existing.and(propertyMatcher)).orElse(propertyMatcher);
        return new Pattern<>(type, Optional.of(newPropertyMatcher), Optional.empty(), parentMatcher);
    }

    public Pattern<T> sources(Pattern<? extends PlanNode>... sourcesPattern)
    {
        return new Pattern<>(type, propertyMatcher, Optional.of(ImmutableList.copyOf(sourcesPattern)), parentMatcher);
    }

    public Pattern<T> parents(Pattern<? extends PlanNode>... parentsPattern)
    {
        return new Pattern<>(type, propertyMatcher, sourceMatcher, Optional.of(ImmutableList.copyOf(parentsPattern)));
    }

    public Pattern<T> noParent()
    {
        return new Pattern<>(type, propertyMatcher, sourceMatcher, Optional.of(ImmutableList.of()));
    }

    public boolean matches(PlanIterator it)
    {
        if (!matches(it.get(PlanNode.class))) {
            return false;
        }

        return (!sourceMatcher.isPresent() || sourceMatcher.get().size() == it.getSources().size() &&
                Streams.zip(sourceMatcher.get().stream(), it.getSources().stream(), (pattern, source) -> pattern.matches(source)).allMatch(Boolean::booleanValue)) &&
                (!parentMatcher.isPresent() || parentMatcher.get().size() == it.getParents().size() &&
                        Streams.zip(parentMatcher.get().stream(), it.getParents().stream(), (pattern, parent) -> pattern.matches(parent)).allMatch(Boolean::booleanValue));
    }

    private boolean matches(PlanNode node)
    {
        return type.isInstance(node) &&
                propertyMatcher.map(predicate -> predicate.test(type.cast(node))).orElse(true);
    }
}
