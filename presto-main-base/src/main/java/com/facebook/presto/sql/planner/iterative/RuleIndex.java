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

import com.facebook.presto.matching.Pattern;
import com.facebook.presto.matching.pattern.TypeOfPattern;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.reflect.TypeToken;

import java.util.Set;
import java.util.stream.Stream;

public class RuleIndex
{
    private final ListMultimap<Class<?>, Rule<?>> rulesByRootType;

    private RuleIndex(ListMultimap<Class<?>, Rule<?>> rulesByRootType)
    {
        this.rulesByRootType = ImmutableListMultimap.copyOf(rulesByRootType);
    }

    public Stream<Rule<?>> getCandidates(Object object)
    {
        return supertypes(object.getClass())
                .flatMap(clazz -> rulesByRootType.get(clazz).stream());
    }

    private static Stream<Class<?>> supertypes(Class<?> type)
    {
        return TypeToken.of(type).getTypes().stream()
                .map(TypeToken::getRawType);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final ImmutableListMultimap.Builder<Class<?>, Rule<?>> rulesByRootType = ImmutableListMultimap.builder();

        public Builder register(Set<Rule<?>> rules)
        {
            rules.forEach(this::register);
            return this;
        }

        public Builder register(Rule<?> rule)
        {
            Pattern pattern = getFirstPattern(rule.getPattern());
            if (pattern instanceof TypeOfPattern<?>) {
                rulesByRootType.put(((TypeOfPattern<?>) pattern).expectedClass(), rule);
            }
            else {
                throw new IllegalArgumentException("Unexpected Pattern: " + pattern);
            }
            return this;
        }

        private Pattern<?> getFirstPattern(Pattern<?> pattern)
        {
            while (pattern.previous() != null) {
                pattern = pattern.previous();
            }
            return pattern;
        }

        public RuleIndex build()
        {
            return new RuleIndex(rulesByRootType.build());
        }
    }
}
