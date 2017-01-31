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
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Streams;

import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

public class RuleStore
{
    private final ListMultimap<Class<? extends PlanNode>, Rule> rulesByClass;

    private RuleStore(ListMultimap<Class<? extends PlanNode>, Rule> rulesByClass)
    {
        this.rulesByClass = ImmutableListMultimap.copyOf(rulesByClass);
    }

    public Stream<Rule> getCandidates(PlanNode planNode)
    {
        return Streams.stream(ancestors(planNode.getClass()))
                .flatMap(clazz -> rulesByClass.get(clazz).stream());
    }

    private static Iterator<Class<? extends PlanNode>> ancestors(Class<? extends PlanNode> planNodeClass)
    {
        return new AbstractIterator<Class<? extends PlanNode>>() {
            private Class<?> current = planNodeClass;

            @Override
            protected Class<? extends PlanNode> computeNext()
            {
                if (!PlanNode.class.isAssignableFrom(current)) {
                    return endOfData();
                }

                Class<? extends PlanNode> result = (Class<? extends PlanNode>) current;
                current = current.getSuperclass();

                return result;
            }
        };
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final ImmutableListMultimap.Builder<Class<? extends PlanNode>, Rule> rulesByClass = ImmutableListMultimap.builder();

        public Builder register(Set<Rule> newRules)
        {
            newRules.forEach(this::register);
            return this;
        }

        public Builder register(Rule newRule)
        {
            Pattern pattern = newRule.getPattern();
            if (pattern instanceof Pattern.MatchNodeClass) {
                rulesByClass.put(((Pattern.MatchNodeClass) pattern).getNodeClass(), newRule);
            }
            else {
                throw new IllegalArgumentException("Unexpected Pattern: " + pattern);
            }
            return this;
        }

        public RuleStore build()
        {
            return new RuleStore(rulesByClass.build());
        }
    }
}
