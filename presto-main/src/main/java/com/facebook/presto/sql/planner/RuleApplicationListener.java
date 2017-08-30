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
package com.facebook.presto.sql.planner;

import com.facebook.presto.matching.Captures;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.Optional;

public interface RuleApplicationListener
{
    void appliedRule(RuleApplication ruleApplication);

    class RuleApplication
    {
        private final Rule<?> rule;
        private final PlanNode node;
        private final Captures captures;
        private final Rule.Context context;
        private final Optional<PlanNode> result;

        public RuleApplication(Rule<?> rule, PlanNode node, Captures captures, Rule.Context context, Optional<PlanNode> result)
        {
            this.rule = rule;
            this.node = node;
            this.captures = captures;
            this.context = context;
            this.result = result;
        }

        public PlanNode getNode()
        {
            return node;
        }

        public Rule<?> getRule()
        {
            return rule;
        }

        public Captures getCaptures()
        {
            return captures;
        }

        public Optional<PlanNode> getResult()
        {
            return result;
        }

        public Rule.Context getContext()
        {
            return context;
        }
    }

    static RuleApplicationListener noListener()
    {
        return ruleApplication -> {
        };
    }
}
