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

import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.RuleSet;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ExpressionRewriteRuleSet
        implements RuleSet
{
    private final ExpressionRewriteRule rewriter;
    private final List<Class<? extends PlanNode>> supportedPlanNodes;

    public ExpressionRewriteRuleSet(ExpressionRewriteRule rewrite)
    {
        this(rewrite, ImmutableList.of(
                FilterNode.class,
                ProjectNode.class,
                AggregationNode.class,
                ApplyNode.class,
                TableScanNode.class,
                JoinNode.class,
                ValuesNode.class));
    }

    public ExpressionRewriteRuleSet(ExpressionRewriteRule rewrite, List<Class<? extends PlanNode>> supportedPlanNodes)
    {
        this.rewriter = requireNonNull(rewrite);
        this.supportedPlanNodes = supportedPlanNodes;
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.copyOf(ExpressionRewriteRuleFactory.createRules(rewriter, supportedPlanNodes));
    }
}
