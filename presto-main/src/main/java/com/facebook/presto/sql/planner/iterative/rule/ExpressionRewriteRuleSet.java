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

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.RuleSet;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.applyNode;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static com.facebook.presto.sql.planner.plan.Patterns.values;

public class ExpressionRewriteRuleSet
        implements RuleSet
{
    public interface ExpressionRewriteRule
    {
        Expression rewrite(Expression expression, Rule.Context context);
    }

    private final ExpressionRewriteRule rewriter;

    private final ImmutableSet<Rule<?>> rules = ImmutableSet.of(
            new ProjectExpressionRewrite(),
            new AggregationExpressionRewrite(),
            new FilterExpressionRewrite(),
            new TableScanExpressionRewrite(),
            new JoinExpressionRewrite(),
            new ValuesExpressionRewrite(),
            new ApplyExpressionRewrite());

    public ExpressionRewriteRuleSet(ExpressionRewriteRule rewrite)
    {
        this.rewriter = rewrite;
    }

    public Set<Rule<?>> rules()
    {
        return rules;
    }

    private final class ProjectExpressionRewrite
            implements Rule<ProjectNode>
    {
        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return project();
        }

        @Override
        public Optional<PlanNode> apply(ProjectNode projectNode, Captures captures, Context context)
        {
            Assignments assignments = projectNode.getAssignments().rewrite(x -> ExpressionRewriteRuleSet.this.rewriter.rewrite(x, context));
            if (projectNode.getAssignments().equals(assignments)) {
                return Optional.empty();
            }
            return Optional.of(new ProjectNode(projectNode.getId(), projectNode.getSource(), assignments));
        }
    }

    private final class AggregationExpressionRewrite
            implements Rule<AggregationNode>
    {
        @Override
        public Pattern<AggregationNode> getPattern()
        {
            return aggregation();
        }

        @Override
        public Optional<PlanNode> apply(AggregationNode aggregationNode, Captures captures, Context context)
        {
            boolean anyRewritten = false;
            ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();
            for (Map.Entry<Symbol, Aggregation> aggregation : aggregationNode.getAggregations().entrySet()) {
                FunctionCall call = (FunctionCall) rewriter.rewrite(aggregation.getValue().getCall(), context);
                aggregations.put(aggregation.getKey(), new Aggregation(call, aggregation.getValue().getSignature(), aggregation.getValue().getMask()));
                if (!aggregation.getValue().getCall().equals(call)) {
                    anyRewritten = true;
                }
            }
            if (anyRewritten) {
                return Optional.of(new AggregationNode(
                        aggregationNode.getId(),
                        aggregationNode.getSource(),
                        aggregations.build(),
                        aggregationNode.getGroupingSets(),
                        aggregationNode.getStep(),
                        aggregationNode.getHashSymbol(),
                        aggregationNode.getGroupIdSymbol()));
            }
            return Optional.empty();
        }
    }

    private final class FilterExpressionRewrite
            implements Rule<FilterNode>
    {
        @Override
        public Pattern<FilterNode> getPattern()
        {
            return filter();
        }

        @Override
        public Optional<PlanNode> apply(FilterNode filterNode, Captures captures, Context context)
        {
            Expression rewritten = rewriter.rewrite(filterNode.getPredicate(), context);
            if (filterNode.getPredicate().equals(rewritten)) {
                return Optional.empty();
            }
            return Optional.of(new FilterNode(filterNode.getId(), filterNode.getSource(), rewritten));
        }
    }

    private final class TableScanExpressionRewrite
            implements Rule<TableScanNode>
    {
        @Override
        public Pattern<TableScanNode> getPattern()
        {
            return tableScan();
        }

        @Override
        public Optional<PlanNode> apply(TableScanNode tableScanNode, Captures captures, Context context)
        {
            if (tableScanNode.getOriginalConstraint() == null) {
                return Optional.empty();
            }
            Expression rewrittenOriginalContraint = rewriter.rewrite(tableScanNode.getOriginalConstraint(), context);
            if (!tableScanNode.getOriginalConstraint().equals(rewrittenOriginalContraint)) {
                return Optional.of(new TableScanNode(
                        tableScanNode.getId(),
                        tableScanNode.getTable(),
                        tableScanNode.getOutputSymbols(),
                        tableScanNode.getAssignments(),
                        tableScanNode.getLayout(),
                        tableScanNode.getCurrentConstraint(),
                        rewrittenOriginalContraint));
            }
            return Optional.empty();
        }
    }

    private final class JoinExpressionRewrite
            implements Rule<JoinNode>
    {
        @Override
        public Pattern<JoinNode> getPattern()
        {
            return join();
        }

        @Override
        public Optional<PlanNode> apply(JoinNode joinNode, Captures captures, Context context)
        {
            Optional<Expression> filter = joinNode.getFilter().map(x -> ExpressionRewriteRuleSet.this.rewriter.rewrite(x, context));
            if (!joinNode.getFilter().equals(filter)) {
                return Optional.of(new JoinNode(
                        joinNode.getId(),
                        joinNode.getType(),
                        joinNode.getLeft(),
                        joinNode.getRight(),
                        joinNode.getCriteria(),
                        joinNode.getOutputSymbols(),
                        filter,
                        joinNode.getLeftHashSymbol(),
                        joinNode.getRightHashSymbol(),
                        joinNode.getDistributionType()));
            }
            return Optional.empty();
        }
    }

    private final class ValuesExpressionRewrite
            implements Rule<ValuesNode>
    {
        @Override
        public Pattern<ValuesNode> getPattern()
        {
            return values();
        }

        @Override
        public Optional<PlanNode> apply(ValuesNode valuesNode, Captures captures, Context context)
        {
            boolean anyRewritten = false;
            ImmutableList.Builder<List<Expression>> rows = ImmutableList.builder();
            for (List<Expression> row : valuesNode.getRows()) {
                ImmutableList.Builder<Expression> newRow = ImmutableList.builder();
                for (Expression expression : row) {
                    Expression rewritten = rewriter.rewrite(expression, context);
                    if (!expression.equals(rewritten)) {
                        anyRewritten = true;
                    }
                    newRow.add(rewritten);
                }
                rows.add(newRow.build());
            }
            if (anyRewritten) {
                return Optional.of(new ValuesNode(valuesNode.getId(), valuesNode.getOutputSymbols(), rows.build()));
            }
            return Optional.empty();
        }
    }

    private final class ApplyExpressionRewrite
            implements Rule<ApplyNode>
    {
        @Override
        public Pattern<ApplyNode> getPattern()
        {
            return applyNode();
        }

        @Override
        public Optional<PlanNode> apply(ApplyNode applyNode, Captures captures, Context context)
        {
            Assignments subqueryAssignments = applyNode.getSubqueryAssignments().rewrite(x -> rewriter.rewrite(x, context));
            if (applyNode.getSubqueryAssignments().equals(subqueryAssignments)) {
                return Optional.empty();
            }
            return Optional.of(new ApplyNode(applyNode.getId(), applyNode.getInput(), applyNode.getSubquery(), subqueryAssignments, applyNode.getCorrelation()));
        }
    }
}
