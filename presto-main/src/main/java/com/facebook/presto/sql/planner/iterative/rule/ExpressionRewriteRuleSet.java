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

import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.PlanNodePatterns;
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

import static com.facebook.presto.sql.planner.iterative.PlanNodePatterns.aggregation;
import static com.facebook.presto.sql.planner.iterative.PlanNodePatterns.filter;
import static com.facebook.presto.sql.planner.iterative.PlanNodePatterns.join;
import static com.facebook.presto.sql.planner.iterative.PlanNodePatterns.project;
import static com.facebook.presto.sql.planner.iterative.PlanNodePatterns.tablesScan;
import static com.facebook.presto.sql.planner.iterative.PlanNodePatterns.values;

public class ExpressionRewriteRuleSet
        implements RuleSet
{
    public interface ExpressionRewriteRule
    {
        Expression rewrite(Expression expression, Rule.Context context);
    }

    private final ExpressionRewriteRule rewriter;

    private final ImmutableSet<Rule> rules = ImmutableSet.of(
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

    public Set<Rule> rules()
    {
        return rules;
    }

    private final class ProjectExpressionRewrite
            implements Rule
    {
        @Override
        public Pattern getPattern()
        {
            return project();
        }

        @Override
        public Optional<PlanNode> apply(PlanNode node, Context context)
        {
            ProjectNode projectNode = (ProjectNode) node;
            Assignments assignments = projectNode.getAssignments().rewrite(x -> ExpressionRewriteRuleSet.this.rewriter.rewrite(x, context));
            if (projectNode.getAssignments().equals(assignments)) {
                return Optional.empty();
            }
            return Optional.of(new ProjectNode(node.getId(), projectNode.getSource(), assignments));
        }
    }

    private final class AggregationExpressionRewrite
            implements Rule
    {
        @Override
        public Pattern getPattern()
        {
            return aggregation();
        }

        @Override
        public Optional<PlanNode> apply(PlanNode node, Context context)
        {
            AggregationNode aggregationNode = (AggregationNode) node;
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
            implements Rule
    {
        @Override
        public Pattern getPattern()
        {
            return filter();
        }

        @Override
        public Optional<PlanNode> apply(PlanNode node, Context context)
        {
            FilterNode filterNode = (FilterNode) node;
            Expression rewritten = rewriter.rewrite(filterNode.getPredicate(), context);
            if (filterNode.getPredicate().equals(rewritten)) {
                return Optional.empty();
            }
            return Optional.of(new FilterNode(node.getId(), filterNode.getSource(), rewritten));
        }
    }

    private final class TableScanExpressionRewrite
            implements Rule
    {
        @Override
        public Pattern getPattern()
        {
            return tablesScan();
        }

        @Override
        public Optional<PlanNode> apply(PlanNode node, Context context)
        {
            TableScanNode tableScanNode = (TableScanNode) node;
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
            implements Rule
    {
        @Override
        public Pattern getPattern()
        {
            return join();
        }

        @Override
        public Optional<PlanNode> apply(PlanNode node, Context context)
        {
            JoinNode joinNode = (JoinNode) node;
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
            implements Rule
    {
        @Override
        public Pattern getPattern()
        {
            return values();
        }

        @Override
        public Optional<PlanNode> apply(PlanNode node, Context context)
        {
            ValuesNode valuesNode = (ValuesNode) node;
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
                return Optional.of(new ValuesNode(node.getId(), node.getOutputSymbols(), rows.build()));
            }
            return Optional.empty();
        }
    }

    private final class ApplyExpressionRewrite
            implements Rule
    {
        @Override
        public Pattern getPattern()
        {
            return PlanNodePatterns.apply();
        }

        @Override
        public Optional<PlanNode> apply(PlanNode node, Context context)
        {
            ApplyNode applyNode = (ApplyNode) node;
            Assignments subqueryAssignments = applyNode.getSubqueryAssignments().rewrite(x -> rewriter.rewrite(x, context));
            if (applyNode.getSubqueryAssignments().equals(subqueryAssignments)) {
                return Optional.empty();
            }
            return Optional.of(new ApplyNode(applyNode.getId(), applyNode.getInput(), applyNode.getSubquery(), subqueryAssignments, applyNode.getCorrelation()));
        }
    }
}
