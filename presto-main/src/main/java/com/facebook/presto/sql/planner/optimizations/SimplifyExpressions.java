package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.SymbolResolver;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;

public class SimplifyExpressions
        extends PlanOptimizer
{
    private final Metadata metadata;
    private final Session session;

    public SimplifyExpressions(Metadata metadata, Session session)
    {
        this.metadata = metadata;
        this.session = session;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Map<Symbol, Type> types)
    {
        ExpressionInterpreter interpreter = ExpressionInterpreter.expressionOptimizer(new SymbolResolver()
        {
            @Override
            public Object getValue(Symbol symbol)
            {
                return new QualifiedNameReference(symbol.toQualifiedName());
            }
        }, metadata, session);

        return plan.accept(new Visitor(interpreter), null);
    }

    private static class Visitor
            extends PlanVisitor<Void, PlanNode>
    {
        private final ExpressionInterpreter interpreter;

        private Visitor(ExpressionInterpreter interpreter)
        {
            this.interpreter = interpreter;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);
            Map<Symbol, Expression> assignments = ImmutableMap.copyOf(Maps.transformValues(node.getOutputMap(), simplifyExpressionFunction()));
            return new ProjectNode(source, assignments);
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);
            return new AggregationNode(source, node.getGroupBy(), node.getAggregations(), node.getFunctions());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, Void context)
        {
            return node;
        }

        @Override
        public PlanNode visitFilter(FilterNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);
            return new FilterNode(source, simplifyExpression(node.getPredicate()), node.getOutputSymbols());
        }

        @Override
        public PlanNode visitOutput(OutputNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);
            return new OutputNode(source, node.getColumnNames(), node.getAssignments());
        }

        @Override
        public PlanNode visitLimit(LimitNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);
            return new LimitNode(source, node.getCount());
        }

        @Override
        public PlanNode visitTopN(TopNNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);
            return new TopNNode(source, node.getCount(), node.getOrderBy(), node.getOrderings());
        }

        @Override
        public PlanNode visitSort(SortNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);
            return new SortNode(source, node.getOrderBy(), node.getOrderings());
        }

        @Override
        public PlanNode visitJoin(JoinNode node, Void context)
        {
            PlanNode left = node.getLeft().accept(this, context);
            PlanNode right = node.getRight().accept(this, context);

            return new JoinNode(left, right, node.getCriteria());
        }

        @Override
        protected PlanNode visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + getClass().getName());
        }

        private Function<Expression, Expression> simplifyExpressionFunction()
        {
            return new Function<Expression, Expression>()
            {
                @Override
                public Expression apply(Expression input)
                {
                    return simplifyExpression(input);
                }
            };
        }

        private Expression simplifyExpression(Expression input)
        {
            return ExpressionInterpreter.toExpression(interpreter.process(input, null));
        }
    }

}
