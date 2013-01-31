package com.facebook.presto.sql.planner;

import com.facebook.presto.split.SplitAssignments;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SinkNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DistributedExecutionPlanner
{
    private final SplitManager splitManager;
    private final Session session;

    @Inject
    public DistributedExecutionPlanner(SplitManager splitManager, Session session)
    {
        this.splitManager = splitManager;
        this.session = session;
    }

    public StageExecutionPlan plan(SubPlan root)
    {
        return plan(root, BooleanLiteral.TRUE_LITERAL);
    }

    public StageExecutionPlan plan(SubPlan root, Expression inheritedPredicate)
    {
        PlanFragment currentFragment = root.getFragment();

        // get splits for this fragment, this is lazy so split assignments aren't actually calculated here
        Visitor visitor = new Visitor();
        Optional<Iterable<SplitAssignments>> splits = currentFragment.getRoot().accept(visitor, inheritedPredicate);

        // create child stages
        ImmutableList.Builder<StageExecutionPlan> dependencies = ImmutableList.builder();
        for (SubPlan childPlan : root.getChildren()) {
            Expression predicate = visitor.getInheritedPredicatesBySourceFragmentId().get(childPlan.getFragment().getId());
            Preconditions.checkNotNull(predicate, "Expected to find a predicate for fragment %s", childPlan.getFragment().getId());

            StageExecutionPlan dependency = plan(childPlan, predicate);
            dependencies.add(dependency);
        }

        return new StageExecutionPlan(currentFragment, splits, dependencies.build());
    }

    private final class Visitor
            extends PlanVisitor<Expression, Optional<Iterable<SplitAssignments>>>
    {
        private final Map<PlanFragmentId, Expression> inheritedPredicatesBySourceFragmentId = new HashMap<>();

        public Map<PlanFragmentId, Expression> getInheritedPredicatesBySourceFragmentId()
        {
            return inheritedPredicatesBySourceFragmentId;
        }

        @Override
        public Optional<Iterable<SplitAssignments>> visitTableScan(TableScanNode node, Expression inheritedPredicate)
        {
            // get splits for table
            Iterable<SplitAssignments> splitAssignments = splitManager.getSplitAssignments(session, node.getTable(), inheritedPredicate, node.getAssignments());
            return Optional.of(splitAssignments);
        }

        @Override
        public Optional<Iterable<SplitAssignments>> visitJoin(JoinNode node, Expression inheritedPredicate)
        {
            List<Expression> leftConjuncts = new ArrayList<>();
            List<Expression> rightConjuncts = new ArrayList<>();

            for (Expression conjunct : ExpressionUtils.extractConjuncts(inheritedPredicate)) {
                Set<Symbol> symbols = DependencyExtractor.extract(conjunct);

                // is the expression "fully bound" by the either side? If so, it's safe to push it down
                if (node.getLeft().getOutputSymbols().containsAll(symbols)) {
                    leftConjuncts.add(conjunct);
                }
                else if (node.getRight().getOutputSymbols().containsAll(symbols)) {
                    rightConjuncts.add(conjunct);
                }
            }

            Expression leftPredicate = BooleanLiteral.TRUE_LITERAL;
            if (!leftConjuncts.isEmpty()) {
                leftPredicate = ExpressionUtils.and(leftConjuncts);
            }

            Expression rightPredicate = BooleanLiteral.TRUE_LITERAL;
            if (!rightConjuncts.isEmpty()) {
                rightPredicate = ExpressionUtils.and(rightConjuncts);
            }

            Optional<Iterable<SplitAssignments>> leftSplits = node.getLeft().accept(this, leftPredicate);
            Optional<Iterable<SplitAssignments>> rightSplits = node.getRight().accept(this, rightPredicate);
            if (leftSplits.isPresent() && rightSplits.isPresent()) {
                throw new IllegalArgumentException("Both left and right join nodes are partitioned"); // TODO: "partitioned" may not be the right term
            }
            if (leftSplits.isPresent()) {
                return leftSplits;
            }
            else {
                return rightSplits;
            }
        }

        @Override
        public Optional<Iterable<SplitAssignments>> visitExchange(ExchangeNode node, Expression inheritedPredicate)
        {
            inheritedPredicatesBySourceFragmentId.put(node.getSourceFragmentId(), inheritedPredicate);

            // exchange node does not have splits
            return Optional.absent();
        }

        @Override
        public Optional<Iterable<SplitAssignments>> visitFilter(FilterNode node, Expression inheritedPredicate)
        {
            Expression predicate = node.getPredicate();
            if (!inheritedPredicate.equals(BooleanLiteral.TRUE_LITERAL)) {
                predicate = new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND, predicate, inheritedPredicate);
            }

            return node.getSource().accept(this, predicate);
        }

        @Override
        public Optional<Iterable<SplitAssignments>> visitAggregation(AggregationNode node, Expression inheritedPredicate)
        {
            return node.getSource().accept(this, BooleanLiteral.TRUE_LITERAL);
        }

        @Override
        public Optional<Iterable<SplitAssignments>> visitWindow(WindowNode node, Expression inheritedPredicate)
        {
            return node.getSource().accept(this, inheritedPredicate);
        }

        @Override
        public Optional<Iterable<SplitAssignments>> visitProject(ProjectNode node, Expression inheritedPredicate)
        {
            return node.getSource().accept(this, inheritedPredicate);
        }

        @Override
        public Optional<Iterable<SplitAssignments>> visitTopN(TopNNode node, Expression inheritedPredicate)
        {
            return node.getSource().accept(this, inheritedPredicate);
        }

        @Override
        public Optional<Iterable<SplitAssignments>> visitOutput(OutputNode node, Expression inheritedPredicate)
        {
            return node.getSource().accept(this, inheritedPredicate);
        }

        @Override
        public Optional<Iterable<SplitAssignments>> visitLimit(LimitNode node, Expression inheritedPredicate)
        {
            return node.getSource().accept(this, inheritedPredicate);
        }

        @Override
        public Optional<Iterable<SplitAssignments>> visitSort(SortNode node, Expression inheritedPredicate)
        {
            return node.getSource().accept(this, inheritedPredicate);
        }

        @Override
        public Optional<Iterable<SplitAssignments>> visitSink(SinkNode node, Expression context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        protected Optional<Iterable<SplitAssignments>> visitPlan(PlanNode node, Expression inheritedPredicate)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }
    }
}
