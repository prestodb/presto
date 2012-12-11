package com.facebook.presto.sql.planner;

import com.facebook.presto.execution.QueryState;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.split.Split;
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
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SinkNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.Iterables.transform;

public class DistributedExecutionPlanner
{
    private final NodeManager nodeManager;
    private final SplitManager splitManager;
    private final AtomicReference<QueryState> queryState;
    private final Session session;
    private final Random random = new Random();

    @Inject
    public DistributedExecutionPlanner(NodeManager nodeManager, SplitManager splitManager, AtomicReference<QueryState> queryState, Session session)
    {
        this.nodeManager = nodeManager;
        this.splitManager = splitManager;
        this.queryState = queryState;
        this.session = session;
    }

    public StageExecutionPlan plan(SubPlan root)
    {
        return plan(root, BooleanLiteral.TRUE_LITERAL);
    }

    public StageExecutionPlan plan(SubPlan root, Expression inheritedPredicate)
    {
        PlanFragment currentFragment = root.getFragment();

        // get partitions for this fragment

        Visitor visitor = new Visitor();

        List<Partition> partitions = currentFragment.getRoot().accept(visitor, inheritedPredicate);
        if (!currentFragment.isPartitioned()) {
            // create a single partition on a random node for this fragment
            ArrayList<Node> nodes = new ArrayList<>(nodeManager.getActiveNodes());
            Preconditions.checkState(!nodes.isEmpty(), "Cluster does not have any active nodes");
            Collections.shuffle(nodes, random);
            Node node = nodes.get(0);
            partitions = ImmutableList.of(new Partition(node, ImmutableList.<PlanFragmentSource>of()));
        }

        // create child stages
        ImmutableList.Builder<StageExecutionPlan> dependencies = ImmutableList.builder();
        for (SubPlan childPlan : root.getChildren()) {
            Expression predicate = visitor.getInheritedPredicatesBySourceFragmentId().get(childPlan.getFragment().getId());
            Preconditions.checkNotNull(predicate, "Expected to find a predicate for fragment %s", childPlan.getFragment().getId());

            StageExecutionPlan dependency = plan(childPlan, predicate);
            dependencies.add(dependency);
        }

        return new StageExecutionPlan(currentFragment, partitions, dependencies.build());
    }


    private final class Visitor
        extends PlanVisitor<Expression, List<Partition>>
    {
        private final Map<Integer, Expression> inheritedPredicatesBySourceFragmentId = new HashMap<>();

        public Map<Integer, Expression> getInheritedPredicatesBySourceFragmentId()
        {
            return inheritedPredicatesBySourceFragmentId;
        }

        @Override
        public List<Partition> visitTableScan(TableScanNode node, Expression inheritedPredicate)
        {
            // get splits for table
            Iterable<SplitAssignments> splitAssignments = splitManager.getSplitAssignments(session, node.getTable(), inheritedPredicate, node.getAssignments());

            // divide splits amongst the nodes
            Multimap<Node, Split> nodeSplits = SplitAssignments.balancedNodeAssignment(queryState, splitAssignments);

            // create a partition for each node
            ImmutableList.Builder<Partition> partitions = ImmutableList.builder();
            for (Entry<Node, Collection<Split>> entry : nodeSplits.asMap().entrySet()) {
                List<PlanFragmentSource> sources = ImmutableList.copyOf(transform(entry.getValue(), new Function<Split, PlanFragmentSource>()
                {
                    @Override
                    public PlanFragmentSource apply(Split split)
                    {
                        return new TableScanPlanFragmentSource(split);
                    }
                }));
                partitions.add(new Partition(entry.getKey(), sources));
            }
            return partitions.build();
        }

        @Override
        public List<Partition> visitJoin(JoinNode node, Expression inheritedPredicate)
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

            List<Partition> leftPartitions = node.getLeft().accept(this, leftPredicate);
            List<Partition> rightPartitions = node.getRight().accept(this, rightPredicate);
            if (!leftPartitions.isEmpty() && !rightPartitions.isEmpty()) {
                throw new IllegalArgumentException("Both left and right join nodes are partitioned"); // TODO: "partitioned" may not be the right term
            }
            if (!leftPartitions.isEmpty()) {
                return leftPartitions;
            }
            else {
                return rightPartitions;
            }
        }

        @Override
        public List<Partition> visitExchange(ExchangeNode node, Expression inheritedPredicate)
        {
            inheritedPredicatesBySourceFragmentId.put(node.getSourceFragmentId(), inheritedPredicate);

            // exchange node is unpartitioned
            return ImmutableList.of();
        }

        @Override
        public List<Partition> visitFilter(FilterNode node, Expression inheritedPredicate)
        {
            Expression predicate = node.getPredicate();
            if (!inheritedPredicate.equals(BooleanLiteral.TRUE_LITERAL)) {
                predicate = new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND, predicate, inheritedPredicate);
            }

            return node.getSource().accept(this, predicate);
        }

        @Override
        public List<Partition> visitAggregation(AggregationNode node, Expression inheritedPredicate)
        {
            return node.getSource().accept(this, BooleanLiteral.TRUE_LITERAL);
        }

        @Override
        public List<Partition> visitProject(ProjectNode node, Expression inheritedPredicate)
        {
            return node.getSource().accept(this, inheritedPredicate);
        }

        @Override
        public List<Partition> visitTopN(TopNNode node, Expression inheritedPredicate)
        {
            return node.getSource().accept(this, inheritedPredicate);
        }

        @Override
        public List<Partition> visitOutput(OutputNode node, Expression inheritedPredicate)
        {
            return node.getSource().accept(this, inheritedPredicate);
        }

        @Override
        public List<Partition> visitLimit(LimitNode node, Expression inheritedPredicate)
        {
            return node.getSource().accept(this, inheritedPredicate);
        }

        @Override
        public List<Partition> visitSort(SortNode node, Expression inheritedPredicate)
        {
            return node.getSource().accept(this, inheritedPredicate);
        }

        @Override
        public List<Partition> visitSink(SinkNode node, Expression context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        protected List<Partition> visitPlan(PlanNode node, Expression inheritedPredicate)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }
    }
}
