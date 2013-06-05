package com.facebook.presto.sql.planner;

import com.facebook.presto.execution.DataSource;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.Split;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SinkNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class DistributedExecutionPlanner
{
    private final SplitManager splitManager;
    private final Session session;
    private final ShardManager shardManager;

    @Inject
    public DistributedExecutionPlanner(SplitManager splitManager,
            Session session,
            ShardManager shardManager)
    {
        this.splitManager = splitManager;
        this.session = session;
        this.shardManager = checkNotNull(shardManager, "databaseShardManager is null");
    }

    public StageExecutionPlan plan(SubPlan root)
    {
        return plan(root, VisitorContext.INITIAL_VISITOR_CONTEXT);
    }

    public StageExecutionPlan plan(SubPlan root, VisitorContext context)
    {
        PlanFragment currentFragment = root.getFragment();

        // get splits for this fragment, this is lazy so split assignments aren't actually calculated here
        Visitor visitor = new Visitor();
        NodeSplits nodeSplits = currentFragment.getRoot().accept(visitor, context);

        // create child stages
        ImmutableList.Builder<StageExecutionPlan> dependencies = ImmutableList.builder();
        for (SubPlan childPlan : root.getChildren()) {
            Expression predicate = visitor.getInheritedPredicatesBySourceFragmentId().get(childPlan.getFragment().getId());
            Preconditions.checkNotNull(predicate, "Expected to find a predicate for fragment %s", childPlan.getFragment().getId());

            StageExecutionPlan dependency = plan(childPlan, new VisitorContext(predicate, context.getPartitionPredicate()));
            dependencies.add(dependency);
        }

        return new StageExecutionPlan(currentFragment,
                nodeSplits.dataSource,
                dependencies.build(),
                visitor.getOutputReceivers());
    }

    public static final class VisitorContext
    {
        private static final VisitorContext INITIAL_VISITOR_CONTEXT = new VisitorContext(BooleanLiteral.TRUE_LITERAL, Predicates.<Partition>alwaysTrue());

        private final Expression inheritedPredicate;
        private final Predicate<Partition> partitionPredicate;

        public VisitorContext(Expression inheritedPredicate, Predicate<Partition> partitionPredicate)
        {
            this.inheritedPredicate = inheritedPredicate;
            this.partitionPredicate = partitionPredicate;
        }

        public Expression getInheritedPredicate()
        {
            return inheritedPredicate;
        }

        public Predicate<Partition> getPartitionPredicate()
        {
            return partitionPredicate;
        }
    }

    private final class Visitor
            extends PlanVisitor<VisitorContext, NodeSplits>
    {
        private final Map<PlanFragmentId, Expression> inheritedPredicatesBySourceFragmentId = new HashMap<>();

        public Map<PlanFragmentId, Expression> getInheritedPredicatesBySourceFragmentId()
        {
            return inheritedPredicatesBySourceFragmentId;
        }

        private final Map<PlanNodeId, OutputReceiver> outputReceivers = new HashMap<>();

        public Map<PlanNodeId, OutputReceiver> getOutputReceivers()
        {
            return ImmutableMap.copyOf(outputReceivers);
        }

        @Override
        public NodeSplits visitTableScan(TableScanNode node, VisitorContext context)
        {
            // get dataSource for table
            DataSource dataSource = splitManager.getSplits(session,
                    node.getTable(),
                    context.getInheritedPredicate(),
                    context.getPartitionPredicate(),
                    node.getAssignments());

            return new NodeSplits(node.getId(), dataSource);
        }

        @Override
        public NodeSplits visitJoin(JoinNode node, VisitorContext context)
        {
            List<Expression> leftConjuncts = new ArrayList<>();
            List<Expression> rightConjuncts = new ArrayList<>();

            for (Expression conjunct : ExpressionUtils.extractConjuncts(context.getInheritedPredicate())) {
                Set<Symbol> symbols = DependencyExtractor.extract(conjunct);

                // is the expression "fully bound" by the either side? If so, it's safe to push it down
                if (node.getLeft().getOutputSymbols().containsAll(symbols)) {
                    leftConjuncts.add(conjunct);
                }
                else if (node.getRight().getOutputSymbols().containsAll(symbols)) {
                    rightConjuncts.add(conjunct);
                }
            }

            // Predicates cannot be naively pushed down through a LEFT join
            // TODO: fix this with proper predicate push down
            Expression leftPredicate = BooleanLiteral.TRUE_LITERAL;
            if (!leftConjuncts.isEmpty() && node.getType() != JoinNode.Type.LEFT) {
                leftPredicate = ExpressionUtils.and(leftConjuncts);
            }

            Expression rightPredicate = BooleanLiteral.TRUE_LITERAL;
            if (!rightConjuncts.isEmpty() && node.getType() != JoinNode.Type.LEFT) {
                rightPredicate = ExpressionUtils.and(rightConjuncts);
            }

            NodeSplits leftSplits = node.getLeft().accept(this, new VisitorContext(leftPredicate, context.getPartitionPredicate()));
            NodeSplits rightSplits = node.getRight().accept(this, new VisitorContext(rightPredicate, context.getPartitionPredicate()));
            if (leftSplits.dataSource.isPresent() && rightSplits.dataSource.isPresent()) {
                throw new IllegalArgumentException("Both left and right join nodes are partitioned"); // TODO: "partitioned" may not be the right term
            }
            if (leftSplits.dataSource.isPresent()) {
                return leftSplits;
            }
            else {
                return rightSplits;
            }
        }

        @Override
        public NodeSplits visitExchange(ExchangeNode node, VisitorContext context)
        {
            for (PlanFragmentId planFragmentId : node.getSourceFragmentIds()) {
                inheritedPredicatesBySourceFragmentId.put(planFragmentId, context.getInheritedPredicate());
            }

            // exchange node does not have splits
            return new NodeSplits(node.getId());
        }

        @Override
        public NodeSplits visitFilter(FilterNode node, VisitorContext context)
        {
            Expression inheritedPredicate = context.getInheritedPredicate();
            Expression predicate = node.getPredicate();
            if (!inheritedPredicate.equals(BooleanLiteral.TRUE_LITERAL)) {
                predicate = new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND, predicate, inheritedPredicate);
            }

            return node.getSource().accept(this, new VisitorContext(predicate, context.getPartitionPredicate()));
        }

        @Override
        public NodeSplits visitAggregation(AggregationNode node, VisitorContext context)
        {
            return node.getSource().accept(this, new VisitorContext(BooleanLiteral.TRUE_LITERAL, context.getPartitionPredicate()));
        }

        @Override
        public NodeSplits visitWindow(WindowNode node, VisitorContext context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public NodeSplits visitProject(ProjectNode node, VisitorContext context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public NodeSplits visitTopN(TopNNode node, VisitorContext context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public NodeSplits visitOutput(OutputNode node, VisitorContext context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public NodeSplits visitLimit(LimitNode node, VisitorContext context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public NodeSplits visitSort(SortNode node, VisitorContext context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public NodeSplits visitSink(SinkNode node, VisitorContext context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public NodeSplits visitTableWriter(final TableWriterNode node, VisitorContext context)
        {
            TableWriter tableWriter = new TableWriter(node, shardManager);

            // get source splits
            NodeSplits nodeSplits = node.getSource().accept(this, new VisitorContext(context.getInheritedPredicate(), tableWriter.getPartitionPredicate()));
            checkState(nodeSplits.dataSource.isPresent(), "No splits present for import");
            DataSource dataSource = nodeSplits.dataSource.get();

            // record output
            outputReceivers.put(node.getId(), tableWriter.getOutputReceiver());

            // wrap splits with table writer info
            Iterable<Split> newSplits = tableWriter.wrapSplits(nodeSplits.planNodeId, dataSource.getSplits());
            return new NodeSplits(node.getId(), new DataSource(dataSource.getDataSourceName(), newSplits));
        }

        @Override
        protected NodeSplits visitPlan(PlanNode node, VisitorContext context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }
    }

    private class NodeSplits
    {
        private final PlanNodeId planNodeId;
        private final Optional<DataSource> dataSource;

        private NodeSplits(PlanNodeId planNodeId)
        {
            this.planNodeId = planNodeId;
            this.dataSource = Optional.absent();
        }

        private NodeSplits(PlanNodeId planNodeId, DataSource dataSource)
        {
            this.planNodeId = planNodeId;
            this.dataSource = Optional.of(dataSource);
        }
    }
}
