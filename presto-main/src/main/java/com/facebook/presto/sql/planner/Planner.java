package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.sql.compiler.AnalysisResult;
import com.facebook.presto.sql.compiler.AnalyzedAggregation;
import com.facebook.presto.sql.compiler.AnalyzedExpression;
import com.facebook.presto.sql.compiler.NamedSlot;
import com.facebook.presto.sql.compiler.Slot;
import com.facebook.presto.sql.compiler.SlotAllocator;
import com.facebook.presto.sql.compiler.SlotReference;
import com.facebook.presto.sql.compiler.TupleDescriptor;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRewriter;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Subquery;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Planner
{
    private final static List<PlanOptimizer> OPTIMIZATIONS = ImmutableList.of(
            new PruneUnreferencedOutputs(),
            new UnaliasSlotReferences(),
            new PruneRedundantProjections()
    );

    public PlanNode plan(Query query, AnalysisResult analysis)
    {
        PlanNode root = createOutputPlan(query, analysis);

        for (PlanOptimizer optimizer : OPTIMIZATIONS) {
            root = optimizer.optimize(root);
        }

        return root;
    }

    private PlanNode createOutputPlan(Query query, AnalysisResult analysis)
    {
        PlanNode result = createQueryPlan(query, analysis);

        ImmutableList.Builder<String> names = ImmutableList.builder();
        for (Optional<QualifiedName> name : analysis.getOutputDescriptor().getNames()) {
            names.add(name.or(QualifiedName.of("col")).getSuffix());
        }

        return new OutputPlan(result, names.build());
    }

    private PlanNode createQueryPlan(Query query, AnalysisResult analysis)
    {
        PlanNode root = createRelationPlan(query.getFrom(), analysis);

        if (analysis.getPredicate() != null) {
            root = createFilterPlan(root, analysis.getPredicate());
        }

        if (!analysis.getAggregations().isEmpty()) {
            root = createAggregatePlan(root, analysis.getOutputExpressions(), analysis.getAggregations(), analysis.getGroupByExpressions(), analysis.getSlotAllocator());
        }
        else {
            root = createProjectPlan(root, analysis.getOutputExpressions());
        }

        return root;
    }

    private PlanNode createAggregatePlan(PlanNode source,
            Map<Slot, AnalyzedExpression> outputs,
            List<AnalyzedAggregation> aggregations,
            List<AnalyzedExpression> groupBys,
            SlotAllocator allocator)
    {
        /**
         * Turns SELECT k1, k2, sum(v1 * v2) - sum(v3 * v4) GROUP BY k1, k2 into
         *
         * 3. Project $0, $1, $7 = $5 + $6
         *   2. Aggregate by ($0, $1): $5 = sum($3), $6 = sum($4)
         *     1. Project $0 = k1, $1 = k2, $3 = v1 * v2, $4 = v3 * v4
         */
        final Map<Expression, Slot> outputRewriteMap = new HashMap<>();

        // 1. pre-project group by expressions
        Map<Slot, Expression> slotsForGroupBy = new HashMap<>();
        for (AnalyzedExpression groupBy : groupBys) {
            Slot slot = allocator.newSlot(groupBy);
            slotsForGroupBy.put(slot, groupBy.getRewrittenExpression());
            outputRewriteMap.put(groupBy.getRewrittenExpression(), slot);
        }

        Map<Slot, FunctionCall> functionCalls = new HashMap<>();
        Map<Slot, FunctionInfo> functionInfos = new HashMap<>();
        Map<Slot, Expression> slotsForAggregationArgs = new HashMap<>();
        for (AnalyzedAggregation aggregation : aggregations) {
            List<Expression> argumentExpressions = new ArrayList<>();
            for (AnalyzedExpression argument : aggregation.getArguments()) {
                Slot argumentSlot = allocator.newSlot(argument);
                slotsForAggregationArgs.put(argumentSlot, argument.getRewrittenExpression());
                argumentExpressions.add(new SlotReference(argumentSlot));
            }

            Slot slot = allocator.newSlot(aggregation.getType());
            functionCalls.put(slot, new FunctionCall(aggregation.getFunctionName(), argumentExpressions));
            functionInfos.put(slot, aggregation.getFunctionInfo());
            outputRewriteMap.put(aggregation.getRewrittenCall(), slot);
        }

        Map<Slot, Expression> preProjectOutputs = new HashMap<>();
        preProjectOutputs.putAll(slotsForGroupBy);
        preProjectOutputs.putAll(slotsForAggregationArgs);

        PlanNode preProjectNode = source;
        if (!preProjectOutputs.isEmpty()) { // workaround to deal with COUNT's lack of inputs
            preProjectNode = new ProjectNode(source, preProjectOutputs);
        }

        // 2. aggregation node in terms of pre-project outputs
        PlanNode aggregationNode = new AggregationNode(preProjectNode, ImmutableList.copyOf(slotsForGroupBy.keySet()), functionCalls, functionInfos);

        // 3. post project to compute scalar expressions on aggregation results
        ImmutableMap.Builder<Slot, Expression> outputBuilder = ImmutableMap.builder();
        for (Map.Entry<Slot, AnalyzedExpression> entry : outputs.entrySet()) {
            Expression rewrittenOutput = TreeRewriter.rewriteWith(new NodeRewriter<Void>()
            {
                @Override
                public Node rewriteExpression(Expression node, Void context, TreeRewriter<Void> treeRewriter)
                {
                    Slot slot = outputRewriteMap.get(node);
                    if (slot != null) {
                        return new SlotReference(slot);
                    }

                    return treeRewriter.defaultRewrite(node, context);
                }
            }, entry.getValue().getRewrittenExpression());

            outputBuilder.put(entry.getKey(), rewrittenOutput);
        }

        return new ProjectNode(aggregationNode, outputBuilder.build());
    }

    private PlanNode createProjectPlan(PlanNode root, Map<Slot, AnalyzedExpression> outputAnalysis)
    {
        Map<Slot, Expression> outputs = Maps.transformValues(outputAnalysis, new Function<AnalyzedExpression, Expression>()
        {
            @Override
            public Expression apply(AnalyzedExpression input)
            {
                return input.getRewrittenExpression();
            }
        });

        return new ProjectNode(root, outputs);
    }

    private FilterNode createFilterPlan(PlanNode source, AnalyzedExpression predicate)
    {
        return new FilterNode(source, predicate.getRewrittenExpression(), source.getOutputs());
    }

    private PlanNode createRelationPlan(List<Relation> relations, AnalysisResult analysis)
    {
        Relation relation = Iterables.getOnlyElement(relations); // TODO: add join support

        if (relation instanceof Table) {
            return createScanNode((Table) relation, analysis);
        }
        else if (relation instanceof AliasedRelation) {
            return createRelationPlan(ImmutableList.of(((AliasedRelation) relation).getRelation()), analysis);
        }
        else if (relation instanceof Subquery) {
            return createQueryPlan(((Subquery) relation).getQuery(), analysis.getAnalysis((Subquery) relation));
        }

        throw new UnsupportedOperationException("not yet implemented");
    }

    private PlanNode createScanNode(Table table, AnalysisResult analysis)
    {
        TupleDescriptor descriptor = analysis.getTableDescriptor(table);

        ImmutableList.Builder<Slot> outputs = ImmutableList.builder();
        ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();

        List<NamedSlot> slots = descriptor.getSlots();

        for (NamedSlot namedSlot : slots) {
            Slot slot = namedSlot.getSlot();
            QualifiedName name = namedSlot.getName().get();

            ColumnScan scan = new ColumnScan(name.getParts().get(0), name.getParts().get(1), name.getParts().get(2), name.getParts().get(3), slot);
            sources.add(scan);
            outputs.add(slot);
        }

        return new AlignNode(sources.build(), outputs.build());
    }
}
