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
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Subquery;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.compiler.AnalyzedAggregation.argumentGetter;
import static com.google.common.collect.Iterables.concat;

public class Planner
{
    private final static List<PlanOptimizer> OPTIMIZATIONS = ImmutableList.of(
            new PruneUnreferencedOutputs(),
            new UnaliasSlotReferences(),
            new PruneRedundantProjections(),
            new CoalesceLimits()
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

        int i = 0;
        ImmutableList.Builder<String> names = ImmutableList.builder();
        ImmutableMap.Builder<String, Slot> assignments = ImmutableMap.builder();
        for (NamedSlot namedSlot : analysis.getOutputDescriptor().getSlots()) {
            String name = namedSlot.getAttribute().or("_col" + i);
            names.add(name);
            assignments.put(name, namedSlot.getSlot());
            i++;
        }

        return new OutputPlan(result, names.build(), assignments.build());
    }

    private PlanNode createQueryPlan(Query query, AnalysisResult analysis)
    {
        PlanNode root = createRelationPlan(query.getFrom(), analysis);

        if (analysis.getPredicate() != null) {
            root = createFilterPlan(root, analysis.getPredicate());
        }

        Map<Expression, Slot> substitutions = new HashMap<>();

        if (!analysis.getAggregations().isEmpty()) {
            root = createAggregatePlan(root,
                    ImmutableList.copyOf(analysis.getOutputExpressions().values()),
                    analysis.getAggregations(),
                    analysis.getGroupByExpressions(),
                    analysis.getSlotAllocator(),
                    substitutions);
        }

        root = createProjectPlan(root, analysis.getOutputExpressions(), substitutions); // project query outputs

        if (analysis.getLimit() != null) {
            root = createLimitPlan(root, analysis.getLimit());
        }

        return root;
    }

    private PlanNode createLimitPlan(PlanNode source, long limit)
    {
        return new LimitNode(source, limit);
    }

    private PlanNode createAggregatePlan(PlanNode source,
            List<AnalyzedExpression> outputs,
            Set<AnalyzedAggregation> aggregations,
            List<AnalyzedExpression> groupBys,
            SlotAllocator allocator,
            Map<Expression, Slot> outputSubstitutions)
    {
        /**
         * Turns SELECT k1 + 1, sum(v1 * v2) - sum(v3 * v4) GROUP BY k1 + 1, k2 into
         *
         * 3. Project $0, $1, $7 = $5 - $6
         *   2. Aggregate by ($0, $1): $5 = sum($3), $6 = sum($4)
         *     1. Project $0 = k1 + 1, $1 = k2, $3 = v1 * v2, $4 = v3 * v4
         */

        // 1. Pre-project all scalar inputs
        Set<AnalyzedExpression> scalarExpressions = ImmutableSet.copyOf(concat(
                IterableTransformer.on(aggregations)
                        .transformAndFlatten(argumentGetter())
                        .list(),
                groupBys));

        BiMap<Slot, Expression> scalarAssignments = HashBiMap.create();
        for (AnalyzedExpression expression : scalarExpressions) {
            Slot slot = allocator.newSlot(expression);
            scalarAssignments.put(slot, expression.getRewrittenExpression());
        }

        PlanNode preProjectNode = source;
        if (!scalarAssignments.isEmpty()) { // workaround to deal with COUNT's lack of inputs
            preProjectNode = new ProjectNode(source, scalarAssignments);
        }

        // 2. Aggregate
        Map<Expression, Slot> substitutions = new HashMap<>();

        BiMap<Slot, FunctionCall> aggregationAssignments = HashBiMap.create();
        Map<Slot, FunctionInfo> functionInfos = new HashMap<>();
        for (AnalyzedAggregation aggregation : aggregations) {
            // rewrite function calls in terms of scalar inputs
            FunctionCall rewrittenFunction = TreeRewriter.rewriteWith(substitution(scalarAssignments.inverse()), aggregation.getRewrittenCall());
            Slot slot = allocator.newSlot(aggregation.getType());
            aggregationAssignments.put(slot, rewrittenFunction);
            functionInfos.put(slot, aggregation.getFunctionInfo());

            // build substitution map to rewrite assignments in post-project
            substitutions.put(aggregation.getRewrittenCall(), slot);
        }

        List<Slot> groupBySlots = new ArrayList<>();
        for (AnalyzedExpression groupBy : groupBys) {
            Expression rewritten = TreeRewriter.rewriteWith(substitution(scalarAssignments.inverse()), groupBy.getRewrittenExpression());
            Slot slot = allocator.newSlot(groupBy.getType(), rewritten);
            groupBySlots.add(slot);

            substitutions.put(groupBy.getRewrittenExpression(), slot);
        }

        PlanNode aggregationNode = new AggregationNode(preProjectNode, groupBySlots, aggregationAssignments, functionInfos);

        // 3. Post-project scalar expressions based on aggregations
        BiMap<Slot, Expression> postProjectScalarAssignments = HashBiMap.create();
        for (AnalyzedExpression expression : ImmutableSet.copyOf(concat(outputs, groupBys))) {
            Expression rewritten = TreeRewriter.rewriteWith(substitution(substitutions), expression.getRewrittenExpression());
            Slot slot = allocator.newSlot(expression.getType(), rewritten);

            postProjectScalarAssignments.put(slot, rewritten);

            // build substitution map to return to caller
            outputSubstitutions.put(expression.getRewrittenExpression(), slot);
        }

        return new ProjectNode(aggregationNode, postProjectScalarAssignments);
    }

    private PlanNode createProjectPlan(PlanNode root, Map<Slot, AnalyzedExpression> outputAnalysis, final Map<Expression, Slot> substitutions)
    {
        Map<Slot, Expression> outputs = Maps.transformValues(outputAnalysis, new Function<AnalyzedExpression, Expression>()
        {
            @Override
            public Expression apply(AnalyzedExpression input)
            {
                return TreeRewriter.rewriteWith(substitution(substitutions), input.getRewrittenExpression());
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
            QualifiedName tableName = namedSlot.getPrefix().get();
            String attribute = namedSlot.getAttribute().get();

            ColumnScan scan = new ColumnScan(tableName.getParts().get(0), tableName.getParts().get(1), tableName.getParts().get(2), attribute, slot);
            sources.add(scan);
            outputs.add(slot);
        }

        return new AlignNode(sources.build(), outputs.build());
    }

    private NodeRewriter<Void> substitution(final Map<Expression, Slot> substitutions)
    {
        return new NodeRewriter<Void>()
        {
            @Override
            public Node rewriteExpression(Expression node, Void context, TreeRewriter<Void> treeRewriter)
            {
                Slot slot = substitutions.get(node);
                if (slot != null) {
                    return new SlotReference(slot);
                }

                return treeRewriter.defaultRewrite(node, context);
            }
        };
    }
}
