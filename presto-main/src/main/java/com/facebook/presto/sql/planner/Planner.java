package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.sql.compiler.AnalysisResult;
import com.facebook.presto.sql.compiler.AnalyzedAggregation;
import com.facebook.presto.sql.compiler.AnalyzedExpression;
import com.facebook.presto.sql.compiler.AnalyzedOrdering;
import com.facebook.presto.sql.compiler.Field;
import com.facebook.presto.sql.compiler.Symbol;
import com.facebook.presto.sql.compiler.SymbolAllocator;
import com.facebook.presto.sql.compiler.TupleDescriptor;
import com.facebook.presto.sql.compiler.Type;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRewriter;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.compiler.AnalyzedAggregation.argumentGetter;
import static com.facebook.presto.sql.compiler.AnalyzedOrdering.expressionGetter;
import static com.google.common.collect.Iterables.concat;

public class Planner
{
    private final static List<PlanOptimizer> OPTIMIZATIONS = ImmutableList.of(
            new PruneUnreferencedOutputs(),
            new UnaliasSymbolReferences(),
            new PruneRedundantProjections(),
            new CoalesceLimits()
    );

    public PlanNode plan(Query query, AnalysisResult analysis)
    {
        PlanNode root = createOutputPlan(query, analysis);

        Map<Symbol, Type> types = analysis.getTypes();

        for (PlanOptimizer optimizer : OPTIMIZATIONS) {
            root = optimizer.optimize(root, types);
        }

        return root;
    }

    private PlanNode createOutputPlan(Query query, AnalysisResult analysis)
    {
        PlanNode result = createQueryPlan(query, analysis);

        int i = 0;
        ImmutableList.Builder<String> names = ImmutableList.builder();
        ImmutableMap.Builder<String, Symbol> assignments = ImmutableMap.builder();
        for (Field field : analysis.getOutputDescriptor().getFields()) {
            String name = field.getAttribute().or("_col" + i);
            names.add(name);
            assignments.put(name, field.getSymbol());
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

        Map<Expression, Symbol> substitutions = new HashMap<>();

        if (!analysis.getAggregations().isEmpty()) {
            root = createAggregatePlan(root,
                    ImmutableList.copyOf(analysis.getOutputExpressions().values()),
                    Lists.transform(analysis.getOrderBy(), expressionGetter()),
                    analysis.getAggregations(),
                    analysis.getGroupByExpressions(),
                    analysis.getSymbolAllocator(),
                    substitutions);
        }

        if (analysis.getLimit() != null && !analysis.getOrderBy().isEmpty()) {
            root = createTopNPlan(root, analysis.getLimit(), analysis.getOrderBy(), analysis.getSymbolAllocator(), substitutions);
        }

        root = createProjectPlan(root, analysis.getOutputExpressions(), substitutions); // project query outputs

        if (analysis.getLimit() != null && analysis.getOrderBy().isEmpty()) {
            root = createLimitPlan(root, analysis.getLimit());
        }

        return root;
    }

    private PlanNode createLimitPlan(PlanNode source, long limit)
    {
        return new LimitNode(source, limit);
    }

    private PlanNode createTopNPlan(PlanNode source, long limit, List<AnalyzedOrdering> orderBy, SymbolAllocator allocator, Map<Expression, Symbol> substitutions)
    {
        /**
         * Turns SELECT $10, $11 ORDER BY expr($0, $1,...), expr($2, $3, ...) LIMIT c into
         *
         * - TopN [c] order by $4, $5
         *     - Project $4 = expr($0, $1, ...), $5 = expr($2, $3, ...), $10, $11
         */

        Map<Symbol, Expression> preProjectAssignments = new HashMap<>();
        for (Symbol symbol : source.getOutputSymbols()) {
            // propagate all output symbols from underlying operator
            QualifiedNameReference expression = new QualifiedNameReference(symbol.toQualifiedName());
            preProjectAssignments.put(symbol, expression);
        }

        List<Symbol> orderBySymbols = new ArrayList<>();
        Map<Symbol,SortItem.Ordering> orderings = new HashMap<>();
        for (AnalyzedOrdering item : orderBy) {
            Expression rewritten = TreeRewriter.rewriteWith(substitution(substitutions), item.getExpression().getRewrittenExpression());

            Symbol symbol = allocator.newSymbol(rewritten, item.getExpression().getType());

            orderBySymbols.add(symbol);
            preProjectAssignments.put(symbol, rewritten);
            orderings.put(symbol, item.getOrdering());
        }

        ProjectNode preProject = new ProjectNode(source, preProjectAssignments);
        return new TopNNode(preProject, limit, orderBySymbols, orderings);
    }

    private PlanNode createAggregatePlan(PlanNode source,
            List<AnalyzedExpression> outputs,
            List<AnalyzedExpression> orderBy,
            Set<AnalyzedAggregation> aggregations,
            List<AnalyzedExpression> groupBys,
            SymbolAllocator allocator,
            Map<Expression, Symbol> outputSubstitutions)
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

        BiMap<Symbol, Expression> scalarAssignments = HashBiMap.create();
        for (AnalyzedExpression expression : scalarExpressions) {
            Symbol symbol = allocator.newSymbol(expression.getRewrittenExpression(), expression.getType());
            scalarAssignments.put(symbol, expression.getRewrittenExpression());
        }

        PlanNode preProjectNode = source;
        if (!scalarAssignments.isEmpty()) { // workaround to deal with COUNT's lack of inputs
            preProjectNode = new ProjectNode(source, scalarAssignments);
        }

        // 2. Aggregate
        Map<Expression, Symbol> substitutions = new HashMap<>();

        BiMap<Symbol, FunctionCall> aggregationAssignments = HashBiMap.create();
        Map<Symbol, FunctionInfo> functionInfos = new HashMap<>();
        for (AnalyzedAggregation aggregation : aggregations) {
            // rewrite function calls in terms of scalar inputs
            FunctionCall rewrittenFunction = TreeRewriter.rewriteWith(substitution(scalarAssignments.inverse()), aggregation.getRewrittenCall());
            Symbol symbol = allocator.newSymbol(aggregation.getFunctionName().getSuffix(), aggregation.getType());
            aggregationAssignments.put(symbol, rewrittenFunction);
            functionInfos.put(symbol, aggregation.getFunctionInfo());

            // build substitution map to rewrite assignments in post-project
            substitutions.put(aggregation.getRewrittenCall(), symbol);
        }

        List<Symbol> groupBySymbols = new ArrayList<>();
        for (AnalyzedExpression groupBy : groupBys) {
            Expression rewritten = TreeRewriter.rewriteWith(substitution(scalarAssignments.inverse()), groupBy.getRewrittenExpression());
            Symbol symbol = allocator.newSymbol(rewritten, groupBy.getType());
            groupBySymbols.add(symbol);

            substitutions.put(groupBy.getRewrittenExpression(), symbol);
        }

        PlanNode aggregationNode = new AggregationNode(preProjectNode, groupBySymbols, aggregationAssignments, functionInfos);

        // 3. Post-project scalar expressions based on aggregations
        BiMap<Symbol, Expression> postProjectScalarAssignments = HashBiMap.create();
        for (AnalyzedExpression expression : ImmutableSet.copyOf(concat(outputs, groupBys, orderBy))) {
            Expression rewritten = TreeRewriter.rewriteWith(substitution(substitutions), expression.getRewrittenExpression());
            Symbol symbol = allocator.newSymbol(rewritten, expression.getType());

            postProjectScalarAssignments.put(symbol, rewritten);

            // build substitution map to return to caller
            outputSubstitutions.put(expression.getRewrittenExpression(), symbol);
        }

        return new ProjectNode(aggregationNode, postProjectScalarAssignments);
    }

    private PlanNode createProjectPlan(PlanNode root, Map<Symbol, AnalyzedExpression> outputAnalysis, final Map<Expression, Symbol> substitutions)
    {
        Map<Symbol, Expression> outputs = Maps.transformValues(outputAnalysis, new Function<AnalyzedExpression, Expression>()
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
        return new FilterNode(source, predicate.getRewrittenExpression(), source.getOutputSymbols());
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

        ImmutableMap.Builder<String, Symbol> attributes = ImmutableMap.builder();

        List<Field> fields = descriptor.getFields();
        for (Field field : fields) {
            Symbol symbol = field.getSymbol();
            String attribute = field.getAttribute().get();
            attributes.put(attribute, symbol);
        }

        QualifiedName tableFullName = Iterables.getFirst(fields, null).getPrefix().get();

        String catalogName = tableFullName.getParts().get(0);
        String schemaName = tableFullName.getParts().get(1);
        String tableName = tableFullName.getParts().get(2);

        return new TableScan(catalogName, schemaName, tableName, attributes.build());
    }

    private NodeRewriter<Void> substitution(final Map<Expression, Symbol> substitutions)
    {
        return new NodeRewriter<Void>()
        {
            @Override
            public Node rewriteExpression(Expression node, Void context, TreeRewriter<Void> treeRewriter)
            {
                Symbol symbol = substitutions.get(node);
                if (symbol != null) {
                    return new QualifiedNameReference(symbol.toQualifiedName());
                }

                return treeRewriter.defaultRewrite(node, context);
            }
        };
    }
}
