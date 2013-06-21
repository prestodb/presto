package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.FunctionHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.EquiJoinClause;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.FieldOrExpression;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.Union;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.analyzer.EquiJoinClause.leftGetter;
import static com.facebook.presto.sql.analyzer.EquiJoinClause.rightGetter;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.google.common.base.Preconditions.checkArgument;

class RelationPlanner
        extends DefaultTraversalVisitor<RelationPlan, Void>
{
    private final Analysis analysis;
    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Metadata metadata;
    private final Session session;

    RelationPlanner(Analysis analysis, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, Metadata metadata, Session session)
    {
        Preconditions.checkNotNull(analysis, "analysis is null");
        Preconditions.checkNotNull(symbolAllocator, "symbolAllocator is null");
        Preconditions.checkNotNull(idAllocator, "idAllocator is null");
        Preconditions.checkNotNull(metadata, "metadata is null");
        Preconditions.checkNotNull(session, "session is null");

        this.analysis = analysis;
        this.symbolAllocator = symbolAllocator;
        this.idAllocator = idAllocator;
        this.metadata = metadata;
        this.session = session;
    }

    @Override
    protected RelationPlan visitTable(Table node, Void context)
    {
        if (!node.getName().getPrefix().isPresent()) {
            Query namedQuery = analysis.getNamedQuery(node);
            if (namedQuery != null) {
                RelationPlan subPlan = process(namedQuery, null);
                return new RelationPlan(subPlan.getRoot(), analysis.getOutputDescriptor(node), subPlan.getOutputSymbols());
            }
        }

        TupleDescriptor descriptor = analysis.getOutputDescriptor(node);
        TableHandle handle = analysis.getTableHandle(node);

        ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, ColumnHandle> columns = ImmutableMap.builder();
        for (int i = 0; i < descriptor.getFields().size(); i++) {
            Field field = descriptor.getFields().get(i);
            Symbol symbol = symbolAllocator.newSymbol(field.getName().get(), field.getType());

            outputSymbolsBuilder.add(symbol);
            columns.put(symbol, analysis.getColumn(field));
        }

        ImmutableList<Symbol> outputSymbols = outputSymbolsBuilder.build();
        return new RelationPlan(new TableScanNode(idAllocator.getNextId(), handle, outputSymbols, columns.build(), TRUE_LITERAL, TRUE_LITERAL), descriptor, outputSymbols);
    }

    @Override
    protected RelationPlan visitAliasedRelation(AliasedRelation node, Void context)
    {
        RelationPlan subPlan = process(node.getRelation(), context);

        TupleDescriptor outputDescriptor = analysis.getOutputDescriptor(node);

        return new RelationPlan(subPlan.getRoot(), outputDescriptor, subPlan.getOutputSymbols());
    }

    @Override
    protected RelationPlan visitJoin(Join node, Void context)
    {
        // TODO: translate the RIGHT join into a mirrored LEFT join when we refactor (@martint)
        RelationPlan leftPlan = process(node.getLeft(), context);
        RelationPlan rightPlan = process(node.getRight(), context);

        List<EquiJoinClause> criteria = analysis.getJoinCriteria(node);

        // Add projections for join criteria
        PlanBuilder leftPlanBuilder = appendProjections(leftPlan, Iterables.transform(criteria, leftGetter()));
        PlanBuilder rightPlanBuilder = appendProjections(rightPlan, Iterables.transform(criteria, rightGetter()));

        ImmutableList.Builder<JoinNode.EquiJoinClause> clauses = ImmutableList.builder();
        for (EquiJoinClause clause : criteria) {
            Symbol leftSymbol = leftPlanBuilder.translate(clause.getLeft());
            Symbol rightSymbol = rightPlanBuilder.translate(clause.getRight());

            clauses.add(new JoinNode.EquiJoinClause(leftSymbol, rightSymbol));
        }

        List<Symbol> outputSymbols = ImmutableList.<Symbol>builder()
                .addAll(leftPlan.getOutputSymbols())
                .addAll(rightPlan.getOutputSymbols())
                .build();

        return new RelationPlan(new JoinNode(idAllocator.getNextId(), JoinNode.Type.typeConvert(node.getType()), leftPlanBuilder.getRoot(), rightPlanBuilder.getRoot(), clauses.build()), analysis.getOutputDescriptor(node), outputSymbols);
    }

    @Override
    protected RelationPlan visitTableSubquery(TableSubquery node, Void context)
    {
        return process(node.getQuery(), context);
    }

    @Override
    protected RelationPlan visitQuery(Query node, Void context)
    {
        PlanBuilder subPlan = new QueryPlanner(analysis, symbolAllocator, idAllocator, metadata, session).process(node, null);

        ImmutableList.Builder<Symbol> outputSymbols = ImmutableList.builder();
        for (FieldOrExpression fieldOrExpression : analysis.getOutputExpressions(node)) {
            outputSymbols.add(subPlan.translate(fieldOrExpression));
        }

        return new RelationPlan(subPlan.getRoot(), analysis.getOutputDescriptor(node),  outputSymbols.build());
    }

    @Override
    protected RelationPlan visitQuerySpecification(QuerySpecification node, Void context)
    {
        PlanBuilder subPlan = new QueryPlanner(analysis, symbolAllocator, idAllocator, metadata, session).process(node, null);

        ImmutableList.Builder<Symbol> outputSymbols = ImmutableList.builder();
        for (FieldOrExpression fieldOrExpression : analysis.getOutputExpressions(node)) {
            outputSymbols.add(subPlan.translate(fieldOrExpression));
        }

        return new RelationPlan(subPlan.getRoot(), analysis.getOutputDescriptor(node), outputSymbols.build());
    }

    @Override
    protected RelationPlan visitUnion(Union node, Void context)
    {
        checkArgument(!node.getRelations().isEmpty(), "No relations specified for UNION");

        List<Symbol> outputSymbols = null;
        ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();
        ImmutableListMultimap.Builder<Symbol, Symbol> symbolMapping = ImmutableListMultimap.builder();
        for (Relation relation : node.getRelations()) {
            RelationPlan relationPlan = process(relation, context);

            if (outputSymbols == null) {
                // Use the first Relation to derive output symbol names
                ImmutableList.Builder<Symbol> outputSymbolBuilder = ImmutableList.builder();
                for (Symbol symbol : relationPlan.getOutputSymbols()) {
                    outputSymbolBuilder.add(symbolAllocator.newSymbol(symbol.getName(), symbolAllocator.getTypes().get(symbol)));
                }
                outputSymbols = outputSymbolBuilder.build();
            }

            for (int i = 0; i < outputSymbols.size(); i++) {
                symbolMapping.put(outputSymbols.get(i), relationPlan.getOutputSymbols().get(i));
            }

            sources.add(relationPlan.getRoot());
        }

        PlanNode planNode = new UnionNode(idAllocator.getNextId(), sources.build(), symbolMapping.build());
        if (node.isDistinct()) {
            planNode = distinct(planNode);
        }
        return new RelationPlan(planNode, analysis.getOutputDescriptor(node), planNode.getOutputSymbols());
    }

    private PlanBuilder appendProjections(RelationPlan subPlan, Iterable<Expression> expressions)
    {
        TranslationMap translations = new TranslationMap(subPlan, analysis);

        // Make field->symbol mapping from underlying relation plan available for translations
        // This makes it possible to rewrite FieldOrExpressions that reference fields from the underlying tuple directly
        translations.setFieldMappings(subPlan.getOutputSymbols());

        ImmutableMap.Builder<Symbol, Expression> projections = ImmutableMap.builder();

        // add an identity projection for underlying plan
        for (Symbol symbol : subPlan.getRoot().getOutputSymbols()) {
            Expression expression = new QualifiedNameReference(symbol.toQualifiedName());
            projections.put(symbol, expression);
        }

        ImmutableMap.Builder<Symbol, Expression> newTranslations = ImmutableMap.builder();
        for (Expression expression : expressions) {
            Symbol symbol = symbolAllocator.newSymbol(expression, analysis.getType(expression));

            projections.put(symbol, translations.rewrite(expression));
            newTranslations.put(symbol, expression);
        }
        // Now append the new translations into the TranslationMap
        for (Map.Entry<Symbol, Expression> entry : newTranslations.build().entrySet()) {
            translations.put(entry.getValue(), entry.getKey());
        }

        return new PlanBuilder(translations, new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), projections.build()));
    }

    private PlanNode distinct(PlanNode node)
    {
        return new AggregationNode(idAllocator.getNextId(),
                node,
                node.getOutputSymbols(),
                ImmutableMap.<Symbol, FunctionCall>of(),
                ImmutableMap.<Symbol, FunctionHandle>of());
    }
}
