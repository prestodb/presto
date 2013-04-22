package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.EquiJoinClause;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.FieldOrExpression;
import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Subquery;
import com.facebook.presto.sql.tree.Table;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.analyzer.EquiJoinClause.leftGetter;
import static com.facebook.presto.sql.analyzer.EquiJoinClause.rightGetter;

class RelationPlanner
        extends DefaultTraversalVisitor<RelationPlan, Void>
{
    private final Analysis analysis;
    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;

    RelationPlanner(Analysis analysis, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        Preconditions.checkNotNull(analysis, "analysis is null");
        Preconditions.checkNotNull(symbolAllocator, "symbolAllocator is null");
        Preconditions.checkNotNull(idAllocator, "idAllocator is null");
        Preconditions.checkNotNull(metadata, "metadata is null");
        Preconditions.checkNotNull(session, "session is null");

        this.analysis = analysis;
        this.symbolAllocator = symbolAllocator;
        this.idAllocator = idAllocator;
    }

    @Override
    protected RelationPlan visitTable(Table node, Void context)
    {
        if (!node.getName().getPrefix().isPresent()) {
            Query namedQuery = analysis.getNamedQuery(node);
            if (namedQuery != null) {
                return process(namedQuery, null);
            }
        }

        TupleDescriptor descriptor = analysis.getOutputDescriptor(node);
        TableHandle handle = analysis.getTableHandle(node);

        ImmutableMap.Builder<Field, Symbol> mappings = ImmutableMap.builder();
        ImmutableMap.Builder<Symbol, ColumnHandle> columns = ImmutableMap.builder();
        for (Field field : descriptor.getFields()) {
            Symbol symbol = symbolAllocator.newSymbol(field.getName().get(), field.getType());
            mappings.put(field, symbol);
            columns.put(symbol, analysis.getColumn(field));
        }

        return new RelationPlan(mappings.build(), new TableScanNode(idAllocator.getNextId(), handle, columns.build()), descriptor);
    }

    @Override
    protected RelationPlan visitAliasedRelation(AliasedRelation node, Void context)
    {
        RelationPlan subPlan = process(node.getRelation(), context);

        TupleDescriptor outputDescriptor = analysis.getOutputDescriptor(node);

        ImmutableMap.Builder<Field, Symbol> mappings = ImmutableMap.builder();

        Iterator<Field> inputs = subPlan.getDescriptor().getFields().iterator();
        Iterator<Field> outputs = outputDescriptor.getFields().iterator();
        while (inputs.hasNext() && outputs.hasNext()) {
            Field inputField = inputs.next();
            Field outputField = outputs.next();

            mappings.put(outputField, subPlan.getSymbol(inputField));
        }

        return new RelationPlan(mappings.build(), subPlan.getRoot(), outputDescriptor);
    }

    @Override
    protected RelationPlan visitJoin(Join node, Void context)
    {
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

        Map<Field, Symbol> mappings = ImmutableMap.<Field, Symbol>builder()
                .putAll(leftPlan.getOutputMappings())
                .putAll(rightPlan.getOutputMappings())
                .build();

        return new RelationPlan(mappings, new JoinNode(idAllocator.getNextId(), leftPlanBuilder.getRoot(), rightPlanBuilder.getRoot(), clauses.build()), analysis.getOutputDescriptor(node));
    }

    @Override
    protected RelationPlan visitSubquery(Subquery node, Void context)
    {
        return process(node.getQuery(), context);
    }

    @Override
    protected RelationPlan visitQuery(Query node, Void context)
    {
        PlanBuilder subPlan = new QueryPlanner(analysis, symbolAllocator, idAllocator).process(node, null);

        TupleDescriptor outputDescriptor = analysis.getOutputDescriptor(node);
        Iterator<Field> outputFields = outputDescriptor.getFields().iterator();
        Iterator<FieldOrExpression> outputExpressions = analysis.getOutputExpressions(node).iterator();

        ImmutableMap.Builder<Field, Symbol> mappings = ImmutableMap.builder();
        while (outputFields.hasNext() && outputExpressions.hasNext()) {
            Field outputField = outputFields.next();
            FieldOrExpression outputExpression = outputExpressions.next();

            Symbol symbol = subPlan.translate(outputExpression);
            mappings.put(outputField, symbol);
        }

        return new RelationPlan(mappings.build(), subPlan.getRoot(), outputDescriptor);
    }

    private PlanBuilder appendProjections(RelationPlan subPlan, Iterable<Expression> expressions)
    {
        TranslationMap translations = new TranslationMap(subPlan, analysis);

        // Make field->symbol mapping from underlying relation plan available for translations
        // This makes it possible to rewrite FieldOrExpressions that reference fields from the underlying tuple directly
        translations.addMappings(subPlan.getOutputMappings());

        ImmutableMap.Builder<Symbol, Expression> projections = ImmutableMap.builder();

        // add an identity projection for underlying plan
        for (Symbol symbol : subPlan.getRoot().getOutputSymbols()) {
            Expression expression = new QualifiedNameReference(symbol.toQualifiedName());
            projections.put(symbol, expression);
        }

        for (Expression expression : expressions) {
            Symbol symbol = symbolAllocator.newSymbol(expression, analysis.getType(expression));

            projections.put(symbol, translations.rewrite(expression));
            translations.put(expression, symbol);
        }

        return new PlanBuilder(translations, new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), projections.build()));
    }
}
