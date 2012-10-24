package com.facebook.presto.sql.compiler.transforms;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.sql.compiler.IterableUtils;
import com.facebook.presto.sql.compiler.NameGenerator;
import com.facebook.presto.sql.compiler.NodeRewriter;
import com.facebook.presto.sql.compiler.SessionMetadata;
import com.facebook.presto.sql.compiler.TreeRewriter;
import com.facebook.presto.sql.tree.AliasedExpression;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Subquery;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.concat;

/**
 * Rewrites
 *
 * <pre>
 * SELECT MAX(x) + MIN(y), SUM(z), MAX(x)
 * FROM T
 * </pre>
 *
 * as
 *
 * <pre>
 * SELECT _a0 + _a1, _a2, _a0
 * FROM (
 *  SELECT MAX(x) _a0, MIN(y) _a1, SUM(z) _a2
 *  FROM T
 * ) U
 * </pre>
 *
 * Prerequisites:
 *  {@link MaterializeImplicitAliases}
 *  {@link ExpandAllColumnsWildcard}
 */
public class ExtractAggregates
        extends NodeRewriter<Void>
{
    private final SessionMetadata metadata;
    private final NameGenerator namer;

    public ExtractAggregates(SessionMetadata metadata, NameGenerator namer)
    {
        this.metadata = metadata;
        this.namer = namer;
    }

    @Override
    public Query rewriteQuery(Query query, Void context, TreeRewriter<Void> treeRewriter)
    {
        Preconditions.checkArgument(!Iterables.all(query.getSelect().getSelectItems(), Predicates.instanceOf(AllColumns.class)), "'*' column specifier must be expanded");
        Preconditions.checkArgument(query.getHaving() == null, "Queries with HAVING clause not supported by this transformer");
        Preconditions.checkArgument(Iterables.all(query.getSelect().getSelectItems(), Predicates.instanceOf(AliasedExpression.class)), "All SELECT terms must be properly aliased");

        if (!query.getOrderBy().isEmpty()) {
            throw new UnsupportedOperationException("not yet implemented: queries with ORDER BY");
        }

        // process query recursively
        query = treeRewriter.defaultRewrite(query, context);

        // find aggregates recursively in the SELECT terms
        List<FunctionCall> aggregates = extractAggregates(query.getSelect().getSelectItems());
        if (aggregates.isEmpty()) {
            return query;
        }

        // Compute the fields for the inner query from the terms in the group by clause and aggregates
        Map<Expression, String> syntheticAttributes = IterableUtils.toMap(concat(query.getGroupBy(), aggregates), NameGenerator.<Expression>fieldAliasGenerator(namer));

        ImmutableList.Builder<Expression> fields = ImmutableList.builder();
        for (Map.Entry<Expression, String> entry : syntheticAttributes.entrySet()) {
            fields.add(new AliasedExpression(entry.getKey(), entry.getValue()));
        }

        Query subquery = new Query(new Select(false, fields.build()),
                query.getFrom(),
                query.getWhere(),
                query.getGroupBy(),
                null,
                ImmutableList.<SortItem>of(),
                null);


        String syntheticRelation = namer.newRelationAlias();

        // rewrite expressions in outer query in terms of fields produced by subquery
        ImmutableList.Builder<Expression> rewritten = ImmutableList.builder();
        for (Expression term : query.getSelect().getSelectItems()) {
            // qualify synthetic attributes with synthetic relation alias and rewrite the expression
            Map<Expression, QualifiedName> qualified = Maps.transformValues(syntheticAttributes, QualifiedName.addPrefixFunction(QualifiedName.of(syntheticRelation)));
            Expression rewrittenTerm = TreeRewriter.rewriteWith(new ReplaceWithAttributeReference(qualified), term);

            // alias the term if it's not already aliased
            if (!(rewrittenTerm instanceof AliasedExpression)) {
                rewrittenTerm = new AliasedExpression(rewrittenTerm, namer.newFieldAlias());
            }
            rewritten.add(rewrittenTerm);
        }

        Query result = new Query(new Select(query.getSelect().isDistinct(), rewritten.build()),
                ImmutableList.<Relation>of(new AliasedRelation(new Subquery(subquery), syntheticRelation, null)),
                null,
                ImmutableList.<Expression>of(),
                null,
                ImmutableList.<SortItem>of(), // TODO: order by
                query.getLimit());

        return result;
    }

    @Override
    public AliasedRelation rewriteAliasedRelation(AliasedRelation node, Void context, TreeRewriter<Void> treeRewriter)
    {
        if (node.getColumnNames() != null && !node.getColumnNames().isEmpty()) {
            // TODO
            throw new UnsupportedOperationException("not yet implemented: aliased relation with column mappings");
        }

        Relation child = treeRewriter.rewrite(node.getRelation(), context);

        return new AliasedRelation(child, node.getAlias(), node.getColumnNames());
    }

    private List<FunctionCall> extractAggregates(List<Expression> terms)
    {
        final ImmutableList.Builder<FunctionCall> builder = ImmutableList.builder();

        AstVisitor<Void, Void> extractor = new DefaultTraversalVisitor<Void, Void>()
        {
            @Override
            protected Void visitFunctionCall(FunctionCall node, Void context)
            {
                FunctionInfo info = metadata.getFunction(node.getName());
                if (info != null && info.isAggregate()) {
                    builder.add(node);
                }

                super.visitFunctionCall(node, context); // visit children

                return null;
            }
        };

        for (Expression term : terms) {
            extractor.process(term, null);
        }

        return builder.build();
    }
}
