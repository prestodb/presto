package com.facebook.presto.sql.compiler.transforms;

import com.facebook.presto.sql.compiler.SessionMetadata;
import com.facebook.presto.sql.compiler.IterableUtils;
import com.facebook.presto.sql.compiler.NameGenerator;
import com.facebook.presto.sql.compiler.NodeRewriter;
import com.facebook.presto.sql.compiler.TreeRewriter;
import com.facebook.presto.sql.tree.AliasedExpression;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Subquery;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.AstFunctions.isAliasedAggregateFunction;
import static com.facebook.presto.sql.AstFunctions.isAliasedQualifiedNameReference;
import static com.facebook.presto.sql.compiler.MoreFunctions.cast;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Predicates.or;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;

/**
 * Rewrites
 *
 * <pre>
 * SELECT a, SUM(b + c)
 * FROM T
 * WHERE b > 10
 * GROUP BY a
 * </pre>
 *
 * as
 *
 * <pre>
 * SELECT a, SUM(x)
 * FROM (
 *  SELECT a, b + c x
 *  FROM T
 *  WHERE b > 10
 * ) U
 * GROUP BY a
 * </pre>
 *
 * Prerequisites:
 *  - {@link ExtractAggregates}
 */
public class ExtractScalarExpressions
        extends NodeRewriter<Void>
{
    private final SessionMetadata metadata;
    private final NameGenerator namer;

    public ExtractScalarExpressions(SessionMetadata metadata, NameGenerator namer)
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

        if (query.getLimit() != null) {
            throw new UnsupportedOperationException("not yet implemented: queries with LIMIT");
        }

        // process query recursively
        query = treeRewriter.defaultRewrite(query, context);

        if (!isCandidate(query.getSelect().getSelectItems())) {
            return query;
        }

        ImmutableSet.Builder<Expression> expressions = ImmutableSet.builder();
        for (AliasedExpression aliased : transform(query.getSelect().getSelectItems(), cast(AliasedExpression.class))) {
            Expression expression = aliased.getExpression();
            if (expression instanceof FunctionCall) {
                // unnest arguments to aggregation functions
                FunctionCall function = (FunctionCall) expression;

                // This is invariant is guaranteed by the call to isCandidate() above, but check anyway in case the code changes later
                Preconditions.checkState(metadata.getFunction(function.getName()).isAggregate(), "Expected function '%s' to be an aggregation", function.getName());

                expressions.addAll(function.getArguments());
            }
            else if (expression instanceof QualifiedNameReference) {
                expressions.add(expression);
            }
            else {
                throw new IllegalArgumentException("SELECT terms must be either aggregate functions or qualified name references");
            }
        }

        // Compute the fields for the inner query from the terms in the group by clause and aggregates
        Map<Expression, String> syntheticAttributes = IterableUtils.toMap(expressions.build(), NameGenerator.<Expression>fieldAliasGenerator(namer));

        ImmutableList.Builder<Expression> fields = ImmutableList.builder();
        for (Map.Entry<Expression, String> entry : syntheticAttributes.entrySet()) {
            // Alias each term with a synthetic attribute
            fields.add(new AliasedExpression(entry.getKey(), entry.getValue()));
        }

        Query subquery = new Query(new Select(false, fields.build()),
                query.getFrom(),
                query.getWhere(),
                ImmutableList.<Expression>of(),
                null,
                ImmutableList.<SortItem>of(),
                null);


        String syntheticRelation = namer.newRelationAlias();

        // rewrite expressions in terms of references to synthetic attributes produced nested query
        Map<Expression, QualifiedName> qualified = Maps.transformValues(syntheticAttributes, QualifiedName.addPrefixFunction(QualifiedName.of(syntheticRelation)));
        Function<Node, Expression> rewriter = TreeRewriter.rewriteFunction(new ReplaceWithAttributeReference(qualified));
        List<Expression> rewrittenSelect = Lists.transform(query.getSelect().getSelectItems(), rewriter);
        List<Expression> rewrittenGroupBy = Lists.transform(query.getGroupBy(), rewriter);

        Query result = new Query(new Select(query.getSelect().isDistinct(), rewrittenSelect),
                ImmutableList.<Relation>of(new AliasedRelation(new Subquery(subquery), syntheticRelation, null)),
                null,
                rewrittenGroupBy,
                null,
                ImmutableList.<SortItem>of(), // TODO: order by
                query.getLimit());

        return result;
    }

    private boolean isCandidate(List<Expression> terms)
    {
        // conditions:
        //  1. Terms must be either QualifiedNameReferences or aggregation Functions
        if (!Iterables.all(terms, or(isAliasedQualifiedNameReference(), isAliasedAggregateFunction(metadata)))) {
            return false;
        }

        //  2. At least one term that is not an aggregation function or an argument to one aggregate needs to be a complex expression
        Iterable<Expression> unnested = concat(Iterables.transform(terms, new Function<Expression, List<Expression>>()
        {
            @Override
            public List<Expression> apply(Expression input)
            {
                if (input instanceof AliasedExpression) {
                    return apply(((AliasedExpression) input).getExpression());
                }

                if (input instanceof FunctionCall) {
                    // guaranteed to be an aggregation by the precondition above
                    return ((FunctionCall) input).getArguments();
                }

                return ImmutableList.of(input);
            }
        }));

        return Iterables.any(unnested, not(instanceOf(QualifiedNameReference.class)));
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
}
