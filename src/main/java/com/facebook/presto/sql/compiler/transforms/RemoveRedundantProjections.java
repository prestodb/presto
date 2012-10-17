package com.facebook.presto.sql.compiler.transforms;

import com.facebook.presto.sql.compiler.NodeRewriter;
import com.facebook.presto.sql.compiler.TreeRewriter;
import com.facebook.presto.sql.tree.AliasedExpression;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.Subquery;
import com.facebook.presto.sql.tree.Table;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import static com.facebook.presto.sql.AstFunctions.unalias;
import static com.facebook.presto.sql.compiler.MoreFunctions.cast;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.collect.Iterables.transform;

/**
 * Gets rid of unnecessary subselects.
 *
 * All terms in outer query must be simple aliased qualified name references (arbitrary expressions or functions not supported)
 *
 * E.g., rewrites
 *
 * <pre>
 * SELECT a x, b y
 * FROM (
 *  SELECT id a, value b
 *  FROM T
 * ) U
 * </pre>
 *
 * as
 *
 * <pre>
 * SELECT id x, value y
 * FROM T U
 * </pre>
 *
 * Preconditions:
 *  {@link MaterializeImplicitAliases}
 *  {@link ExpandAllColumnsWildcard}
 */
public class RemoveRedundantProjections
    extends NodeRewriter<Void>
{
    @Override
    public Query rewriteQuery(Query node, Void context, TreeRewriter<Void> treeRewriter)
    {
        // process query recursively
        Query query = treeRewriter.defaultRewrite(node, context);

        Preconditions.checkArgument(query.getHaving() == null, "Queries with HAVING not supported by this transformer");
        Preconditions.checkArgument(Iterables.all(query.getSelect().getSelectItems(), instanceOf(AliasedExpression.class)), "All SELECT terms must have explicit aliases");

        Preconditions.checkArgument(query.getLimit() == null, "Queries with LIMIT not yet supported");
        Preconditions.checkArgument(query.getOrderBy().isEmpty(), "Queries with ORDER BY not yet supported");

        // Qualifying conditions:
        //  1. No WHERE or GROUP BY clause
        //  2. Single-relation FROM
        //  3. All select terms are simple QualifiedNameReferences
        if (query.getWhere() != null || !query.getGroupBy().isEmpty() || query.getFrom().size() > 1 ||
                !Iterables.all(transform(query.getSelect().getSelectItems(), unalias()), instanceOf(QualifiedNameReference.class))) {
            return query;
        }

        // 4. FROM must be a subquery
        Relation source = unaliasRelation(Iterables.getOnlyElement(query.getFrom()));
        if (!(source instanceof Subquery)) {
            // only rewrite if source is a nested subquery
            return query;
        }

        // 5. FROM must not be a group by query
        Query subquery = ((Subquery) source).getQuery();
        if (!subquery.getGroupBy().isEmpty()) {
            return query;
        }

        Preconditions.checkArgument(!Iterables.any(subquery.getSelect().getSelectItems(), instanceOf(AllColumns.class)), "Wildcards must be expanded");
        Preconditions.checkArgument(Iterables.all(subquery.getSelect().getSelectItems(), instanceOf(AliasedExpression.class)), "All SELECT terms must have explicit aliases");

        // Handle reordering: SELECT x x, y y, z z FROM ( SELECT b y, a x, c z ... )
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        for (AliasedExpression aliased : transform(query.getSelect().getSelectItems(), cast(AliasedExpression.class))) {
            QualifiedNameReference reference = (QualifiedNameReference) aliased.getExpression();

            for (AliasedExpression aliasedChild : transform(subquery.getSelect().getSelectItems(), cast(AliasedExpression.class))) {
                if (reference.getName().hasSuffix(QualifiedName.of(aliasedChild.getAlias()))) {
                    builder.add(new AliasedExpression(aliasedChild.getExpression(), aliased.getAlias()));
                    break;
                }
            }
        }

        Select select = new Select(query.getSelect().isDistinct() || subquery.getSelect().isDistinct(), builder.build());

        return new Query(select,
                subquery.getFrom(),
                subquery.getWhere(),
                subquery.getGroupBy(),
                subquery.getHaving(),
                subquery.getOrderBy(),
                subquery.getLimit());
    }

    private Relation unaliasRelation(Relation relation)
    {
        if (relation instanceof AliasedRelation) {
            return unaliasRelation(((AliasedRelation) relation).getRelation());
        }
        else if (relation instanceof Table || relation instanceof Subquery) {
            return relation;
        }
        throw new UnsupportedOperationException("not yet implemented: " + relation.getClass().getName());
    }
}
