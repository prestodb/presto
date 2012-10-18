package com.facebook.presto.sql.compiler.transforms;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.compiler.NodeRewriter;
import com.facebook.presto.sql.compiler.SchemaExtractor;
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
import com.facebook.presto.sql.tree.Table;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.compiler.IterableUtils.sameElements;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Predicates.or;

/**
 * Rewrites
 *
 * <pre>
 * SELECT * FROM T
 * </pre>
 * as
 *
 * <pre>
 * SELECT T.field0 field0, T.field1 field1, ... FROM T
 * </pre>
 *
 * and
 *
 * <pre>
 * SELECT U.* FROM (
 *  SELECT field0 a, field1 a,
 *  FROM T
 * ) U
 * </pre>
 *
 * as
 *
 * <pre>
 * SELECT _0 a, _1 a FROM (
 *  SELECT field0 _0, field1 _1
 *  FROM T
 * ) U
 * </pre>
 *
 * Prerequisites:
 *  {@link MaterializeImplicitAliases}
 */
public class ExpandAllColumnsWildcard
        extends NodeRewriter<Void>
{
    private final Metadata metadata;

    public ExpandAllColumnsWildcard(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public Query rewriteQuery(Query node, Void context, TreeRewriter<Void> treeRewriter)
    {
        // process query recursively
        Query query = treeRewriter.defaultRewrite(node, context);

        if (!Iterables.any(query.getSelect().getSelectItems(), instanceOf(AllColumns.class))) {
            return query; // nothing to do
        }

        Preconditions.checkArgument(Iterables.all(node.getSelect().getSelectItems(),
                or(instanceOf(AllColumns.class), instanceOf(AliasedExpression.class))), "SELECT terms must be wildcard (* or T.*) or aliased expressions");

        Map<QualifiedName, Relation> relationsByName = Maps.uniqueIndex(query.getFrom(), extractRelationName());

        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        for (Expression term : query.getSelect().getSelectItems()) {
            if (term instanceof AllColumns) {
                Optional<QualifiedName> optionalPrefix = ((AllColumns) term).getPrefix();

                List<Relation> relations = query.getFrom(); // assume *
                if (optionalPrefix.isPresent()) {
                    // X.*
                    relations = ImmutableList.of(relationsByName.get(optionalPrefix.get()));
                }

                for (Relation relation : relations) {
                    SchemaExtractor extractor = new SchemaExtractor(metadata);
                    for (QualifiedName name : extractor.extract(relation)) {
                        builder.add(new AliasedExpression(new QualifiedNameReference(name), name.getSuffix()));
                    }
                }
            }
            else {
                builder.add(term);
            }
        }

        // TODO: handle case where subquery select terms are aliased with ambiguous names:
        // SELECT * FROM (SELECT T.id x, T.value x FROM T) U

        if (!sameElements(query.getSelect().getSelectItems(), builder.build())) {
            return new Query(new Select(query.getSelect().isDistinct(), builder.build()),
                    query.getFrom(),
                    query.getWhere(),
                    query.getGroupBy(),
                    query.getHaving(),
                    query.getOrderBy(),
                    query.getLimit());
        }

        return query;
    }

    private static Function<Relation, QualifiedName> extractRelationName()
    {
        return new Function<Relation, QualifiedName>()
        {
            @Override
            public QualifiedName apply(Relation input)
            {
                if (input instanceof AliasedRelation) {
                    return QualifiedName.of(((AliasedRelation) input).getAlias());
                }
                else if (input instanceof Table) {
                    return ((Table) input).getName();
                }

                throw new UnsupportedOperationException("not yet implemented: " + input.getClass().getName());
            }
        };
    }
}
