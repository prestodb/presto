package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.tree.AliasedExpression;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Subquery;
import com.facebook.presto.sql.tree.Table;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.List;

import static com.facebook.presto.sql.tree.AliasedExpression.aliasGetter;
import static com.google.common.base.Predicates.instanceOf;

/**
 * Computes the output schema (attribute names) for a relation
 *
 * TODO: this is done implicitly by the SemanticAnalyzer. Figure whether it's possible to get the analyzer to use this class instead
 */
public class SchemaExtractor
{
    private final SessionMetadata metadata;

    public SchemaExtractor(SessionMetadata metadata)
    {
        this.metadata = metadata;
    }

    public List<QualifiedName> extract(Relation relation)
    {
        AstVisitor<List<QualifiedName>, Void> extractor = new AstVisitor<List<QualifiedName>, Void>()
        {
            @Override
            protected List<QualifiedName> visitNode(Node node, Void context)
            {
                throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
            }

            @Override
            protected List<QualifiedName> visitTable(Table node, Void context)
            {
                return metadata.getTableSchema(node.getName());
            }

            @Override
            protected List<QualifiedName> visitAliasedRelation(AliasedRelation node, Void context)
            {
                Preconditions.checkArgument(node.getColumnNames() == null || node.getColumnNames().isEmpty(), "column aliasing not yet supported");

                List<QualifiedName> child = process(node.getRelation(), context);
                return Lists.transform(child, AliasedRelation.applyAlias(node));
            }

            @Override
            protected List<QualifiedName> visitSubquery(Subquery node, Void context)
            {
                return process(node.getQuery(), context);
            }

            @Override
            protected List<QualifiedName> visitQuery(Query node, Void context)
            {
                Preconditions.checkArgument(Iterables.all(node.getSelect().getSelectItems(), instanceOf(AliasedExpression.class)), "SELECT terms in subquery must be aliased");

                return IterableTransformer.on(node.getSelect().getSelectItems())
                        .cast(AliasedExpression.class)
                        .transform(aliasGetter())
                        .list();
            }
        };

        return extractor.process(relation, null);
    }

}
