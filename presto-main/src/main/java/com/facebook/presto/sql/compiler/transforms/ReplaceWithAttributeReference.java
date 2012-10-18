package com.facebook.presto.sql.compiler.transforms;

import com.facebook.presto.sql.compiler.NodeRewriter;
import com.facebook.presto.sql.compiler.TreeRewriter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;

import java.util.Map;

public class ReplaceWithAttributeReference
    extends NodeRewriter<Void>
{
    private final Map<Expression, QualifiedName> attributes;

    public ReplaceWithAttributeReference(Map<Expression, QualifiedName> attributes)
    {
        this.attributes = attributes;
    }

    @Override
    public Node rewriteExpression(Expression node, Void context, TreeRewriter<Void> treeRewriter)
    {
        QualifiedName name = attributes.get(node);
        if (name != null) {
            return new QualifiedNameReference(name);
        }

        return null;
    }

}
