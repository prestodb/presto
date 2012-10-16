package com.facebook.presto.sql.compiler.transforms;

import com.facebook.presto.sql.compiler.NodeRewriter;
import com.facebook.presto.sql.compiler.TreeRewriter;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;

import java.util.Map;

public class QualifyNames
        extends NodeRewriter<Void>
{
    private final Map<QualifiedNameReference, QualifiedName> names;

    public QualifyNames(Map<QualifiedNameReference, QualifiedName> names)
    {
        this.names = names;
    }

    @Override
    public QualifiedNameReference rewriteQualifiedNameReference(QualifiedNameReference node, Void context, TreeRewriter<Void> treeRewriter)
    {
        QualifiedName qualified = names.get(node);
        if (qualified != null) {
            return new QualifiedNameReference(qualified);
        }

        return node;
    }

    // TODO: qualify function call name
    // TODO: qualify table names
}
