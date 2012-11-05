package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRewriter;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

public class NameToSlotRewriter
        extends NodeRewriter<Void>
{
    private final TupleDescriptor descriptor;

    public NameToSlotRewriter(TupleDescriptor descriptor)
    {
        this.descriptor = descriptor;
    }

    @Override
    public Node rewriteQualifiedNameReference(QualifiedNameReference node, Void context, TreeRewriter<Void> treeRewriter)
    {
        Slot slot = Iterables.getOnlyElement(descriptor.resolve(node.getName())).getSlot();

        Preconditions.checkState(slot != null, "Found unresolved name: %s", node.getName());

        return new SlotReference(slot);
    }
}

