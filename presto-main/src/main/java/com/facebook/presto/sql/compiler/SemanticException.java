package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.tree.Node;

public class SemanticException
    extends RuntimeException
{
    private final Node node;

    public SemanticException(Node node, String format, Object... args)
    {
        super(String.format(format, args));
        this.node = node;
    }

    public Node getNode()
    {
        return node;
    }
}
