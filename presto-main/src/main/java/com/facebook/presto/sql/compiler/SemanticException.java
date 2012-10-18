package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.tree.Node;

public class SemanticException
    extends RuntimeException
{
    private final Node node;

    public SemanticException(String message, Node node)
    {
        super(message);
        this.node = node;
    }

    public Node getNode()
    {
        return node;
    }
}
