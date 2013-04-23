package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.Node;

import static com.google.common.base.Preconditions.checkNotNull;

public class SemanticException
    extends RuntimeException
{
    private final SemanticErrorCode code;
    private final Node node;

    public SemanticException(SemanticErrorCode code, Node node, String format, Object... args)
    {
        super(String.format(format, args));

        checkNotNull(code, "code is null");
        checkNotNull(node, "node is null");

        this.code = code;
        this.node = node;
    }

    public Node getNode()
    {
        return node;
    }

    public SemanticErrorCode getCode()
    {
        return code;
    }
}
