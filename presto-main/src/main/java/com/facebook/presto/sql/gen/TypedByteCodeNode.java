package com.facebook.presto.sql.gen;

import com.facebook.presto.byteCode.ByteCodeNode;

public class TypedByteCodeNode
{
    public static TypedByteCodeNode typedByteCodeNode(ByteCodeNode node, Class<?> type)
    {
        return new TypedByteCodeNode(node, type);
    }

    private final ByteCodeNode node;
    private final Class<?> type;

    private TypedByteCodeNode(ByteCodeNode node, Class<?> type)
    {
        this.node = node;
        this.type = type;
    }

    public ByteCodeNode getNode()
    {
        return node;
    }

    public Class<?> getType()
    {
        return type;
    }
}
