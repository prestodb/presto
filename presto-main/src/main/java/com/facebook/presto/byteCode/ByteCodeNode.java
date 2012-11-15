package com.facebook.presto.byteCode;

import org.objectweb.asm.MethodVisitor;

import java.util.List;

public interface ByteCodeNode
{
    List<ByteCodeNode> getChildNodes();

    void accept(MethodVisitor visitor);

    public abstract <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor);

}
