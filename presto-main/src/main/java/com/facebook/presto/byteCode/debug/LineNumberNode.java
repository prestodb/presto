package com.facebook.presto.byteCode.debug;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ByteCodeVisitor;
import com.facebook.presto.byteCode.instruction.LabelNode;

import java.util.List;

public class LineNumberNode implements DebugNode
{
    private final int lineNumber;
    private final LabelNode label = new LabelNode();

    public LineNumberNode(int lineNumber)
    {
        this.lineNumber = lineNumber;
    }

    @Override
    public void accept(MethodVisitor visitor)
    {
        visitor.visitLineNumber(lineNumber, label.getLabel());
    }

    public int getLineNumber()
    {
        return lineNumber;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("line", lineNumber)
                .toString();
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.of();
    }

    @Override
    public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
    {
        return visitor.visitLineNumber(parent, this);
    }
}
