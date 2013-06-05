package com.facebook.presto.byteCode;

import com.facebook.presto.byteCode.instruction.InstructionNode;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;

import java.util.List;

public class Comment
        implements InstructionNode
{
    protected final String comment;

    public Comment(String comment)
    {
        this.comment = comment;
    }

    public String getComment()
    {
        return comment;
    }

    public void accept(MethodVisitor visitor)
    {
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(comment)
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
        return visitor.visitComment(parent, this);
    }
}
