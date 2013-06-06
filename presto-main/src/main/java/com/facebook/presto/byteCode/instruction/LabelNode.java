package com.facebook.presto.byteCode.instruction;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ByteCodeVisitor;

import java.util.List;

public class LabelNode implements InstructionNode
{
    private final String name;
    private final Label label;

    public LabelNode()
    {
        this.label = new Label();
        this.name = "label@" + label.hashCode();
    }

    public LabelNode(com.facebook.presto.byteCode.instruction.LabelNode labelNode)
    {
        this.label = labelNode.getLabel();
        this.name = "label@" + label.hashCode();
    }

    public LabelNode(String name)
    {
        this.label = new Label();
        this.name = name + "@" + label.hashCode();
    }

    public String getName()
    {
        return name;
    }

    public Label getLabel()
    {
        return label;
    }

    @Override
    public void accept(MethodVisitor visitor)
    {
        visitor.visitLabel(label);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("name", name)
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
        return visitor.visitLabel(parent, this);
    }
}
