package com.facebook.presto.byteCode.instruction;

import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ByteCodeVisitor;
import com.facebook.presto.byteCode.OpCodes;

import java.util.List;

import static com.facebook.presto.byteCode.OpCodes.*;

public class JumpInstruction implements InstructionNode
{
    public static InstructionNode jump(LabelNode label)
    {
        return new JumpInstruction(GOTO, label);
    }

    public static InstructionNode jumpIfEqualZero(LabelNode label)
    {
        return new JumpInstruction(IFEQ, label);
    }

    public static InstructionNode jumpIfNotEqualZero(LabelNode label)
    {
        return new JumpInstruction(IFNE, label);
    }

    public static InstructionNode jumpIfLessThanZero(LabelNode label)
    {
        return new JumpInstruction(IFLT, label);
    }

    public static InstructionNode jumpIfGreaterThanZero(LabelNode label)
    {
        return new JumpInstruction(IFGT, label);
    }

    public static InstructionNode jumpIfLessThanOrEqualZero(LabelNode label)
    {
        return new JumpInstruction(IFLE, label);
    }

    public static InstructionNode jumpIfIntGreaterThanOrEqualZero(LabelNode label)
    {
        return new JumpInstruction(IFGE, label);
    }

    public static InstructionNode jumpIfIntEqual(LabelNode label)
    {
        return new JumpInstruction(IF_ICMPEQ, label);
    }

    public static InstructionNode jumpIfIntNotEqual(LabelNode label)
    {
        return new JumpInstruction(IF_ICMPNE, label);
    }

    public static InstructionNode jumpIfIntLessThan(LabelNode label)
    {
        return new JumpInstruction(IF_ICMPLT, label);
    }

    public static InstructionNode jumpIfIntGreaterThan(LabelNode label)
    {
        return new JumpInstruction(IF_ICMPGT, label);
    }

    public static InstructionNode jumpIfIntLessThanOrEqual(LabelNode label)
    {
        return new JumpInstruction(IF_ICMPLE, label);
    }

    public static InstructionNode jumpIfIntGreaterThanOrEqual(LabelNode label)
    {
        return new JumpInstruction(IF_ICMPGE, label);
    }

    public static InstructionNode jumpIfNull(LabelNode label)
    {
        return new JumpInstruction(IFNULL, label);
    }

    public static InstructionNode jumpIfNotNull(LabelNode label)
    {
        return new JumpInstruction(IFNONNULL, label);
    }

    public static InstructionNode jumpIfObjectSame(LabelNode label)
    {
        return new JumpInstruction(IF_ACMPEQ, label);
    }

    public static InstructionNode jumpIfObjectNotSame(LabelNode label)
    {
        return new JumpInstruction(IF_ACMPNE, label);
    }

    private final OpCodes opCode;
    private final LabelNode label;

    private JumpInstruction(OpCodes opCode, LabelNode label)
    {
        this.opCode = opCode;
        this.label = label;
    }

    public OpCodes getOpCode()
    {
        return opCode;
    }

    public LabelNode getLabel()
    {
        return label;
    }

    @Override
    public void accept(MethodVisitor visitor)
    {
        visitor.visitJumpInsn(opCode.getOpCode(), label.getLabel());
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.of();
    }

    @Override
    public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
    {
        return visitor.visitJumpInstruction(parent, this);
    }
}
