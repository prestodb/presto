/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.byteCode.instruction;

import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ByteCodeVisitor;
import com.facebook.presto.byteCode.OpCodes;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;

import java.util.List;

import static com.facebook.presto.byteCode.OpCodes.GOTO;
import static com.facebook.presto.byteCode.OpCodes.IFEQ;
import static com.facebook.presto.byteCode.OpCodes.IFGE;
import static com.facebook.presto.byteCode.OpCodes.IFGT;
import static com.facebook.presto.byteCode.OpCodes.IFLE;
import static com.facebook.presto.byteCode.OpCodes.IFLT;
import static com.facebook.presto.byteCode.OpCodes.IFNE;
import static com.facebook.presto.byteCode.OpCodes.IFNONNULL;
import static com.facebook.presto.byteCode.OpCodes.IFNULL;
import static com.facebook.presto.byteCode.OpCodes.IF_ACMPEQ;
import static com.facebook.presto.byteCode.OpCodes.IF_ACMPNE;
import static com.facebook.presto.byteCode.OpCodes.IF_ICMPEQ;
import static com.facebook.presto.byteCode.OpCodes.IF_ICMPGE;
import static com.facebook.presto.byteCode.OpCodes.IF_ICMPGT;
import static com.facebook.presto.byteCode.OpCodes.IF_ICMPLE;
import static com.facebook.presto.byteCode.OpCodes.IF_ICMPLT;
import static com.facebook.presto.byteCode.OpCodes.IF_ICMPNE;

public class JumpInstruction
        implements InstructionNode
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
