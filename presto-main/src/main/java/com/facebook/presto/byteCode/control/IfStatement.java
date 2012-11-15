package com.facebook.presto.byteCode.control;

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ByteCodeNodeFactory;
import com.facebook.presto.byteCode.ByteCodeVisitor;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;

import java.util.List;

import static com.facebook.presto.byteCode.ByteCodeNodes.buildBlock;
import static com.facebook.presto.byteCode.ExpectedType.BOOLEAN;
import static com.facebook.presto.byteCode.ExpectedType.VOID;

public class IfStatement implements FlowControl
{
    public static IfStatementBuilder ifStatementBuilder(CompilerContext context)
    {
        return new IfStatementBuilder(context);
    }

    public static class IfStatementBuilder
    {
        private final CompilerContext context;

        private Block condition;
        private Block ifTrue;
        private Block ifFalse;

        public IfStatementBuilder(CompilerContext context)
        {
            this.context = context;
        }

        public void condition(Block condition)
        {
            this.condition = condition;
        }

        public IfStatementBuilder condition(ByteCodeNodeFactory condition)
        {
            this.condition = buildBlock(context, condition, BOOLEAN, "condition");
            return this;
        }

        public void ifTrue(Block ifTrue)
        {
            this.ifTrue = ifTrue;
        }

        public IfStatementBuilder ifTrue(ByteCodeNodeFactory ifTrue)
        {
            this.ifTrue = buildBlock(context, ifTrue, VOID, "ifTrue");
            return this;
        }

        public void ifFalse(Block ifFalse)
        {
            this.ifFalse = ifFalse;
        }

        public IfStatementBuilder ifFalse(ByteCodeNodeFactory ifFalse)
        {
            this.ifFalse = buildBlock(context, ifFalse, VOID, "ifFalse");
            return this;
        }

        public IfStatement build()
        {
            IfStatement ifStatement = new IfStatement(context, condition, ifTrue, ifFalse);
            return ifStatement;
        }
    }

    private final CompilerContext context;
    private final Block condition;
    private final Block ifTrue;
    private final Block ifFalse;

    private final LabelNode falseLabel = new LabelNode("false");
    private final LabelNode outLabel = new LabelNode("out");

    private IfStatement(CompilerContext context, Block condition, Block ifTrue, Block ifFalse)
    {
        this.context = context;
        this.condition = condition;
        this.ifTrue = ifTrue;
        this.ifFalse = ifFalse;
    }

    public Block getCondition()
    {
        return condition;
    }

    public Block getIfTrue()
    {
        return ifTrue;
    }

    public Block getIfFalse()
    {
        return ifFalse;
    }

    @Override
    public void accept(MethodVisitor visitor)
    {
        Block block = new Block(context);

        block.append(condition)
                .ifZeroGoto(falseLabel)
                .append(ifTrue);

        if (ifFalse != null) {
            block.gotoLabel(outLabel)
                    .visitLabel(falseLabel)
                    .append(ifFalse)
                    .visitLabel(outLabel);
        }
        else {
            block.visitLabel(falseLabel);
        }

        block.accept(visitor);
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        if (ifFalse == null) {
            return ImmutableList.<ByteCodeNode>of(condition, ifTrue);
        }
        else {
            return ImmutableList.<ByteCodeNode>of(condition, ifTrue, ifFalse);
        }
    }

    @Override
    public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
    {
        return visitor.visitIf(parent, this);
    }
}
