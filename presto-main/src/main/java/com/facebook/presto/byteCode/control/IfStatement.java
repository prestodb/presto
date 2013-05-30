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

        private ByteCodeNode condition;
        private ByteCodeNode ifTrue;
        private ByteCodeNode ifFalse;

        public IfStatementBuilder(CompilerContext context)
        {
            this.context = context;
        }

        public IfStatementBuilder condition(ByteCodeNode condition)
        {
            this.condition = buildBlock(context, condition, "condition");
            return this;
        }

        public IfStatementBuilder condition(ByteCodeNodeFactory condition)
        {
            this.condition = buildBlock(context, condition, BOOLEAN, "condition");
            return this;
        }

        public IfStatementBuilder ifTrue(ByteCodeNode ifTrue)
        {
            this.ifTrue = buildBlock(context, ifTrue, "ifTrue");
            return this;
        }

        public IfStatementBuilder ifTrue(ByteCodeNodeFactory ifTrue)
        {
            this.ifTrue = buildBlock(context, ifTrue, VOID, "ifTrue");
            return this;
        }

        public IfStatementBuilder ifFalse(ByteCodeNode ifFalse)
        {
            this.ifFalse = buildBlock(context, ifFalse, "ifFalse");
            return this;
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
    private final ByteCodeNode condition;
    private final ByteCodeNode ifTrue;
    private final ByteCodeNode ifFalse;

    private final LabelNode falseLabel = new LabelNode("false");
    private final LabelNode outLabel = new LabelNode("out");

    public IfStatement(CompilerContext context, ByteCodeNode condition, ByteCodeNode ifTrue, ByteCodeNode ifFalse)
    {
        this.context = context;
        this.condition = condition;
        this.ifTrue = ifTrue;
        this.ifFalse = ifFalse;
    }

    public ByteCodeNode getCondition()
    {
        return condition;
    }

    public ByteCodeNode getIfTrue()
    {
        return ifTrue;
    }

    public ByteCodeNode getIfFalse()
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
            return ImmutableList.of(condition, ifTrue);
        }
        else {
            return ImmutableList.of(condition, ifTrue, ifFalse);
        }
    }

    @Override
    public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
    {
        return visitor.visitIf(parent, this);
    }
}
