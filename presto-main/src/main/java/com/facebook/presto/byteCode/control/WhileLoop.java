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

public class WhileLoop implements FlowControl
{
    public static WhileLoopBuilder whileLoopBuilder(CompilerContext context)
    {
        return new WhileLoopBuilder(context);
    }

    public static class WhileLoopBuilder
    {
        private final CompilerContext context;

        private final LabelNode beginLabel = new LabelNode("begin");
        private final LabelNode endLabel = new LabelNode("end");

        private Block condition;
        private Block body;

        public WhileLoopBuilder(CompilerContext context)
        {
            this.context = context;
            context.pushIterationScope(beginLabel, endLabel);
        }

        public WhileLoopBuilder condition(ByteCodeNodeFactory condition)
        {
            this.condition = buildBlock(context, condition, BOOLEAN, "condition");
            return this;
        }

        public WhileLoopBuilder body(ByteCodeNodeFactory body)
        {
            this.body = buildBlock(context, body, VOID, "body");
            return this;
        }

        public WhileLoop build()
        {
            WhileLoop whileLoop = new WhileLoop(context, condition, body, beginLabel, endLabel);
            context.popIterationScope();
            return whileLoop;
        }
    }

    private final CompilerContext context;
    private final Block condition;
    private final Block body;

    private final LabelNode beginLabel;
    private final LabelNode endLabel;

    private WhileLoop(CompilerContext context, Block condition, Block body, LabelNode beginLabel, LabelNode endLabel)
    {
        this.context = context;
        this.condition = condition;
        this.body = body;
        this.beginLabel = beginLabel;
        this.endLabel = endLabel;
    }

    @Override
    public void accept(MethodVisitor visitor)
    {
        Block block = new Block(context)
                .visitLabel(beginLabel)
                .append(condition)
                .ifZeroGoto(endLabel)
                .append(body)
                .gotoLabel(beginLabel)
                .visitLabel(endLabel);

        block.accept(visitor);
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.<ByteCodeNode>of(condition, body);
    }

    @Override
    public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
    {
        return visitor.visitWhile(parent, this);
    }
}
