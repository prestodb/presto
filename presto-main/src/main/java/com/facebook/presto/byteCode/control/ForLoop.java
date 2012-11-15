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

public class ForLoop implements FlowControl
{
    public static ForLoopBuilder forLoopBuilder(CompilerContext context)
    {
        return new ForLoopBuilder(context);
    }

    public static class ForLoopBuilder
    {
        private final CompilerContext context;

        private final LabelNode continueLabel = new LabelNode("continue");
        private final LabelNode endLabel = new LabelNode("end");

        private Block initialize;
        private Block condition;
        private Block update;
        private Block body;

        public ForLoopBuilder(CompilerContext context)
        {
            this.context = context;
            context.pushIterationScope(continueLabel, endLabel);
        }

        public ForLoopBuilder initialize(ByteCodeNodeFactory initialize)
        {
            this.initialize = buildBlock(context, initialize, VOID, "initialize");
            return this;
        }

        public ForLoopBuilder condition(ByteCodeNodeFactory condition)
        {
            this.condition = buildBlock(context, condition, BOOLEAN, "condition");
            return this;
        }

        public ForLoopBuilder update(ByteCodeNodeFactory update)
        {
            this.update = buildBlock(context, update, VOID, "update");
            return this;
        }

        public ForLoopBuilder body(ByteCodeNodeFactory body)
        {
            this.body = buildBlock(context, body, VOID, "body");
            return this;
        }

        public ForLoop build()
        {
            ForLoop forLoop = new ForLoop(context, initialize, condition, update, body, continueLabel, endLabel);
            context.popIterationScope();
            return forLoop;
        }
    }

    private final CompilerContext context;
    private final Block initialize;
    private final Block condition;
    private final Block update;
    private final Block body;

    private final LabelNode beginLabel = new LabelNode("beginLabel");
    private final LabelNode continueLabel;
    private final LabelNode endLabel;

    private ForLoop(CompilerContext context, Block initialize, Block condition, Block update, Block body, LabelNode continueLabel, LabelNode endLabel)
    {
        this.context = context;
        this.initialize = initialize;
        this.condition = condition;
        this.update = update;
        this.body = body;
        this.continueLabel = continueLabel;
        this.endLabel = endLabel;
    }

    @Override
    public void accept(MethodVisitor visitor)
    {
        Block block = new Block(context)
                .append(initialize)
                .visitLabel(beginLabel)
                .append(condition)
                .ifZeroGoto(endLabel);

        if (body != null) {
            block.append(body);
        }

        block.visitLabel(continueLabel)
                .append(update)
                .gotoLabel(beginLabel)
                .visitLabel(endLabel);

        block.accept(visitor);
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.<ByteCodeNode>of(initialize, condition, update, body);
    }

    @Override
    public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
    {
        return visitor.visitFor(parent, this);
    }
}
