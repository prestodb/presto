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
package com.facebook.presto.byteCode.control;

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ByteCodeVisitor;
import com.facebook.presto.byteCode.MethodGenerationContext;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;

import java.util.List;

import static com.facebook.presto.byteCode.ByteCodeNodes.buildBlock;

public class ForLoop
        implements FlowControl
{
    public static ForLoopBuilder forLoopBuilder()
    {
        return new ForLoopBuilder();
    }

    public static class ForLoopBuilder
    {
        private final LabelNode continueLabel = new LabelNode("continue");
        private final LabelNode endLabel = new LabelNode("end");

        private String comment;
        private ByteCodeNode initialize;
        private ByteCodeNode condition;
        private ByteCodeNode update;
        private ByteCodeNode body;

        public ForLoopBuilder comment(String format, Object... args)
        {
            this.comment = String.format(format, args);
            return this;
        }

        public ForLoopBuilder initialize(ByteCodeNode initialize)
        {
            this.initialize = buildBlock(initialize, "initialize");
            return this;
        }

        public ForLoopBuilder condition(ByteCodeNode condition)
        {
            this.condition = buildBlock(condition, "condition");
            return this;
        }

        public ForLoopBuilder update(ByteCodeNode update)
        {
            this.update = buildBlock(update, "update");
            return this;
        }

        public ForLoopBuilder body(ByteCodeNode body)
        {
            this.body = buildBlock(body, "body");
            return this;
        }

        public ForLoop build()
        {
            ForLoop forLoop = new ForLoop(comment, initialize, condition, update, body, continueLabel, endLabel);
            return forLoop;
        }
    }

    private final String comment;
    private final ByteCodeNode initialize;
    private final ByteCodeNode condition;
    private final ByteCodeNode update;
    private final ByteCodeNode body;

    private final LabelNode beginLabel = new LabelNode("beginLabel");
    private final LabelNode continueLabel;
    private final LabelNode endLabel;

    private ForLoop(String comment,
            ByteCodeNode initialize,
            ByteCodeNode condition,
            ByteCodeNode update,
            ByteCodeNode body,
            LabelNode continueLabel,
            LabelNode endLabel)
    {
        this.comment = comment;
        this.initialize = initialize;
        this.condition = condition;
        this.update = update;
        this.body = body;
        this.continueLabel = continueLabel;
        this.endLabel = endLabel;
    }

    @Override
    public String getComment()
    {
        return comment;
    }

    public ByteCodeNode getInitialize()
    {
        return initialize;
    }

    public ByteCodeNode getCondition()
    {
        return condition;
    }

    public ByteCodeNode getUpdate()
    {
        return update;
    }

    public ByteCodeNode getBody()
    {
        return body;
    }

    @Override
    public void accept(MethodVisitor visitor, MethodGenerationContext generationContext)
    {
        Block block = new Block()
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

        block.accept(visitor, generationContext);
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.of(initialize, condition, update, body);
    }

    @Override
    public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
    {
        return visitor.visitFor(parent, this);
    }
}
