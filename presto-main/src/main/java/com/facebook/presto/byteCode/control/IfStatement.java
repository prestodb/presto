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

public class IfStatement
        implements FlowControl
{
    public static IfStatementBuilder ifStatementBuilder(CompilerContext context)
    {
        return new IfStatementBuilder(context);
    }

    public static class IfStatementBuilder
    {
        private final CompilerContext context;

        private String comment;
        private ByteCodeNode condition;
        private ByteCodeNode ifTrue;
        private ByteCodeNode ifFalse;

        public IfStatementBuilder(CompilerContext context)
        {
            this.context = context;
        }

        public IfStatementBuilder comment(String format, Object... args)
        {
            this.comment = String.format(format, args);
            return this;
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
            IfStatement ifStatement = new IfStatement(context, comment, condition, ifTrue, ifFalse);
            return ifStatement;
        }
    }

    private final CompilerContext context;
    private final String comment;
    private final ByteCodeNode condition;
    private final ByteCodeNode ifTrue;
    private final ByteCodeNode ifFalse;

    private final LabelNode falseLabel = new LabelNode("false");
    private final LabelNode outLabel = new LabelNode("out");

    public IfStatement(CompilerContext context, ByteCodeNode condition, ByteCodeNode ifTrue, ByteCodeNode ifFalse)
    {
        this(context, null, condition, ifTrue, ifFalse);
    }

    public IfStatement(CompilerContext context, String comment, ByteCodeNode condition, ByteCodeNode ifTrue, ByteCodeNode ifFalse)
    {
        this.context = context;
        this.comment = comment;
        this.condition = condition;
        this.ifTrue = ifTrue;
        this.ifFalse = ifFalse;
    }

    public String getComment()
    {
        return comment;
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
