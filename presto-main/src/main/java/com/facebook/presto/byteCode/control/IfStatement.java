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

import com.facebook.presto.byteCode.ByteCodeBlock;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ByteCodeVisitor;
import com.facebook.presto.byteCode.MethodGenerationContext;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class IfStatement
        implements FlowControl
{
    private final String comment;
    private final ByteCodeBlock condition = new ByteCodeBlock();
    private final ByteCodeBlock ifTrue = new ByteCodeBlock();
    private final ByteCodeBlock ifFalse = new ByteCodeBlock();

    private final LabelNode falseLabel = new LabelNode("false");
    private final LabelNode outLabel = new LabelNode("out");

    public IfStatement()
    {
        this.comment = null;
    }

    public IfStatement(String format, Object... args)
    {
        this.comment = String.format(format, args);
    }

    @Override
    public String getComment()
    {
        return comment;
    }

    public ByteCodeBlock condition()
    {
        return condition;
    }

    public IfStatement condition(ByteCodeNode node)
    {
        checkState(condition.isEmpty(), "condition already set");
        condition.append(node);
        return this;
    }

    public ByteCodeBlock ifTrue()
    {
        return ifTrue;
    }

    public IfStatement ifTrue(ByteCodeNode node)
    {
        checkState(ifTrue.isEmpty(), "ifTrue already set");
        ifTrue.append(node);
        return this;
    }

    public ByteCodeBlock ifFalse()
    {
        return ifFalse;
    }

    public IfStatement ifFalse(ByteCodeNode node)
    {
        checkState(ifFalse.isEmpty(), "ifFalse already set");
        ifFalse.append(node);
        return this;
    }

    @Override
    public void accept(MethodVisitor visitor, MethodGenerationContext generationContext)
    {
        checkState(!condition.isEmpty(), "IfStatement does not have a condition set");
        checkState(!ifTrue.isEmpty() || !ifFalse.isEmpty(), "IfStatement does not have a true or false block set");

        ByteCodeBlock block = new ByteCodeBlock();

        // if !condition goto false;
        block.append(new ByteCodeBlock()
                .setDescription("condition")
                .append(condition));
        block.ifFalseGoto(falseLabel);

        if (!ifTrue.isEmpty()) {
            block.append(new ByteCodeBlock()
                    .setDescription("ifTrue")
                    .append(ifTrue));
        }

        if (!ifFalse.isEmpty()) {
            // close true case by skipping to end
            block.gotoLabel(outLabel);

            block.visitLabel(falseLabel);
            block.append(new ByteCodeBlock()
                    .setDescription("ifFalse")
                    .append(ifFalse));
            block.visitLabel(outLabel);
        }
        else {
            block.visitLabel(falseLabel);
        }

        block.accept(visitor, generationContext);
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.of(condition, ifTrue, ifFalse);
    }

    @Override
    public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
    {
        return visitor.visitIf(parent, this);
    }
}
