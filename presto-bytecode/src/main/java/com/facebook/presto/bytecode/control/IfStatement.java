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
package com.facebook.presto.bytecode.control;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.BytecodeVisitor;
import com.facebook.presto.bytecode.MethodGenerationContext;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class IfStatement
        implements FlowControl
{
    private final String comment;
    private final BytecodeBlock condition = new BytecodeBlock();
    private final BytecodeBlock ifTrue = new BytecodeBlock();
    private final BytecodeBlock ifFalse = new BytecodeBlock();

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

    public BytecodeBlock condition()
    {
        return condition;
    }

    public IfStatement condition(BytecodeNode node)
    {
        checkState(condition.isEmpty(), "condition already set");
        condition.append(node);
        return this;
    }

    public BytecodeBlock ifTrue()
    {
        return ifTrue;
    }

    public IfStatement ifTrue(BytecodeNode node)
    {
        checkState(ifTrue.isEmpty(), "ifTrue already set");
        ifTrue.append(node);
        return this;
    }

    public BytecodeBlock ifFalse()
    {
        return ifFalse;
    }

    public IfStatement ifFalse(BytecodeNode node)
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

        BytecodeBlock block = new BytecodeBlock();

        // if !condition goto false;
        block.append(new BytecodeBlock()
                .setDescription("condition")
                .append(condition));
        block.ifFalseGoto(falseLabel);

        if (!ifTrue.isEmpty()) {
            block.append(new BytecodeBlock()
                    .setDescription("ifTrue")
                    .append(ifTrue));
        }

        if (!ifFalse.isEmpty()) {
            // close true case by skipping to end
            block.gotoLabel(outLabel);

            block.visitLabel(falseLabel);
            block.append(new BytecodeBlock()
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
    public List<BytecodeNode> getChildNodes()
    {
        return ImmutableList.of(condition, ifTrue, ifFalse);
    }

    @Override
    public <T> T accept(BytecodeNode parent, BytecodeVisitor<T> visitor)
    {
        return visitor.visitIf(parent, this);
    }
}
