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
package com.facebook.presto.byteCode.expression;

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.MethodGenerationContext;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.google.common.base.Preconditions.checkArgument;

class NotByteCodeExpression
        extends ByteCodeExpression
{
    private final ByteCodeExpression value;

    NotByteCodeExpression(ByteCodeExpression value)
    {
        super(type(boolean.class));
        this.value = value;
        checkArgument(value.getType().getPrimitiveType() == boolean.class, "Expected value to be type boolean but is %s", value.getType());
    }

    @Override
    public ByteCodeNode getByteCode(MethodGenerationContext generationContext)
    {
        LabelNode trueLabel = new LabelNode("true");
        LabelNode endLabel = new LabelNode("end");
        return new Block()
                .append(value)
                .ifTrueGoto(trueLabel)
                .push(true)
                .gotoLabel(endLabel)
                .visitLabel(trueLabel)
                .push(false)
                .visitLabel(endLabel);
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.<ByteCodeNode>of(value);
    }

    @Override
    protected String formatOneLine()
    {
        return "(!" + value + ")";
    }
}
