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
import static com.google.common.base.Preconditions.checkNotNull;

class OrByteCodeExpression
        extends ByteCodeExpression
{
    private final ByteCodeExpression left;
    private final ByteCodeExpression right;

    OrByteCodeExpression(ByteCodeExpression left, ByteCodeExpression right)
    {
        super(type(boolean.class));
        this.left = checkNotNull(left, "left is null");
        checkArgument(left.getType().getPrimitiveType() == boolean.class, "Expected left to be type boolean but is %s", left.getType());
        this.right = checkNotNull(right, "right is null");
        checkArgument(right.getType().getPrimitiveType() == boolean.class, "Expected right to be type boolean but is %s", right.getType());
    }

    @Override
    public ByteCodeNode getByteCode(MethodGenerationContext generationContext)
    {
        LabelNode trueLabel = new LabelNode("true");
        LabelNode endLabel = new LabelNode("end");
        return new Block()
                .append(left)
                .ifTrueGoto(trueLabel)
                .append(right)
                .ifTrueGoto(trueLabel)
                .push(false)
                .gotoLabel(endLabel)
                .visitLabel(trueLabel)
                .push(true)
                .visitLabel(endLabel);
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.<ByteCodeNode>of(left, right);
    }

    @Override
    protected String formatOneLine()
    {
        return "(" + left + " || " + right + ")";
    }
}
