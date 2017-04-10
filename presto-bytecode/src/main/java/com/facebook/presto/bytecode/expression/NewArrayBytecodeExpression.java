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
package com.facebook.presto.bytecode.expression;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.MethodGenerationContext;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.instruction.TypeInstruction;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;

import static com.facebook.presto.bytecode.ArrayOpCode.getArrayOpCode;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

class NewArrayBytecodeExpression
        extends BytecodeExpression
{
    private final BytecodeExpression length;
    private final ParameterizedType elementType;

    @Nullable
    private final List<BytecodeExpression> elements;

    public NewArrayBytecodeExpression(ParameterizedType type, int length)
    {
        this(type, constantInt(length));
    }

    public NewArrayBytecodeExpression(ParameterizedType type, BytecodeExpression length)
    {
        super(type);
        requireNonNull(type, "type is null");
        checkArgument(type.getArrayComponentType() != null, "type %s must be array type");
        this.elementType = type.getArrayComponentType();
        this.length = requireNonNull(length, "length is null");
        this.elements = null;
    }

    public NewArrayBytecodeExpression(ParameterizedType type, List<BytecodeExpression> elements)
    {
        super(type);
        requireNonNull(type, "type is null");
        checkArgument(type.getArrayComponentType() != null, "type %s must be array type", type);
        requireNonNull(elements, "elements is null");
        this.elementType = type.getArrayComponentType();
        for (int i = 0; i < elements.size(); i++) {
            BytecodeExpression element = elements.get(i);
            ParameterizedType elementType = element.getType();
            checkArgument(elementType.equals(this.elementType), "element %s must be %s type, but is %s", i, this.elementType, elementType);
        }
        this.length = constantInt(elements.size());
        this.elements = elements;
    }

    @Override
    public BytecodeNode getBytecode(MethodGenerationContext generationContext)
    {
        BytecodeBlock bytecodeBlock;
        if (elementType.isPrimitive()) {
            bytecodeBlock = new BytecodeBlock()
                    .append(length)
                    .append(TypeInstruction.newPrimitiveArray(elementType));
        }
        else {
            bytecodeBlock = new BytecodeBlock()
                    .append(length)
                    .append(TypeInstruction.newObjectArray(elementType));
        }
        if (elements != null) {
            for (int i = 0; i < elements.size(); i++) {
                BytecodeExpression element = elements.get(i);
                bytecodeBlock
                        .dup()
                        .append(constantInt(i))
                        .append(element)
                        .append(getArrayOpCode(elementType).getStore());
            }
        }
        return bytecodeBlock;
    }

    @Override
    protected String formatOneLine()
    {
        if (elements == null) {
            return "new " + elementType.getSimpleName() + "[" + length + "]";
        }
        return "new " + elementType.getSimpleName() + "[] {" + Joiner.on(", ").join(elements) + "}";
    }

    @Override
    public List<BytecodeNode> getChildNodes()
    {
        return ImmutableList.of();
    }
}
