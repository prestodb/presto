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

import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.MethodGenerationContext;
import com.facebook.presto.byteCode.ParameterizedType;
import com.facebook.presto.byteCode.instruction.Constant;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.byteCode.ParameterizedType.type;

class ConstantByteCodeExpression
        extends ByteCodeExpression
{
    private final Constant value;

    public ConstantByteCodeExpression(Class<?> type, Constant value)
    {
        this(type(type), value);
    }

    public ConstantByteCodeExpression(ParameterizedType type, Constant value)
    {
        super(type);
        this.value = value;
    }

    @Override
    public Constant getByteCode(MethodGenerationContext generationContext)
    {
        return value;
    }

    @Override
    protected String formatOneLine()
    {
        return renderConstant(value.getValue());
    }

    public static String renderConstant(Object value)
    {
        if (value instanceof Long) {
            return value + "L";
        }
        if (value instanceof Float) {
            return value + "f";
        }
        if (value instanceof ParameterizedType) {
            return ((ParameterizedType) value).getSimpleName() + ".class";
        }
        // todo escape string
        if (value instanceof String) {
            return "\"" + value + "\"";
        }
        return String.valueOf(value);
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.of();
    }
}
