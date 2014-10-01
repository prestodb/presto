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
package com.facebook.presto.sql.gen;

import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.expression.ByteCodeExpression;
import com.facebook.presto.byteCode.instruction.InvokeInstruction;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.lang.reflect.Method;
import java.util.List;

import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.google.common.base.Preconditions.checkNotNull;

public class SqlTypeByteCodeExpression
        extends ByteCodeExpression
{
    public static ByteCodeExpression constantType(CompilerContext context, CallSiteBinder callSiteBinder, Type type)
    {
        checkNotNull(context, "context is null");
        checkNotNull(callSiteBinder, "callSiteBinder is null");
        checkNotNull(type, "type is null");

        Binding binding = callSiteBinder.bind(type, Type.class);
        return new SqlTypeByteCodeExpression(type, binding, context.getDefaultBootstrapMethod());
    }

    private final Type type;
    private final Binding binding;
    private final Method bootstrapMethod;

    private SqlTypeByteCodeExpression(Type type, Binding binding, Method bootstrapMethod)
    {
        super(type(Type.class));

        this.type = checkNotNull(type, "type is null");
        this.binding = checkNotNull(binding, "binding is null");
        this.bootstrapMethod = checkNotNull(bootstrapMethod, "bootstrapMethod is null");
    }

    @Override
    public ByteCodeNode getByteCode()
    {
        return InvokeInstruction.invokeDynamic(type.getName().replaceAll("\\W+", "_"), binding.getType(), bootstrapMethod, binding.getBindingId());
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.of();
    }

    @Override
    protected String formatOneLine()
    {
        return type.getName();
    }
}
