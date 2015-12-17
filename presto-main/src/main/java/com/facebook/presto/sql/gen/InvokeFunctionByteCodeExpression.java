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
import com.facebook.presto.byteCode.FieldDefinition;
import com.facebook.presto.byteCode.MethodGenerationContext;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.expression.ByteCodeExpression;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.sql.gen.ByteCodeUtils.generateInvocation;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static java.util.Objects.requireNonNull;

public class InvokeFunctionByteCodeExpression
        extends ByteCodeExpression
{
    public static ByteCodeExpression invokeFunction(Scope scope, CachedInstanceBinder cachedInstanceBinder, String name, ScalarFunctionImplementation function, ByteCodeExpression... parameters)
    {
        return invokeFunction(scope, cachedInstanceBinder, name, function, ImmutableList.copyOf(parameters));
    }

    public static ByteCodeExpression invokeFunction(Scope scope, CachedInstanceBinder cachedInstanceBinder, String name, ScalarFunctionImplementation function, List<ByteCodeExpression> parameters)
    {
        requireNonNull(scope, "scope is null");
        requireNonNull(function, "function is null");

        Binding binding = cachedInstanceBinder.getCallSiteBinder().bind(function.getMethodHandle());
        Optional<ByteCodeNode> instance = Optional.empty();
        if (function.getInstanceFactory().isPresent()) {
            FieldDefinition field = cachedInstanceBinder.getCachedInstance(function.getInstanceFactory().get());
            instance = Optional.of(scope.getThis().getField(field));
        }
        return new InvokeFunctionByteCodeExpression(scope, binding, name, function, instance, parameters);
    }

    private final ByteCodeNode invocation;
    private final String oneLineDescription;

    private InvokeFunctionByteCodeExpression(
            Scope scope,
            Binding binding,
            String name,
            ScalarFunctionImplementation function,
            Optional<ByteCodeNode> instance,
            List<ByteCodeExpression> parameters)
    {
        super(type(function.getMethodHandle().type().returnType()));

        this.invocation = generateInvocation(scope, name, function, instance, parameters.stream().map(ByteCodeNode.class::cast).collect(toImmutableList()), binding);
        this.oneLineDescription = name + "(" + Joiner.on(", ").join(parameters) + ")";
    }

    @Override
    public ByteCodeNode getByteCode(MethodGenerationContext generationContext)
    {
        return invocation;
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.of();
    }

    @Override
    protected String formatOneLine()
    {
        return oneLineDescription;
    }
}
