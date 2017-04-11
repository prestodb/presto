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

import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodGenerationContext;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.sql.gen.BytecodeUtils.generateInvocation;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class InvokeFunctionBytecodeExpression
        extends BytecodeExpression
{
    public static BytecodeExpression invokeFunction(Scope scope, CachedInstanceBinder cachedInstanceBinder, String name, ScalarFunctionImplementation function, BytecodeExpression... parameters)
    {
        return invokeFunction(scope, cachedInstanceBinder, name, function, ImmutableList.copyOf(parameters));
    }

    public static BytecodeExpression invokeFunction(Scope scope, CachedInstanceBinder cachedInstanceBinder, String name, ScalarFunctionImplementation function, List<BytecodeExpression> parameters)
    {
        requireNonNull(scope, "scope is null");
        requireNonNull(function, "function is null");

        Binding binding = cachedInstanceBinder.getCallSiteBinder().bind(function.getMethodHandle());
        Optional<BytecodeNode> instance = Optional.empty();
        if (function.getInstanceFactory().isPresent()) {
            FieldDefinition field = cachedInstanceBinder.getCachedInstance(function.getInstanceFactory().get());
            instance = Optional.of(scope.getThis().getField(field));
        }
        return new InvokeFunctionBytecodeExpression(scope, binding, name, function, instance, parameters);
    }

    private final BytecodeNode invocation;
    private final String oneLineDescription;

    private InvokeFunctionBytecodeExpression(
            Scope scope,
            Binding binding,
            String name,
            ScalarFunctionImplementation function,
            Optional<BytecodeNode> instance,
            List<BytecodeExpression> parameters)
    {
        super(type(function.getMethodHandle().type().returnType()));

        this.invocation = generateInvocation(scope, name, function, instance, parameters.stream().map(BytecodeNode.class::cast).collect(toImmutableList()), binding);
        this.oneLineDescription = name + "(" + Joiner.on(", ").join(parameters) + ")";
    }

    @Override
    public BytecodeNode getBytecode(MethodGenerationContext generationContext)
    {
        return invocation;
    }

    @Override
    public List<BytecodeNode> getChildNodes()
    {
        return ImmutableList.of();
    }

    @Override
    protected String formatOneLine()
    {
        return oneLineDescription;
    }
}
