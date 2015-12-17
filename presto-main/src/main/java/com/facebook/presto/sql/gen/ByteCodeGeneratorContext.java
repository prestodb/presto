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
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.sql.relational.RowExpression;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.gen.ByteCodeUtils.generateInvocation;
import static java.util.Objects.requireNonNull;

public class ByteCodeGeneratorContext
{
    private final ByteCodeExpressionVisitor byteCodeGenerator;
    private final Scope scope;
    private final CallSiteBinder callSiteBinder;
    private final CachedInstanceBinder cachedInstanceBinder;
    private final FunctionRegistry registry;
    private final Variable wasNull;

    public ByteCodeGeneratorContext(
            ByteCodeExpressionVisitor byteCodeGenerator,
            Scope scope,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            FunctionRegistry registry)
    {
        requireNonNull(byteCodeGenerator, "byteCodeGenerator is null");
        requireNonNull(cachedInstanceBinder, "cachedInstanceBinder is null");
        requireNonNull(scope, "scope is null");
        requireNonNull(callSiteBinder, "callSiteBinder is null");
        requireNonNull(registry, "registry is null");

        this.byteCodeGenerator = byteCodeGenerator;
        this.scope = scope;
        this.callSiteBinder = callSiteBinder;
        this.cachedInstanceBinder = cachedInstanceBinder;
        this.registry = registry;
        this.wasNull = scope.getVariable("wasNull");
    }

    public Scope getScope()
    {
        return scope;
    }

    public CallSiteBinder getCallSiteBinder()
    {
        return callSiteBinder;
    }

    public ByteCodeNode generate(RowExpression expression)
    {
        return expression.accept(byteCodeGenerator, scope);
    }

    public FunctionRegistry getRegistry()
    {
        return registry;
    }

    /**
     * Generates a function call with null handling, automatic binding of session parameter, etc.
     */
    public ByteCodeNode generateCall(String name, ScalarFunctionImplementation function, List<ByteCodeNode> arguments)
    {
        Binding binding = callSiteBinder.bind(function.getMethodHandle());
        Optional<ByteCodeNode> instance = Optional.empty();
        if (function.getInstanceFactory().isPresent()) {
            FieldDefinition field = cachedInstanceBinder.getCachedInstance(function.getInstanceFactory().get());
            instance = Optional.of(scope.getThis().getField(field));
        }
        return generateInvocation(scope, name, function, instance, arguments, binding);
    }

    public Variable wasNull()
    {
        return wasNull;
    }
}
