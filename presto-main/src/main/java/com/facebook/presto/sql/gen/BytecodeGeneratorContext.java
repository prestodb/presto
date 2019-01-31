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

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.relation.column.ColumnExpression;
import com.facebook.presto.sql.gen.BytecodeUtils.OutputBlockVariableAndType;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.gen.BytecodeUtils.generateInvocation;
import static java.util.Objects.requireNonNull;

public class BytecodeGeneratorContext
{
    private final ColumnExpressionCompiler columnExpressionCompiler;
    private final Scope scope;
    private final CallSiteBinder callSiteBinder;
    private final CachedInstanceBinder cachedInstanceBinder;
    private final FunctionRegistry registry;
    private final Variable wasNull;

    public BytecodeGeneratorContext(
            ColumnExpressionCompiler columnExpressionCompiler,
            Scope scope,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            FunctionRegistry registry)
    {
        requireNonNull(columnExpressionCompiler, "bytecodeGenerator is null");
        requireNonNull(cachedInstanceBinder, "cachedInstanceBinder is null");
        requireNonNull(scope, "scope is null");
        requireNonNull(callSiteBinder, "callSiteBinder is null");
        requireNonNull(registry, "registry is null");

        this.columnExpressionCompiler = columnExpressionCompiler;
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

    public BytecodeNode generate(ColumnExpression expression, Optional<Variable> outputBlockVariable)
    {
        return generate(expression, outputBlockVariable, Optional.empty());
    }

    public BytecodeNode generate(ColumnExpression expression, Optional<Variable> outputBlockVariable, Optional<Class> lambdaInterface)
    {
        return columnExpressionCompiler.compile(expression, scope, outputBlockVariable, lambdaInterface);
    }

    public FunctionRegistry getRegistry()
    {
        return registry;
    }

    public BytecodeNode generateCall(String name, ScalarFunctionImplementation function, List<BytecodeNode> arguments)
    {
        return generateCall(name, function, arguments, Optional.empty());
    }

    /**
     * Generates a function call with null handling, automatic binding of session parameter, etc.
     */
    public BytecodeNode generateCall(
            String name,
            ScalarFunctionImplementation function,
            List<BytecodeNode> arguments,
            Optional<OutputBlockVariableAndType> outputBlockVariableAndType)
    {
        Optional<BytecodeNode> instance = Optional.empty();
        if (function.getInstanceFactory().isPresent()) {
            FieldDefinition field = cachedInstanceBinder.getCachedInstance(function.getInstanceFactory().get());
            instance = Optional.of(scope.getThis().getField(field));
        }
        return generateInvocation(scope, name, function, instance, arguments, callSiteBinder, outputBlockVariableAndType);
    }

    public Variable wasNull()
    {
        return wasNull;
    }
}
