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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.stream.IntStream;

import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.util.CompilerUtils.defineClass;
import static com.facebook.presto.util.CompilerUtils.makeClassName;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static java.util.Collections.nCopies;

public final class FormatFunction
        extends SqlScalarFunction
{
    // TODO design new variadic functions binding mechanism that will allow to produce VARCHAR(x) where x < MAX_LENGTH.
    public static final FormatFunction VARCHAR_FORMAT = new FormatFunction(createUnboundedVarcharType().getTypeSignature(), "formats given strings");

    public static final FormatFunction VARBINARY_FORMAT = new FormatFunction(VARBINARY.getTypeSignature(), "formats given varbinary values");

    private final String description;

    private FormatFunction(TypeSignature type, String description)
    {
        super(new Signature(
                "format",
                FunctionKind.SCALAR,
                ImmutableList.of(),
                ImmutableList.of(),
                type,
                ImmutableList.of(type),
                true));
        this.description = description;
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return description;
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        if (arity < 2) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "There must be two or more arguments");
        }

        Class<?> clazz = generateFormat(getSignature().getReturnType(), arity);
        MethodHandle methodHandle = methodHandle(clazz, "format", nCopies(arity, Slice.class).toArray(new Class<?>[arity]));

        return new ScalarFunctionImplementation(
                false,
                nCopies(arity, valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle,
                isDeterministic());
    }

    private static Class<?> generateFormat(TypeSignature type, int arity)
    {
        checkCondition(arity <= 254, NOT_SUPPORTED, "Too many arguments for string format");
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(type.getBase() + "_format" + arity + "ScalarFunction"),
                type(Object.class));

        // Generate constructor
        definition.declareDefaultConstructor(a(PRIVATE));

        // Generate format()
        List<Parameter> parameters = IntStream.range(0, arity)
                .mapToObj(i -> arg("arg" + i, Slice.class))
                .collect(toImmutableList());

        MethodDefinition method = definition.declareMethod(a(PUBLIC, STATIC), "format", type(Slice.class), parameters);
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        Variable length = scope.declareVariable(int.class, "length");
        body.append(length.set(constantInt(0)));

        Variable result = scope.declareVariable(Slice.class, "result");
        body.append(result.set(invokeStatic(Slices.class, "allocate", Slice.class, length)));

        Variable position = scope.declareVariable(int.class, "position");
        body.append(position.set(constantInt(0)));

        Parameter format = parameters.get(0);
        BytecodeExpression expression = invokeStatic(FormatFunction.class, "format", Slice.class, format, parameters.get(1));

        for (int i = 2; i < arity; ++i) {
            expression = invokeStatic(FormatFunction.class, "format", Slice.class, expression, parameters.get(i));
        }

        body.append(result.set(expression));

        body.getVariable(result)
                .retObject();

        return defineClass(definition, Object.class, ImmutableMap.of(), new DynamicClassLoader(FormatFunction.class.getClassLoader()));
    }

    public static Slice format(Slice str, Slice replace)
    {
        Slice search = Slices.utf8Slice("%s");

        // Allocate a reasonable buffer
        Slice buffer = Slices.allocate(str.length());

        int index = 0;
        int indexBuffer = 0;
        int matchIndex = str.indexOf(search, index);
        // Found a match?
        if (matchIndex >= 0) {
            int bytesToCopy = matchIndex - index;
            buffer = Slices.ensureSize(buffer, indexBuffer + bytesToCopy + replace.length());
            // Non empty match?
            if (bytesToCopy > 0) {
                buffer.setBytes(indexBuffer, str, index, bytesToCopy);
                indexBuffer += bytesToCopy;
            }
            // Non empty replace?
            if (replace.length() > 0) {
                buffer.setBytes(indexBuffer, replace);
                indexBuffer += replace.length();
            }
            // Continue searching after match
            index = matchIndex + search.length();
        }

        // Copy the rest of string
        int bytesToCopy = str.length() - index;
        buffer = Slices.ensureSize(buffer, indexBuffer + bytesToCopy);
        buffer.setBytes(indexBuffer, str, index, bytesToCopy);
        indexBuffer += bytesToCopy;

        return buffer.slice(0, indexBuffer);
    }
}
