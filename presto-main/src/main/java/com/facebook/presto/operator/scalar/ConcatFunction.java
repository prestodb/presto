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

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.expression.BytecodeExpressions;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.stream.IntStream;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.util.CompilerUtils.defineClass;
import static com.facebook.presto.util.CompilerUtils.makeClassName;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.nCopies;

public final class ConcatFunction
        extends SqlScalarFunction
{
    // TODO design new variadic functions binding mechanism that will allow to produce VARCHAR(x) where x < MAX_LENGTH.
    public static final ConcatFunction VARCHAR_CONCAT = new ConcatFunction(createUnboundedVarcharType().getTypeSignature(), "concatenates given strings");

    public static final ConcatFunction VARBINARY_CONCAT = new ConcatFunction(VARBINARY.getTypeSignature(), "concatenates given varbinary values");

    private final String description;

    private ConcatFunction(TypeSignature type, String description)
    {
        super(new Signature(
                QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "concat"),
                FunctionKind.SCALAR,
                ImmutableList.of(),
                ImmutableList.of(),
                type,
                ImmutableList.of(type),
                true));
        this.description = description;
    }

    @Override
    public SqlFunctionVisibility getVisibility()
    {
        return SqlFunctionVisibility.PUBLIC;
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
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        if (arity < 2) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "There must be two or more concatenation arguments");
        }

        Class<?> clazz = generateConcat(getSignature().getReturnType(), arity);
        MethodHandle methodHandle = methodHandle(clazz, "concat", nCopies(arity, Slice.class).toArray(new Class<?>[arity]));

        return new BuiltInScalarFunctionImplementation(
                false,
                nCopies(arity, valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Slice concat(Slice... slices)
    {
        int i;
        for (i = 0; i < slices.length; i++) {
            if (slices[i].length() > 0) {
                break;
            }
        }

        if (i == slices.length) {
            return Slices.EMPTY_SLICE;
        }

        int start = i;
        Slice firstNonEmpty = slices[i];
        int length = 0;
        for (; i < slices.length; i++) {
            Slice slice = slices[i];
            if (slice.length() > 0) {
                length += slice.length();
                if (length < 0) {
                    return resultTooLarge();
                }
            }
        }

        if (length == firstNonEmpty.length()) {
            // return the only non-empty slice
            return firstNonEmpty;
        }

        int position = 0;
        Slice result = Slices.allocate(length);
        for (i = start; i < slices.length; i++) {
            Slice slice = slices[i];
            if (slice.length() > 0) {
                result.setBytes(position, slice);
                position += slice.length();
            }
        }

        return result;
    }

    private static Class<?> generateConcat(TypeSignature type, int arity)
    {
        checkCondition(arity <= 254, NOT_SUPPORTED, "Too many arguments for string concatenation");
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(type.getBase() + "_concat" + arity + "ScalarFunction"),
                type(Object.class));

        // Generate constructor
        definition.declareDefaultConstructor(a(PRIVATE));

        // Generate concat()
        List<Parameter> parameters = IntStream.range(0, arity)
                .mapToObj(i -> arg("arg" + i, Slice.class))
                .collect(toImmutableList());

        MethodDefinition method = definition.declareMethod(a(PUBLIC, STATIC), "concat", type(Slice.class), parameters);
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();
        Variable result = scope.declareVariable(Slice.class, "result");
        body.append(invokeStatic(ConcatFunction.class, "concat", Slice.class,
                BytecodeExpressions.newArray(ParameterizedType.type(Slice[].class), parameters)))
                .retObject();
        return defineClass(definition, Object.class, ImmutableMap.of(), new DynamicClassLoader(ConcatFunction.class.getClassLoader()));
    }

    @UsedByGeneratedCode
    public static Slice resultTooLarge()
    {
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Concatenated string is too large");
    }
}
