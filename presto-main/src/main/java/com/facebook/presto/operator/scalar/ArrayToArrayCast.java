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

import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.Parameter;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricOperator;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.gen.ArrayGeneratorUtils;
import com.facebook.presto.sql.gen.ArrayMapByteCodeExpression;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.CompilerUtils;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.TypeUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.Parameter.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantBoolean;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.invokeStatic;
import static com.facebook.presto.metadata.FunctionRegistry.operatorInfo;
import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class ArrayToArrayCast
        extends ParametricOperator
{
    public static final ArrayToArrayCast ARRAY_TO_ARRAY_CAST = new ArrayToArrayCast();

    private ArrayToArrayCast()
    {
        super(CAST, ImmutableList.of(typeParameter("F"), typeParameter("T")), "array<T>", ImmutableList.of("array<F>"));
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(arity == 1, "Expected arity to be 1");
        Type fromType = types.get("F");
        Type toType = types.get("T");

        ArrayType fromArrayType = (ArrayType) typeManager.getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(fromType.getTypeSignature()), ImmutableList.of());
        ArrayType toArrayType = (ArrayType) typeManager.getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(toType.getTypeSignature()), ImmutableList.of());

        FunctionInfo functionInfo = functionRegistry.getExactFunction(internalOperator(CAST.name(), toType.getTypeSignature(), ImmutableList.of(fromType.getTypeSignature())));
        if (functionInfo == null) {
            throw new PrestoException(FUNCTION_NOT_FOUND, format("Can not cast %s to %s", fromArrayType, toArrayType));
        }

        Class<?> castOperatorClass = generateArrayCast(typeManager, functionInfo);
        MethodHandle methodHandle = methodHandle(castOperatorClass, "castArray", ConnectorSession.class, Slice.class);

        return operatorInfo(CAST, toArrayType.getTypeSignature(), ImmutableList.of(fromArrayType.getTypeSignature()), methodHandle, false, ImmutableList.of(false));
    }

    private static Class<?> generateArrayCast(TypeManager typeManager, FunctionInfo elementCast)
    {
        CallSiteBinder binder = new CallSiteBinder();

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName(Joiner.on("$").join("ArrayCast", elementCast.getArgumentTypes().get(0), elementCast.getReturnType())),
                type(Object.class));

        definition.declareDefaultConstructor(a(PRIVATE));

        Parameter session = arg("session", ConnectorSession.class);
        Parameter value = arg("value", Slice.class);

        MethodDefinition method = definition.declareMethod(
                a(PUBLIC, STATIC),
                "castArray",
                type(Slice.class),
                session,
                value);

        Scope scope = method.getScope();
        com.facebook.presto.byteCode.Block body = method.getBody();

        Variable wasNull = scope.declareVariable(boolean.class, "wasNull");
        body.append(wasNull.set(constantBoolean(false)));

        Variable array = scope.declareVariable(Block.class, "array");
        body.append(array.set(invokeStatic(TypeUtils.class, "readStructuralBlock", Block.class, value)));

        // cast map elements
        ArrayMapByteCodeExpression newArray = ArrayGeneratorUtils.map(scope, binder, typeManager, array, elementCast);

        // convert block to slice
        body.append(invokeStatic(TypeUtils.class, "buildStructuralSlice", Slice.class, newArray).ret());

        return defineClass(definition, Object.class, binder.getBindings(), ArrayToArrayCast.class.getClassLoader());
    }
}
