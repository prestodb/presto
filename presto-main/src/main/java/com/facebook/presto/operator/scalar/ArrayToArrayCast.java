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

import com.facebook.presto.byteCode.ByteCodeBlock;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.Parameter;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.gen.ArrayGeneratorUtils;
import com.facebook.presto.sql.gen.ArrayMapByteCodeExpression;
import com.facebook.presto.sql.gen.CachedInstanceBinder;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.CompilerUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.Parameter.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantBoolean;
import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;

public class ArrayToArrayCast
        extends SqlOperator
{
    public static final ArrayToArrayCast ARRAY_TO_ARRAY_CAST = new ArrayToArrayCast();

    private ArrayToArrayCast()
    {
        super(CAST, ImmutableList.of(typeParameter("F"), typeParameter("T")), "array<T>", ImmutableList.of("array<F>"));
    }

    @Override
    public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(arity == 1, "Expected arity to be 1");
        Type fromType = types.get("F");
        Type toType = types.get("T");

        Signature signature = internalOperator(CAST.name(), toType.getTypeSignature(), ImmutableList.of(fromType.getTypeSignature()));
        ScalarFunctionImplementation function = functionRegistry.getScalarFunctionImplementation(signature);
        Class<?> castOperatorClass = generateArrayCast(typeManager, signature, function);
        MethodHandle methodHandle = methodHandle(castOperatorClass, "castArray", ConnectorSession.class, Block.class);
        return new ScalarFunctionImplementation(false, ImmutableList.of(false), methodHandle, isDeterministic());
    }

    private static Class<?> generateArrayCast(TypeManager typeManager, Signature elementCastSignature, ScalarFunctionImplementation elementCast)
    {
        CallSiteBinder binder = new CallSiteBinder();

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName(Joiner.on("$").join("ArrayCast", elementCastSignature.getArgumentTypes().get(0), elementCastSignature.getReturnType())),
                type(Object.class));

        Parameter session = arg("session", ConnectorSession.class);
        Parameter value = arg("value", Block.class);

        MethodDefinition method = definition.declareMethod(
                a(PUBLIC, STATIC),
                "castArray",
                type(Block.class),
                session,
                value);

        Scope scope = method.getScope();
        ByteCodeBlock body = method.getBody();

        Variable wasNull = scope.declareVariable(boolean.class, "wasNull");
        body.append(wasNull.set(constantBoolean(false)));

        // cast map elements
        Type fromElementType = typeManager.getType(elementCastSignature.getArgumentTypes().get(0));
        Type toElementType = typeManager.getType(elementCastSignature.getReturnType());
        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(definition, binder);
        ArrayMapByteCodeExpression newArray = ArrayGeneratorUtils.map(scope, cachedInstanceBinder, fromElementType, toElementType, value, elementCastSignature.getName(), elementCast);

        // return the block
        body.append(newArray.ret());

        MethodDefinition constructorDefinition = definition.declareConstructor(a(PUBLIC));
        ByteCodeBlock constructorBody = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();
        constructorBody.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);
        cachedInstanceBinder.generateInitializations(thisVariable, constructorBody);
        constructorBody.ret();

        return defineClass(definition, Object.class, binder.getBindings(), ArrayToArrayCast.class.getClassLoader());
    }
}
