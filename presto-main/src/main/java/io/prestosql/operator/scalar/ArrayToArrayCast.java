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
package io.prestosql.operator.scalar;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlOperator;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.sql.gen.ArrayGeneratorUtils;
import io.prestosql.sql.gen.ArrayMapBytecodeExpression;
import io.prestosql.sql.gen.CachedInstanceBinder;
import io.prestosql.sql.gen.CallSiteBinder;

import java.lang.invoke.MethodHandle;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantBoolean;
import static io.prestosql.metadata.Signature.internalOperator;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.util.CompilerUtils.defineClass;
import static io.prestosql.util.CompilerUtils.makeClassName;
import static io.prestosql.util.Reflection.methodHandle;

public class ArrayToArrayCast
        extends SqlOperator
{
    public static final ArrayToArrayCast ARRAY_TO_ARRAY_CAST = new ArrayToArrayCast();

    private ArrayToArrayCast()
    {
        super(CAST,
                ImmutableList.of(typeVariable("F"), typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature("array(T)"),
                ImmutableList.of(parseTypeSignature("array(F)")));
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(arity == 1, "Expected arity to be 1");
        Type fromType = boundVariables.getTypeVariable("F");
        Type toType = boundVariables.getTypeVariable("T");

        Signature signature = internalOperator(CAST.name(), toType.getTypeSignature(), ImmutableList.of(fromType.getTypeSignature()));
        ScalarFunctionImplementation function = functionRegistry.getScalarFunctionImplementation(signature);
        Class<?> castOperatorClass = generateArrayCast(typeManager, signature, function);
        MethodHandle methodHandle = methodHandle(castOperatorClass, "castArray", ConnectorSession.class, Block.class);
        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle,
                isDeterministic());
    }

    private static Class<?> generateArrayCast(TypeManager typeManager, Signature elementCastSignature, ScalarFunctionImplementation elementCast)
    {
        CallSiteBinder binder = new CallSiteBinder();

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(Joiner.on("$").join("ArrayCast", elementCastSignature.getArgumentTypes().get(0), elementCastSignature.getReturnType())),
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
        BytecodeBlock body = method.getBody();

        Variable wasNull = scope.declareVariable(boolean.class, "wasNull");
        body.append(wasNull.set(constantBoolean(false)));

        // cast map elements
        Type fromElementType = typeManager.getType(elementCastSignature.getArgumentTypes().get(0));
        Type toElementType = typeManager.getType(elementCastSignature.getReturnType());
        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(definition, binder);
        ArrayMapBytecodeExpression newArray = ArrayGeneratorUtils.map(scope, cachedInstanceBinder, fromElementType, toElementType, value, elementCastSignature.getName(), elementCast);

        // return the block
        body.append(newArray.ret());

        MethodDefinition constructorDefinition = definition.declareConstructor(a(PUBLIC));
        BytecodeBlock constructorBody = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();
        constructorBody.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);
        cachedInstanceBinder.generateInitializations(thisVariable, constructorBody);
        constructorBody.ret();

        return defineClass(definition, Object.class, binder.getBindings(), ArrayToArrayCast.class.getClassLoader());
    }
}
