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
import com.google.common.primitives.Primitives;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockBuilderStatus;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.sql.gen.CallSiteBinder;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.equal;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.USE_BOXED_TYPE;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.sql.gen.SqlTypeBytecodeExpression.constantType;
import static io.prestosql.util.CompilerUtils.defineClass;
import static io.prestosql.util.CompilerUtils.makeClassName;
import static io.prestosql.util.Failures.checkCondition;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Collections.nCopies;

public final class ArrayConstructor
        extends SqlScalarFunction
{
    public static final ArrayConstructor ARRAY_CONSTRUCTOR = new ArrayConstructor();

    public ArrayConstructor()
    {
        super(new Signature("array_constructor",
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("E")),
                ImmutableList.of(),
                parseTypeSignature("array(E)"),
                ImmutableList.of(parseTypeSignature("E"), parseTypeSignature("E")),
                true));
    }

    @Override
    public boolean isHidden()
    {
        return true;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        // Internal function, doesn't need a description
        return null;
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Map<String, Type> types = boundVariables.getTypeVariables();
        checkArgument(types.size() == 1, "Can only construct arrays from exactly matching types");
        ImmutableList.Builder<Class<?>> builder = ImmutableList.builder();
        Type type = types.get("E");
        for (int i = 0; i < arity; i++) {
            if (type.getJavaType().isPrimitive()) {
                builder.add(Primitives.wrap(type.getJavaType()));
            }
            else {
                builder.add(type.getJavaType());
            }
        }
        ImmutableList<Class<?>> stackTypes = builder.build();
        Class<?> clazz = generateArrayConstructor(stackTypes, type);
        MethodHandle methodHandle;
        try {
            Method method = clazz.getMethod("arrayConstructor", stackTypes.toArray(new Class<?>[stackTypes.size()]));
            methodHandle = lookup().unreflect(method);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
        return new ScalarFunctionImplementation(
                false,
                nCopies(stackTypes.size(), valueTypeArgumentProperty(USE_BOXED_TYPE)),
                methodHandle,
                isDeterministic());
    }

    private static Class<?> generateArrayConstructor(List<Class<?>> stackTypes, Type elementType)
    {
        checkCondition(stackTypes.size() <= 254, NOT_SUPPORTED, "Too many arguments for array constructor");
        List<String> stackTypeNames = stackTypes.stream()
                .map(Class::getSimpleName)
                .collect(toImmutableList());

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(Joiner.on("").join(stackTypeNames) + "ArrayConstructor"),
                type(Object.class));

        // Generate constructor
        definition.declareDefaultConstructor(a(PRIVATE));

        // Generate arrayConstructor()
        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        for (int i = 0; i < stackTypes.size(); i++) {
            Class<?> stackType = stackTypes.get(i);
            parameters.add(arg("arg" + i, stackType));
        }

        MethodDefinition method = definition.declareMethod(a(PUBLIC, STATIC), "arrayConstructor", type(Block.class), parameters.build());
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        Variable blockBuilderVariable = scope.declareVariable(BlockBuilder.class, "blockBuilder");
        CallSiteBinder binder = new CallSiteBinder();

        BytecodeExpression createBlockBuilder = blockBuilderVariable.set(
                constantType(binder, elementType).invoke("createBlockBuilder", BlockBuilder.class, constantNull(BlockBuilderStatus.class), constantInt(stackTypes.size())));
        body.append(createBlockBuilder);

        for (int i = 0; i < stackTypes.size(); i++) {
            Variable argument = scope.getVariable("arg" + i);
            IfStatement ifStatement = new IfStatement()
                    .condition(equal(argument, constantNull(stackTypes.get(i))))
                    .ifTrue(blockBuilderVariable.invoke("appendNull", BlockBuilder.class).pop())
                    .ifFalse(constantType(binder, elementType).writeValue(blockBuilderVariable, argument.cast(elementType.getJavaType())));
            body.append(ifStatement);
        }

        body.append(blockBuilderVariable.invoke("build", Block.class).ret());

        return defineClass(definition, Object.class, binder.getBindings(), new DynamicClassLoader(ArrayConstructor.class.getClassLoader()));
    }
}
