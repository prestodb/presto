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

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.CompilerUtils;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.CompilerUtils.defineClass;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantNull;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.equal;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newInstance;
import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.invoke.MethodHandles.lookup;

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
            throw Throwables.propagate(e);
        }
        List<Boolean> nullableParameters = ImmutableList.copyOf(Collections.nCopies(stackTypes.size(), true));
        return new ScalarFunctionImplementation(false, nullableParameters, methodHandle, isDeterministic());
    }

    private static Class<?> generateArrayConstructor(List<Class<?>> stackTypes, Type elementType)
    {
        List<String> stackTypeNames = stackTypes.stream()
                .map(Class::getSimpleName)
                .collect(toImmutableList());

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName(Joiner.on("").join(stackTypeNames) + "ArrayConstructor"),
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
                constantType(binder, elementType).invoke("createBlockBuilder", BlockBuilder.class, newInstance(BlockBuilderStatus.class), constantInt(stackTypes.size())));
        body.append(createBlockBuilder);

        for (int i = 0; i < stackTypes.size(); i++) {
            if (elementType.getJavaType() == void.class) {
                body.append(blockBuilderVariable.invoke("appendNull", BlockBuilder.class));
            }
            else {
                Variable argument = scope.getVariable("arg" + i);
                IfStatement ifStatement = new IfStatement()
                        .condition(equal(argument, constantNull(stackTypes.get(i))))
                        .ifTrue(blockBuilderVariable.invoke("appendNull", BlockBuilder.class).pop())
                        .ifFalse(constantType(binder, elementType).writeValue(blockBuilderVariable, argument.cast(elementType.getJavaType())));
                body.append(ifStatement);
            }
        }

        body.append(blockBuilderVariable.invoke("build", Block.class).ret());

        return defineClass(definition, Object.class, binder.getBindings(), new DynamicClassLoader(ArrayConstructor.class.getClassLoader()));
    }
}
