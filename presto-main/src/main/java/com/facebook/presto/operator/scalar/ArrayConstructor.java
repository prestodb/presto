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
import com.facebook.presto.bytecode.CallSiteBinder;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockBuilderStatus;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantNull;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.equal;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.USE_BOXED_TYPE;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.HIDDEN;
import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;
import static com.facebook.presto.util.CompilerUtils.defineClass;
import static com.facebook.presto.util.CompilerUtils.makeClassName;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Collections.nCopies;

public final class ArrayConstructor
        extends SqlScalarFunction
{
    public static final ArrayConstructor ARRAY_CONSTRUCTOR = new ArrayConstructor();

    public ArrayConstructor()
    {
        super(new Signature(QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "array_constructor"),
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("E")),
                ImmutableList.of(),
                parseTypeSignature("array(E)"),
                ImmutableList.of(parseTypeSignature("E"), parseTypeSignature("E")),
                true));
    }

    @Override
    public SqlFunctionVisibility getVisibility()
    {
        return HIDDEN;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public boolean isCalledOnNullInput()
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
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
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
        return new BuiltInScalarFunctionImplementation(
                false,
                nCopies(stackTypes.size(), valueTypeArgumentProperty(USE_BOXED_TYPE)),
                methodHandle);
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
