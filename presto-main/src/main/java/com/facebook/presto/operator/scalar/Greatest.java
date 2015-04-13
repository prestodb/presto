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

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.Parameter;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricScalar;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.CompilerOperations;
import com.facebook.presto.sql.gen.CompilerUtils;
import com.facebook.presto.util.ImmutableCollectors;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.Parameter.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.metadata.Signature.internalFunction;
import static com.facebook.presto.metadata.Signature.orderableTypeParameter;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.facebook.presto.sql.gen.SqlTypeByteCodeExpression.constantType;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public final class Greatest
        extends ParametricScalar
{
    public static final Greatest GREATEST = new Greatest();
    private static final Signature SIGNATURE = new Signature("greatest", ImmutableList.of(orderableTypeParameter("E")), "E", ImmutableList.of("E"), true, false);
    public static final int EXPECTED_ELEMENT_SIZE = 32;

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
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
        return "get the largest of the given values";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = types.get("E");
        checkArgument(type.isOrderable(), "Type must be orderable");

        ImmutableList.Builder<Class<?>> builder = ImmutableList.builder();
        for (int i = 0; i < arity; i++) {
            builder.add(type.getJavaType());
        }

        ImmutableList<Class<?>> stackTypes = builder.build();
        Class<?> clazz = generateGreatest(stackTypes, type);
        MethodHandle methodHandle = methodHandle(clazz, "greatest", stackTypes.toArray(new Class<?>[stackTypes.size()]));
        List<Boolean> nullableParameters = ImmutableList.copyOf(Collections.nCopies(stackTypes.size(), false));

        Signature specializedSignature = internalFunction(SIGNATURE.getName(), type.getTypeSignature(), Collections.nCopies(arity, type.getTypeSignature()));
        return new FunctionInfo(specializedSignature, getDescription(), isHidden(), methodHandle, isDeterministic(), false, nullableParameters);
    }

    public static void checkNotNaN(double value)
    {
        if (Double.isNaN(value)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid argument to greatest(): NaN");
        }
    }

    private static Class<?> generateGreatest(List<Class<?>> nativeContainerTypes, Type type)
    {
        List<String> nativeContainerTypeNames = nativeContainerTypes.stream().map(Class::getSimpleName).collect(ImmutableCollectors.toImmutableList());

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName(Joiner.on("").join(nativeContainerTypeNames) + "Greatest"),
                type(Object.class));

        definition.declareDefaultConstructor(a(PRIVATE));

        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        for (int i = 0; i < nativeContainerTypes.size(); i++) {
            Class<?> nativeContainerType = nativeContainerTypes.get(i);
            parameters.add(arg("arg" + i, nativeContainerType));
        }

        MethodDefinition method = definition.declareMethod(a(PUBLIC, STATIC), "greatest", type(nativeContainerTypes.get(0)), parameters.build());
        Scope scope = method.getScope();
        Block body = method.getBody();

        Variable typeVariable = scope.declareVariable(Type.class, "typeVariable");
        CallSiteBinder binder = new CallSiteBinder();
        body.comment("typeVariable = type;")
                .append(constantType(binder, type))
                .putVariable(typeVariable);

        for (int i = 0; i < nativeContainerTypes.size(); i++) {
            Class<?> nativeContainerType = nativeContainerTypes.get(i);
            Variable currentBlock = scope.declareVariable(com.facebook.presto.spi.block.Block.class, "block" + i);
            Variable blockBuilder = scope.declareVariable(BlockBuilder.class, "blockBuilder" + i);
            Block buildBlock = new Block()
                    .comment("blockBuilder%d = typeVariable.createBlockBuilder(new BlockBuilderStatus(), 1, EXPECTED_ELEMENT_SIZE);", i)
                    .getVariable(typeVariable)
                    .newObject(BlockBuilderStatus.class)
                    .dup()
                    .invokeConstructor(BlockBuilderStatus.class)
                    .push(1)
                    .push(EXPECTED_ELEMENT_SIZE)
                    .invokeInterface(Type.class, "createBlockBuilder", BlockBuilder.class, BlockBuilderStatus.class, int.class, int.class)
                    .putVariable(blockBuilder);

            String writeMethodName;
            if (nativeContainerType == long.class) {
                writeMethodName = "writeLong";
            }
            else if (nativeContainerType == boolean.class) {
                writeMethodName = "writeBoolean";
            }
            else if (nativeContainerType == double.class) {
                writeMethodName = "writeDouble";
            }
            else if (nativeContainerType == Slice.class) {
                writeMethodName = "writeSlice";
            }
            else {
                throw new PrestoException(INTERNAL_ERROR, format("Unexpected type %s", nativeContainerType.getName()));
            }

            if (type.getTypeSignature().getBase().equals(StandardTypes.DOUBLE)) {
                buildBlock
                        .append(scope.getVariable("arg" + i))
                        .invokeStatic(Greatest.class, "checkNotNaN", void.class, double.class);
            }

            Block writeBlock = new Block()
                    .comment("typeVariable.%s(blockBuilder%d, arg%d);", writeMethodName, i, i)
                    .getVariable(typeVariable)
                    .getVariable(blockBuilder)
                    .append(scope.getVariable("arg" + i))
                    .invokeInterface(Type.class, writeMethodName, void.class, BlockBuilder.class, nativeContainerType);

            buildBlock.append(writeBlock);

            Block storeBlock = new Block()
                    .comment("block%d = blockBuilder%d.build();", i, i)
                    .getVariable(blockBuilder)
                    .invokeInterface(BlockBuilder.class, "build", com.facebook.presto.spi.block.Block.class)
                    .putVariable(currentBlock);
            buildBlock.append(storeBlock);
            body.append(buildBlock);
        }

        Variable greatestVariable = scope.declareVariable(nativeContainerTypes.get(0), "greatest");
        Variable greatestBlockVariable = scope.declareVariable(com.facebook.presto.spi.block.Block.class, "greatestBlock");

        body.comment("greatest = arg0; greatestBlock = block0;")
                .append(scope.getVariable("arg0"))
                .putVariable(greatestVariable)
                .append(scope.getVariable("block0"))
                .putVariable(greatestBlockVariable);

        for (int i = 1; i < nativeContainerTypes.size(); i++) {
            IfStatement ifStatement = new IfStatement("if (type.compareTo(greatestBlock, 0, block" + i + ", 0) < 0)");

            ifStatement.condition()
                    .getVariable(typeVariable)
                    .getVariable(greatestBlockVariable)
                    .push(0)
                    .append(scope.getVariable("block" + i))
                    .push(0)
                    .invokeInterface(Type.class, "compareTo", int.class, com.facebook.presto.spi.block.Block.class, int.class, com.facebook.presto.spi.block.Block.class, int.class)
                    .push(0)
                    .invokeStatic(CompilerOperations.class, "greaterThan", boolean.class, int.class, int.class);

            ifStatement.ifFalse()
                    .append(scope.getVariable("arg" + i))
                    .putVariable(greatestVariable)
                    .append(scope.getVariable("block" + i))
                    .putVariable(greatestBlockVariable);

            body.append(ifStatement);
        }

        body.comment("return greatest;")
                    .getVariable(greatestVariable)
                    .ret(nativeContainerTypes.get(0));

        return defineClass(definition, Object.class, binder.getBindings(), new DynamicClassLoader(Greatest.class.getClassLoader()));
    }
}
