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

import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.NamedParameterDefinition;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricScalar;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.util.ImmutableCollectors;
import com.facebook.presto.sql.gen.Bootstrap;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.google.common.base.Joiner;
import com.facebook.presto.sql.gen.CompilerUtils;
import com.facebook.presto.type.BigintOperators;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.NamedParameterDefinition.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.facebook.presto.sql.gen.SqlTypeByteCodeExpression.constantType;
import static com.facebook.presto.metadata.Signature.internalFunction;
import static java.lang.String.format;

public final class Hash
        extends ParametricScalar
{
    public static final Hash HASH = new Hash();
    private static final Signature SIGNATURE = new Signature("hash", ImmutableList.of(comparableTypeParameter("bigint"), typeParameter("E")), "bigint", ImmutableList.of("bigint", "E"), true, false);

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
        return "get the hash value within the given range for variable no. of arguments of any type";
    }

    public static void checkNotNaN(double value)
    {
        if (Double.isNaN(value)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid argument to hash(): NaN");
        }
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type rangeType = types.get("bigint");
        Type type = types.get("E");

        // the argument need not be orderable, so no orderable check

        ImmutableList.Builder<Class<?>> builder = ImmutableList.builder();
        builder.add(rangeType.getJavaType());
        for (int i = 1; i < arity; i++) {
            builder.add(type.getJavaType());
        }

        ImmutableList<Class<?>> stackTypes = builder.build();
        Class<?> clazz = generateHash(stackTypes, rangeType, type);
        MethodHandle methodHandle = methodHandle(clazz, "hash", stackTypes.toArray(new Class<?>[stackTypes.size()]));
        List<Boolean> nullableParameters = ImmutableList.copyOf(Collections.nCopies(stackTypes.size(), false));

        ImmutableList.Builder<TypeSignature> typesigBuilder = ImmutableList.builder();
        typesigBuilder.add(rangeType.getTypeSignature());
        for (int i = 1; i < arity; i++) {
            typesigBuilder.add(type.getTypeSignature());
        }
        ImmutableList<TypeSignature> typesigList = typesigBuilder.build();
        Signature specializedSignature = internalFunction(SIGNATURE.getName(), BIGINT.getTypeSignature(), typesigList);
        return new FunctionInfo(specializedSignature, getDescription(), isHidden(), methodHandle, isDeterministic(), false, nullableParameters);
    }

    public static Class<?> generateHash(List<Class<?>> nativeContainerTypes, Type rangeType, Type type)
    {
        List<String> nativeContainerTypeNames = nativeContainerTypes.stream().map(Class::getSimpleName).collect(ImmutableCollectors.toImmutableList());
        CompilerContext context = new CompilerContext(Bootstrap.BOOTSTRAP_METHOD);
        ClassDefinition definition = new ClassDefinition(
                context,
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName(Joiner.on("").join(nativeContainerTypeNames) + "Hash"),
                type(Object.class));

        definition.declareDefaultConstructor(a(PRIVATE));

        ImmutableList.Builder<NamedParameterDefinition> parameters = ImmutableList.builder();
        for (int i = 0; i < nativeContainerTypes.size(); i++) {
            Class<?> nativeContainerType = nativeContainerTypes.get(i);
            parameters.add(arg("arg" + i, nativeContainerType));
        }

        Block body = definition.declareMethod(context, a(PUBLIC, STATIC), "hash", type(nativeContainerTypes.get(0)), parameters.build())
                .getBody();

        Variable typeVariable = context.declareVariable(Type.class, "typeVariable");
        Variable rangeTypeVariable = context.declareVariable(Type.class, "rangeTypeVariable");
        CallSiteBinder binder = new CallSiteBinder();

        body.comment("rangeTypeVariable = rangeType")
                .append(constantType(context, binder, rangeType))
                .putVariable(rangeTypeVariable);

        body.comment("typeVariable = type;")
                .append(constantType(context, binder, type))
                .putVariable(typeVariable);

        for (int i = 0; i < nativeContainerTypes.size(); i++) {
            Class<?> nativeContainerType = nativeContainerTypes.get(i);
            Variable currentBlock = context.declareVariable(com.facebook.presto.spi.block.Block.class, "block" + i);
            Variable blockBuilder = context.declareVariable(BlockBuilder.class, "blockBuilder" + i);
            Block buildBlock = new Block(context)
                    .comment("blockBuilder%d = typeVariable.createBlockBuilder(new BlockBuilderStatus());", i)
                    .getVariable(i == 0 ? rangeTypeVariable : typeVariable)
                    .newObject(BlockBuilderStatus.class)
                    .dup()
                    .invokeConstructor(BlockBuilderStatus.class)
                    .invokeInterface(Type.class, "createBlockBuilder", BlockBuilder.class, BlockBuilderStatus.class)
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

            if (i > 0 && type.getTypeSignature().getBase().equals(StandardTypes.DOUBLE)) {
                buildBlock.comment("arg1 != NaN")
                        .getVariable("arg" + i)
                        .invokeStatic(Hash.class, "checkNotNaN", void.class, double.class);
            }

            Block writeBlock = new Block(context)
                    .comment("typeVariable.%s(blockBuilder%d, arg%d);", writeMethodName, i, i)
                    .getVariable(i == 0 ? rangeTypeVariable : typeVariable)
                    .getVariable(blockBuilder)
                    .getVariable("arg" + i)
                    .invokeInterface(Type.class, writeMethodName, void.class, BlockBuilder.class, nativeContainerType);

            buildBlock.append(writeBlock);

            Block storeBlock = new Block(context)
                    .comment("block%d = blockBuilder%d.build();", i, i)
                    .getVariable(blockBuilder)
                    .invokeInterface(BlockBuilder.class, "build", com.facebook.presto.spi.block.Block.class)
                    .putVariable(currentBlock);
            buildBlock.append(storeBlock);
            body.append(buildBlock);
        }

        Variable rangeVariable = context.declareVariable(nativeContainerTypes.get(0), "range");
        Variable rangeBlockVariable = context.declareVariable(com.facebook.presto.spi.block.Block.class, "rangeBlock");

        body.comment("range = arg0; rangeBlock = block0;")
        .getVariable("arg0")
        .putVariable(rangeVariable)
        .getVariable("block0")
        .putVariable(rangeBlockVariable);

        Variable hashValueVariable = context.declareVariable(nativeContainerTypes.get(0), "hashValue");
        body.comment("hashValue = 0")
                .push(0)
                .intToLong()
                .putVariable(hashValueVariable);

        Variable currenHashValueVariable = context.declareVariable(nativeContainerTypes.get(0), "currentHashValue");
        Variable currentBlockLengthVariable = context.declareVariable(int.class, "currentLength");
        for (int i = 1; i < nativeContainerTypes.size(); i++) {
            Block currentBlockLength = new Block(context)
                    .getVariable("block" + i)
                    .push(0)
                    .invokeInterface(com.facebook.presto.spi.block.Block.class, "getLength", int.class, int.class)
                    .putVariable(currentBlockLengthVariable);

            Block currentHashValueBlock = new Block(context)
                    .getVariable("block" + i)
                    .push(0)
                    .push(0)
                    .getVariable(currentBlockLengthVariable)
                    .invokeInterface(com.facebook.presto.spi.block.Block.class, "hash", int.class, int.class, int.class, int.class)
                    .intToLong()
                    .getVariable("range")
                    .invokeStatic(BigintOperators.class, "modulus", long.class, long.class, nativeContainerTypes.get(0))
                    .putVariable(currenHashValueVariable);

            Block updateHashValueBlock = new Block(context)
                    .getVariable(currenHashValueVariable)
                    .getVariable(hashValueVariable)
                    .invokeStatic(BigintOperators.class, "add", long.class, long.class, long.class)
                    .getVariable("range")
                    .invokeStatic(BigintOperators.class, "modulus", long.class, long.class, nativeContainerTypes.get(0))
                    .putVariable(hashValueVariable);

            body.append(currentBlockLength)
                    .append(currentHashValueBlock)
                    .append(updateHashValueBlock);
        }
        body.comment("return hashValue")
                    .getVariable(hashValueVariable)
                    .getVariable("range")
                    .invokeStatic(BigintOperators.class, "add", long.class, long.class, long.class)
                    .getVariable("range")
                    .invokeStatic(BigintOperators.class, "modulus", long.class, long.class, nativeContainerTypes.get(0))
                    .ret(nativeContainerTypes.get(0));

        return defineClass(definition, Object.class, binder.getBindings(), new DynamicClassLoader(Hash.class.getClassLoader()));
    }
}
