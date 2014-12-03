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
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.NamedParameterDefinition;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.instruction.JumpInstruction;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.ParametricScalar;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.gen.Bootstrap;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.CompilerUtils;
import com.facebook.presto.sql.gen.SqlTypeByteCodeExpression;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.NamedParameterDefinition.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.metadata.Signature.orderableTypeParameter;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.facebook.presto.sql.relational.Signatures.leastSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.String.format;

public final class Least
        extends ParametricScalar
{
    public static final Least LEAST = new Least();
    private static final Signature SIGNATURE = new Signature("least", ImmutableList.of(orderableTypeParameter("E")), "E", ImmutableList.of("E"), true, false);

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
        return "LEAST(arg1, [arg2, ...]) returns the least element among the given set of orderable elements of the same type.";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager)
    {
        checkArgument(types.size() == 1, "Can select the least element only from exactly matching types");
        Class<?> res;
        ImmutableList.Builder<Class<?>> builder = ImmutableList.builder();
        Type type = types.get("E");
        checkArgument(type.isOrderable(), "Type should be orderable");
        for (int i = 0; i < arity; i++) {
            builder.add(type.getJavaType());
        }
        ImmutableList<Class<?>> stackTypes = builder.build();
        Class<?> clazz = generateLeast(stackTypes, type);
        MethodHandle methodHandle;
        try {
            Method method = clazz.getMethod("least", stackTypes.toArray(new Class<?>[stackTypes.size()]));
            methodHandle = lookup().unreflect(method);
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
        List<Boolean> nullableParameters = ImmutableList.copyOf(Collections.nCopies(stackTypes.size(), false));
        return new FunctionInfo(leastSignature(type.getTypeSignature(), Collections.nCopies(arity, type.getTypeSignature())), "Finds the least element", true, methodHandle, true, false, nullableParameters);
    }

    private static Class<?> generateLeast(List<Class<?>> stackTypes, Type elementType)
    {
        List<String> stackTypeNames = FluentIterable.from(stackTypes).transform(new Function<Class<?>, String>() {
            @Override
            public String apply(Class<?> input)
            {
                return input.getSimpleName();
            }
        }).toList();

        CompilerContext context = new CompilerContext(Bootstrap.BOOTSTRAP_METHOD);
        ClassDefinition definition = new ClassDefinition(
                context,
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName(Joiner.on("").join(stackTypeNames) + "Least"),
                type(Object.class));

        definition.declareDefaultConstructor(a(PRIVATE));

        ImmutableList.Builder<NamedParameterDefinition> parameters = ImmutableList.builder();
        for (int i = 0; i < stackTypes.size(); i++) {
            Class<?> stackType = stackTypes.get(i);
            parameters.add(arg("arg" + i, stackType));
        }

        Block body = definition.declareMethod(context, a(PUBLIC, STATIC), "least", type(stackTypes.get(0)), parameters.build())
                .getBody();

        Variable typeVariable = context.declareVariable(Type.class, "typeVar");
        CallSiteBinder binder = new CallSiteBinder();
        body.comment("typeVariable = elementType;")
                .append(SqlTypeByteCodeExpression.constantType(context, binder, elementType))
                .putVariable(typeVariable);

        for (int i = 0; i < stackTypes.size(); i++) {
            Class<?> stackType = stackTypes.get(i);
            Variable currentBlock = context.declareVariable(com.facebook.presto.spi.block.Block.class, "block" + i);
            Variable blockBuilder = context.declareVariable(com.facebook.presto.spi.block.BlockBuilder.class, "blockBuilder" + i);
            Block buildBlock = new Block(context)
                    .comment("blockBuilder%d = typeVariable.createBlockBuilder(new BlockBuilderStatus());", i)
                    .getVariable(typeVariable)
                    .newObject(com.facebook.presto.spi.block.BlockBuilderStatus.class)
                    .dup()
                    .invokeConstructor(com.facebook.presto.spi.block.BlockBuilderStatus.class)
                    .invokeInterface(Type.class, "createBlockBuilder", com.facebook.presto.spi.block.BlockBuilder.class, com.facebook.presto.spi.block.BlockBuilderStatus.class)
                    .putVariable(blockBuilder);

            String writeMethodName;
            if (stackType == long.class) {
                writeMethodName = "writeLong";
            }
            else if (stackType == boolean.class) {
                writeMethodName = "writeBoolean";
            }
            else if (stackType == double.class) {
                writeMethodName = "writeDouble";
            }
            else if (stackType == Slice.class) {
                writeMethodName = "writeSlice";
            }
            else {
                throw new PrestoException(INTERNAL_ERROR, format("Unexpected type %s", stackType.getName()));
            }

            Block writeBlock = new Block(context)
                    .comment("typeVariable.%s(blockBuilder%d, arg%d);", writeMethodName, i, i)
                    .getVariable(typeVariable)
                    .getVariable(blockBuilder)
                    .getVariable("arg" + i)
                    .invokeInterface(Type.class, writeMethodName, void.class, com.facebook.presto.spi.block.BlockBuilder.class, stackType);

            buildBlock.append(writeBlock);

            Block storeBlock = new Block(context)
                    .comment("block%d = blockBuilder%d.build();", i, i)
                    .getVariable(blockBuilder)
                    .invokeInterface(com.facebook.presto.spi.block.BlockBuilder.class, "build", com.facebook.presto.spi.block.Block.class)
                    .putVariable(currentBlock);
            buildBlock.append(storeBlock);
            body.append(buildBlock);
        }

        Variable ansVariable = context.declareVariable(stackTypes.get(0), "ans");
        Variable ansBlockVariable = context.declareVariable(com.facebook.presto.spi.block.Block.class, "ansBlock");

        body.comment("ans = arg0; ansBlock = block0;")
                .getVariable("arg0")
                .putVariable(ansVariable)
                .getVariable("block0")
                .putVariable(ansBlockVariable);

        for (int i = 1; i < stackTypes.size(); i++) {
            LabelNode end = new LabelNode("end" + i);
            Block compare = new Block(context)
                    .getVariable(typeVariable)
                    .getVariable(ansBlockVariable)
                    .push(0)
                    .getVariable("block" + i)
                    .push(0)
                    .invokeInterface(Type.class, "compareTo", int.class, com.facebook.presto.spi.block.Block.class, int.class, com.facebook.presto.spi.block.Block.class, int.class)
                    .push(0)
                    .append(JumpInstruction.jumpIfIntLessThan(end))
                    .getVariable("arg" + i)
                    .putVariable(ansVariable)
                    .getVariable("block" + i)
                    .putVariable(ansBlockVariable)
                    .visitLabel(end);
            body.append(compare);
        }

        body.comment("return ans;")
                    .getVariable(ansVariable)
                    .ret(stackTypes.get(0));

        return defineClass(definition, Object.class, binder.getBindings(), new DynamicClassLoader());
    }
}
