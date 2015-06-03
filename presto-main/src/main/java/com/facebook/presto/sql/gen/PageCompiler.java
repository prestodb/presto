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
package com.facebook.presto.sql.gen;

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.OpCode;
import com.facebook.presto.byteCode.Parameter;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.expression.ByteCodeExpression;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.operator.PagePositionEqualitor;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.Parameter.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantInt;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.facebook.presto.sql.gen.CompilerUtils.makeClassName;
import static com.facebook.presto.sql.gen.SqlTypeByteCodeExpression.constantType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PageCompiler
{
    public static final PageCompiler INSTANCE = new PageCompiler();

    private final LoadingCache<CacheKey, PagePositionEqualitor> pagePositionEqualitors = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .build(new CacheLoader<CacheKey, PagePositionEqualitor>()
            {
                @Override
                public PagePositionEqualitor load(CacheKey key)
                        throws Exception
                {
                    return internalCompile(key.getTypes(), key.getChannels())
                            .getConstructor()
                            .newInstance();
                }
            });

    public PagePositionEqualitor compilePagePositionEqualitor(List<Type> types, List<Integer> channels)
    {
        requireNonNull(types, "types is null");
        requireNonNull(channels, "joinChannels is null");
        checkArgument(channels.size() == types.size(), "Must have the same number of channels as types");

        try {
            return pagePositionEqualitors.get(new CacheKey(types, channels));
        }
        catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    private Class<? extends PagePositionEqualitor> internalCompile(List<Type> types, List<Integer> channels)
    {
        CallSiteBinder callSiteBinder = new CallSiteBinder();

        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("PagePositionEqualitor"),
                type(Object.class),
                type(PagePositionEqualitor.class));

        generateConstructor(classDefinition);
        generateHashRowMethod(classDefinition, callSiteBinder, types, channels);
        generateRowEqualsRowMethod(classDefinition, callSiteBinder, types, channels);

        return defineClass(classDefinition, PagePositionEqualitor.class, callSiteBinder.getBindings(), getClass().getClassLoader());
    }

    private static void generateConstructor(ClassDefinition classDefinition)
    {
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC));

        Variable thisVariable = constructorDefinition.getThis();

        Block constructor = constructorDefinition
                .getBody()
                .comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        constructor.ret();
    }

    private static void generateHashRowMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, List<Type> types, List<Integer> channels)
    {
        Parameter position = arg("position", int.class);
        Parameter page = arg("page", com.facebook.presto.spi.Page.class);
        MethodDefinition hashRowMethod = classDefinition.declareMethod(a(PUBLIC), "hashRow", type(int.class), position, page);

        Variable resultVariable = hashRowMethod.getScope().declareVariable(int.class, "result");

        hashRowMethod.getBody().append(resultVariable.set(constantInt(0)));

        for (int index = 0; index < types.size(); index++) {
            ByteCodeExpression type = constantType(callSiteBinder, types.get(index));
            ByteCodeExpression channel = constantInt(channels.get(index));
            ByteCodeExpression block = page.invoke("getBlock", com.facebook.presto.spi.block.Block.class, channel);

            hashRowMethod
                    .getBody()
                    .getVariable(resultVariable)
                    .push(31)
                    .append(OpCode.IMUL)
                    .append(typeHashCode(type, block, position))
                    .append(OpCode.IADD)
                    .putVariable(resultVariable);
        }

        hashRowMethod
                .getBody()
                .getVariable(resultVariable)
                .retInt();
    }

    private static void generateRowEqualsRowMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, List<Type> types, List<Integer> channels)
    {
        MethodDefinition rowEqualsRowMethod = classDefinition.declareMethod(
                a(PUBLIC),
                "rowEqualsRow",
                type(boolean.class),
                arg("leftPosition", int.class),
                arg("leftPage", com.facebook.presto.spi.Page.class),
                arg("rightPosition", int.class),
                arg("rightPage", com.facebook.presto.spi.Page.class));

        Scope compilerContext = rowEqualsRowMethod.getScope();
        for (int index = 0; index < types.size(); index++) {
            ByteCodeExpression type = constantType(callSiteBinder, types.get(index));

            ByteCodeExpression channel = constantInt(channels.get(index));
            ByteCodeExpression leftBlock = compilerContext
                    .getVariable("leftPage")
                    .invoke("getBlock", com.facebook.presto.spi.block.Block.class, channel);
            ByteCodeExpression rightBlock = compilerContext
                    .getVariable("rightPage")
                    .invoke("getBlock", com.facebook.presto.spi.block.Block.class, channel);

            LabelNode checkNextField = new LabelNode("checkNextField");
            rowEqualsRowMethod
                    .getBody()
                    .append(typeEquals(
                            type,
                            leftBlock,
                            compilerContext.getVariable("leftPosition"),
                            rightBlock,
                            compilerContext.getVariable("rightPosition")))
                    .ifTrueGoto(checkNextField)
                    .push(false)
                    .retBoolean()
                    .visitLabel(checkNextField);
        }

        rowEqualsRowMethod
                .getBody()
                .push(true)
                .retInt();
    }

    private static ByteCodeNode typeEquals(
            ByteCodeExpression type,
            ByteCodeExpression leftBlock,
            ByteCodeExpression leftBlockPosition,
            ByteCodeExpression rightBlock,
            ByteCodeExpression rightBlockPosition)
    {
        IfStatement ifStatement = new IfStatement();
        ifStatement.condition()
                .append(leftBlock.invoke("isNull", boolean.class, leftBlockPosition))
                .append(rightBlock.invoke("isNull", boolean.class, rightBlockPosition))
                .append(OpCode.IOR);

        ifStatement.ifTrue()
                .append(leftBlock.invoke("isNull", boolean.class, leftBlockPosition))
                .append(rightBlock.invoke("isNull", boolean.class, rightBlockPosition))
                .append(OpCode.IAND);

        ifStatement.ifFalse().append(type.invoke("equalTo", boolean.class, leftBlock, leftBlockPosition, rightBlock, rightBlockPosition));

        return ifStatement;
    }

    private static ByteCodeNode typeHashCode(ByteCodeExpression type, ByteCodeExpression blockRef, ByteCodeExpression blockPosition)
    {
        return new IfStatement()
                .condition(blockRef.invoke("isNull", boolean.class, blockPosition))
                .ifTrue(constantInt(0))
                .ifFalse(type.invoke("hash", int.class, blockRef, blockPosition));
    }

    private static final class CacheKey
    {
        private final List<Type> types;
        private final List<Integer> channels;

        private CacheKey(List<? extends Type> types, List<Integer> channels)
        {
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.channels = ImmutableList.copyOf(requireNonNull(channels, "channels is null"));
            checkArgument(channels.size() == types.size(), "Must have the same number of channels as types");
        }

        private List<Type> getTypes()
        {
            return types;
        }

        private List<Integer> getChannels()
        {
            return channels;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(types, channels);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof CacheKey)) {
                return false;
            }
            CacheKey other = (CacheKey) obj;
            return Objects.equals(this.types, other.types) &&
                    Objects.equals(this.channels, other.channels);
        }
    }
}
