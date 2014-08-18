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
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.expression.ByteCodeExpression;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Primitives;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.byteCode.OpCodes.NOP;
import static com.facebook.presto.byteCode.expression.ByteCodeExpression.invokeDynamic;
import static java.lang.String.format;

public class ByteCodeUtils
{
    private ByteCodeUtils()
    {
    }

    public static ByteCodeNode ifWasNullPopAndGoto(CompilerContext context, LabelNode label, Class<?> returnType, Class<?>... stackArgsToPop)
    {
        return handleNullValue(context, label, returnType, ImmutableList.copyOf(stackArgsToPop), false);
    }

    public static ByteCodeNode ifWasNullPopAndGoto(CompilerContext context, LabelNode label, Class<?> returnType, Iterable<? extends Class<?>> stackArgsToPop)
    {
        return handleNullValue(context, label, returnType, ImmutableList.copyOf(stackArgsToPop), false);
    }

    public static ByteCodeNode ifWasNullClearPopAndGoto(CompilerContext context, LabelNode label, Class<?> returnType, Class<?>... stackArgsToPop)
    {
        return handleNullValue(context, label, returnType, ImmutableList.copyOf(stackArgsToPop), true);
    }

    public static ByteCodeNode handleNullValue(CompilerContext context,
            LabelNode label,
            Class<?> returnType,
            List<? extends Class<?>> stackArgsToPop,
            boolean clearNullFlag)
    {
        Block nullCheck = new Block(context)
                .setDescription("ifWasNullGoto")
                .getVariable("wasNull");

        String clearComment = null;
        if (clearNullFlag) {
            nullCheck.putVariable("wasNull", false);
            clearComment = "clear wasNull";
        }

        Block isNull = new Block(context);
        for (Class<?> parameterType : stackArgsToPop) {
            isNull.pop(parameterType);
        }

        isNull.pushJavaDefault(returnType);
        String loadDefaultComment = null;
        if (returnType != void.class) {
            loadDefaultComment = format("loadJavaDefault(%s)", returnType.getName());
        }

        isNull.gotoLabel(label);

        String popComment = null;
        if (!stackArgsToPop.isEmpty()) {
            popComment = format("pop(%s)", Joiner.on(", ").join(stackArgsToPop));
        }

        String comment = format("if wasNull then %s", Joiner.on(", ").skipNulls().join(clearComment, popComment, loadDefaultComment, "goto " + label.getLabel()));
        return new IfStatement(context, comment, nullCheck, isNull, NOP);
    }

    public static ByteCodeNode boxPrimitive(CompilerContext context, Class<?> type)
    {
        Block block = new Block(context).comment("box primitive");
        if (type == long.class) {
            return block.invokeStatic(Long.class, "valueOf", Long.class, long.class);
        }
        if (type == double.class) {
            return block.invokeStatic(Double.class, "valueOf", Double.class, double.class);
        }
        if (type == boolean.class) {
            return block.invokeStatic(Boolean.class, "valueOf", Boolean.class, boolean.class);
        }
        if (type.isPrimitive()) {
            throw new UnsupportedOperationException("not yet implemented: " + type);
        }

        return NOP;
    }

    public static ByteCodeNode unboxPrimitive(CompilerContext context, Class<?> unboxedType)
    {
        Block block = new Block(context).comment("unbox primitive");
        if (unboxedType == long.class) {
            return block.invokeVirtual(Long.class, "longValue", long.class);
        }
        if (unboxedType == double.class) {
            return block.invokeVirtual(Double.class, "doubleValue", double.class);
        }
        if (unboxedType == boolean.class) {
            return block.invokeVirtual(Boolean.class, "booleanValue", boolean.class);
        }
        throw new UnsupportedOperationException("not yet implemented: " + unboxedType);
    }

    public static ByteCodeExpression invokeMethod(CompilerContext context, CallSiteBinder callSiteBinder, MethodHandle method)
    {
        Binding binding = callSiteBinder.bind(method);
        return invokeDynamic("call_" + binding.getBindingId(), binding.getType(), context.getDefaultBootstrapMethod(), binding.getBindingId());
    }

    public static ByteCodeExpression loadConstant(CompilerContext context, CallSiteBinder callSiteBinder, Object constant, Class<?> type)
    {
        Binding binding = callSiteBinder.bind(MethodHandles.constant(type, constant));
        return loadConstant(context, binding);
    }

    public static ByteCodeExpression loadConstant(CompilerContext context, Binding binding)
    {
        return invokeDynamic("constant_" + binding.getBindingId(), binding.getType(), context.getDefaultBootstrapMethod(), binding.getBindingId());
    }

    public static ByteCodeNode generateInvocation(CompilerContext context, FunctionInfo function, ByteCodeNode getSessionByteCode, List<ByteCodeNode> arguments, Binding binding)
    {
        MethodType methodType = binding.getType();

        Signature signature = function.getSignature();
        Class<?> unboxedReturnType = Primitives.unwrap(methodType.returnType());

        LabelNode end = new LabelNode("end");
        Block block = new Block(context)
                .setDescription("invoke " + signature);

        ArrayList<Class<?>> stackTypes = new ArrayList<>();

        int index = 0;
        for (Class<?> type : methodType.parameterArray()) {
            stackTypes.add(type);
            if (type == ConnectorSession.class) {
                block.append(getSessionByteCode);
            }
            else {
                block.append(arguments.get(index));
                index++;
                block.append(ByteCodeUtils.ifWasNullPopAndGoto(context, end, unboxedReturnType, Lists.reverse(stackTypes)));
            }
        }
        block.append(invoke(context, binding));

        if (function.isNullable()) {
            if (unboxedReturnType.isPrimitive()) {
                LabelNode notNull = new LabelNode("notNull");
                block.dup(methodType.returnType())
                        .ifNotNullGoto(notNull)
                        .putVariable("wasNull", true)
                        .comment("swap boxed null with unboxed default")
                        .pop(methodType.returnType())
                        .pushJavaDefault(unboxedReturnType)
                        .gotoLabel(end)
                        .visitLabel(notNull)
                        .append(ByteCodeUtils.unboxPrimitive(context, unboxedReturnType));
            }
            else {
                block.dup(methodType.returnType())
                        .ifNotNullGoto(end)
                        .putVariable("wasNull", true);
            }
        }
        block.visitLabel(end);

        return block;
    }

    public static ByteCodeNode invoke(CompilerContext context, Binding binding)
    {
        return new Block(context)
                .invokeDynamic("call_" + binding.getBindingId(), binding.getType(), binding.getBindingId());
    }
}
