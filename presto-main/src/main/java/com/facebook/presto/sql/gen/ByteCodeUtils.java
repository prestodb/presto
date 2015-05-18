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
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.expression.ByteCodeExpression;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Primitives;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.byteCode.OpCode.NOP;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantFalse;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantTrue;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.invokeDynamic;
import static com.facebook.presto.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static java.lang.String.format;

public final class ByteCodeUtils
{
    private ByteCodeUtils()
    {
    }

    public static ByteCodeNode ifWasNullPopAndGoto(Scope scope, LabelNode label, Class<?> returnType, Class<?>... stackArgsToPop)
    {
        return handleNullValue(scope, label, returnType, ImmutableList.copyOf(stackArgsToPop), false);
    }

    public static ByteCodeNode ifWasNullPopAndGoto(Scope scope, LabelNode label, Class<?> returnType, Iterable<? extends Class<?>> stackArgsToPop)
    {
        return handleNullValue(scope, label, returnType, ImmutableList.copyOf(stackArgsToPop), false);
    }

    public static ByteCodeNode ifWasNullClearPopAndGoto(Scope scope, LabelNode label, Class<?> returnType, Class<?>... stackArgsToPop)
    {
        return handleNullValue(scope, label, returnType, ImmutableList.copyOf(stackArgsToPop), true);
    }

    public static ByteCodeNode handleNullValue(Scope scope,
            LabelNode label,
            Class<?> returnType,
            List<Class<?>> stackArgsToPop,
            boolean clearNullFlag)
    {
        Variable wasNull = scope.getVariable("wasNull");

        Block nullCheck = new Block()
                .setDescription("ifWasNullGoto")
                .append(wasNull);

        String clearComment = null;
        if (clearNullFlag) {
            nullCheck.append(wasNull.set(constantFalse()));
            clearComment = "clear wasNull";
        }

        Block isNull = new Block();
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

        return new IfStatement("if wasNull then %s", Joiner.on(", ").skipNulls().join(clearComment, popComment, loadDefaultComment, "goto " + label.getLabel()))
                .condition(nullCheck)
                .ifTrue(isNull);
    }

    public static ByteCodeNode boxPrimitive(Class<?> type)
    {
        Block block = new Block().comment("box primitive");
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

    public static ByteCodeNode unboxPrimitive(Class<?> unboxedType)
    {
        Block block = new Block().comment("unbox primitive");
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

    public static ByteCodeExpression loadConstant(CallSiteBinder callSiteBinder, Object constant, Class<?> type)
    {
        Binding binding = callSiteBinder.bind(MethodHandles.constant(type, constant));
        return loadConstant(binding);
    }

    public static ByteCodeExpression loadConstant(Binding binding)
    {
        return invokeDynamic(
                BOOTSTRAP_METHOD,
                ImmutableList.of(binding.getBindingId()),
                "constant_" + binding.getBindingId(),
                binding.getType().returnType());
    }

    public static ByteCodeNode generateInvocation(Scope scope, FunctionInfo function, List<ByteCodeNode> arguments, Binding binding)
    {
        MethodType methodType = binding.getType();

        Signature signature = function.getSignature();
        Class<?> returnType = methodType.returnType();
        Class<?> unboxedReturnType = Primitives.unwrap(returnType);

        LabelNode end = new LabelNode("end");
        Block block = new Block()
                .setDescription("invoke " + signature);

        List<Class<?>> stackTypes = new ArrayList<>();

        int index = 0;
        for (Class<?> type : methodType.parameterArray()) {
            stackTypes.add(type);
            if (type == ConnectorSession.class) {
                block.append(scope.getVariable("session"));
            }
            else {
                block.append(arguments.get(index));
                if (!function.getNullableArguments().get(index)) {
                    block.append(ifWasNullPopAndGoto(scope, end, unboxedReturnType, Lists.reverse(stackTypes)));
                }
                else {
                    block.append(boxPrimitiveIfNecessary(scope, type));
                    block.append(scope.getVariable("wasNull").set(constantFalse()));
                }
                index++;
            }
        }
        block.append(invoke(binding, function.getSignature()));

        if (function.isNullable()) {
            block.append(unboxPrimitiveIfNecessary(scope, returnType));
        }
        block.visitLabel(end);

        return block;
    }

    public static Block unboxPrimitiveIfNecessary(Scope scope, Class<?> boxedType)
    {
        Block block = new Block();
        LabelNode end = new LabelNode("end");
        Class<?> unboxedType = Primitives.unwrap(boxedType);
        Variable wasNull = scope.getVariable("wasNull");

        if (unboxedType.isPrimitive() && unboxedType != void.class) {
            LabelNode notNull = new LabelNode("notNull");
            block.dup(boxedType)
                    .ifNotNullGoto(notNull)
                    .append(wasNull.set(constantTrue()))
                    .comment("swap boxed null with unboxed default")
                    .pop(boxedType)
                    .pushJavaDefault(unboxedType)
                    .gotoLabel(end)
                    .visitLabel(notNull)
                    .append(unboxPrimitive(unboxedType));
        }
        else {
            block.dup(boxedType)
                    .ifNotNullGoto(end)
                    .append(wasNull.set(constantTrue()));
        }
        block.visitLabel(end);

        return block;
    }

    public static ByteCodeNode boxPrimitiveIfNecessary(Scope scope, Class<?> type)
    {
        if (!Primitives.isWrapperType(type)) {
            return NOP;
        }
        Block notNull = new Block().comment("box primitive");
        Class<?> expectedCurrentStackType;
        if (type == Long.class) {
            notNull.invokeStatic(Long.class, "valueOf", Long.class, long.class);
            expectedCurrentStackType = long.class;
        }
        else if (type == Double.class) {
            notNull.invokeStatic(Double.class, "valueOf", Double.class, double.class);
            expectedCurrentStackType = double.class;
        }
        else if (type == Boolean.class) {
            notNull.invokeStatic(Boolean.class, "valueOf", Boolean.class, boolean.class);
            expectedCurrentStackType = boolean.class;
        }
        else if (type == Void.class) {
            notNull.pushNull()
                    .checkCast(Void.class);
            expectedCurrentStackType = void.class;
        }
        else {
            throw new UnsupportedOperationException("not yet implemented: " + type);
        }

        Block condition = new Block().append(scope.getVariable("wasNull"));

        Block wasNull = new Block()
                .pop(expectedCurrentStackType)
                .pushNull()
                .checkCast(type);

        return new IfStatement()
                .condition(condition)
                .ifTrue(wasNull)
                .ifFalse(notNull);
    }

    public static ByteCodeNode invoke(Binding binding, String name)
    {
        return invokeDynamic(BOOTSTRAP_METHOD, ImmutableList.of(binding.getBindingId()), name, binding.getType());
    }

    public static ByteCodeNode invoke(Binding binding, Signature signature)
    {
        return invoke(binding, signature.getName());
    }

    public static ByteCodeNode generateWrite(CallSiteBinder callSiteBinder, Scope scope, Variable wasNullVariable, Type type)
    {
        if (type.getJavaType() == void.class) {
            return new Block().comment("output.appendNull();")
                    .invokeInterface(BlockBuilder.class, "appendNull", BlockBuilder.class)
                    .pop();
        }
        String methodName = "write" + Primitives.wrap(type.getJavaType()).getSimpleName();

        // the stack contains [output, value]

        // We should be able to insert the code to get the output variable and compute the value
        // at the right place instead of assuming they are in the stack. We should also not need to
        // use temp variables to re-shuffle the stack to the right shape before Type.writeXXX is called
        // Unfortunately, because of the assumptions made by try_cast, we can't get around it yet.
        // TODO: clean up once try_cast is fixed
        Variable tempValue = scope.createTempVariable(type.getJavaType());
        Variable tempOutput = scope.createTempVariable(BlockBuilder.class);
        return new Block()
                .comment("if (wasNull)")
                .append(new IfStatement()
                        .condition(wasNullVariable)
                        .ifTrue(new Block()
                                .comment("output.appendNull();")
                                .pop(type.getJavaType())
                                .invokeInterface(BlockBuilder.class, "appendNull", BlockBuilder.class)
                                .pop())
                        .ifFalse(new Block()
                                .comment("%s.%s(output, %s)", type.getTypeSignature(), methodName, type.getJavaType().getSimpleName())
                                .putVariable(tempValue)
                                .putVariable(tempOutput)
                                .append(loadConstant(callSiteBinder.bind(type, Type.class)))
                                .getVariable(tempOutput)
                                .getVariable(tempValue)
                                .invokeInterface(Type.class, methodName, void.class, BlockBuilder.class, type.getJavaType())));
    }
}
