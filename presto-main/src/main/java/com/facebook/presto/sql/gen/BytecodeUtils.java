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

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.bytecode.OpCode.NOP;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantTrue;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeDynamic;
import static com.facebook.presto.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

public final class BytecodeUtils
{
    private BytecodeUtils()
    {
    }

    public static BytecodeNode ifWasNullPopAndGoto(Scope scope, LabelNode label, Class<?> returnType, Class<?>... stackArgsToPop)
    {
        return handleNullValue(scope, label, returnType, ImmutableList.copyOf(stackArgsToPop), false);
    }

    public static BytecodeNode ifWasNullPopAndGoto(Scope scope, LabelNode label, Class<?> returnType, Iterable<? extends Class<?>> stackArgsToPop)
    {
        return handleNullValue(scope, label, returnType, ImmutableList.copyOf(stackArgsToPop), false);
    }

    public static BytecodeNode ifWasNullClearPopAndGoto(Scope scope, LabelNode label, Class<?> returnType, Class<?>... stackArgsToPop)
    {
        return handleNullValue(scope, label, returnType, ImmutableList.copyOf(stackArgsToPop), true);
    }

    public static BytecodeNode handleNullValue(Scope scope,
            LabelNode label,
            Class<?> returnType,
            List<Class<?>> stackArgsToPop,
            boolean clearNullFlag)
    {
        Variable wasNull = scope.getVariable("wasNull");

        BytecodeBlock nullCheck = new BytecodeBlock()
                .setDescription("ifWasNullGoto")
                .append(wasNull);

        String clearComment = null;
        if (clearNullFlag) {
            nullCheck.append(wasNull.set(constantFalse()));
            clearComment = "clear wasNull";
        }

        BytecodeBlock isNull = new BytecodeBlock();
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

    public static BytecodeNode boxPrimitive(Class<?> type)
    {
        BytecodeBlock block = new BytecodeBlock().comment("box primitive");
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

    public static BytecodeNode unboxPrimitive(Class<?> unboxedType)
    {
        BytecodeBlock block = new BytecodeBlock().comment("unbox primitive");
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

    public static BytecodeExpression loadConstant(CallSiteBinder callSiteBinder, Object constant, Class<?> type)
    {
        Binding binding = callSiteBinder.bind(MethodHandles.constant(type, constant));
        return loadConstant(binding);
    }

    public static BytecodeExpression loadConstant(Binding binding)
    {
        return invokeDynamic(
                BOOTSTRAP_METHOD,
                ImmutableList.of(binding.getBindingId()),
                "constant_" + binding.getBindingId(),
                binding.getType().returnType());
    }

    public static BytecodeNode generateInvocation(Scope scope, String name, ScalarFunctionImplementation function, Optional<BytecodeNode> instance, List<BytecodeNode> arguments, Binding binding)
    {
        MethodType methodType = binding.getType();

        Class<?> returnType = methodType.returnType();
        Class<?> unboxedReturnType = Primitives.unwrap(returnType);

        LabelNode end = new LabelNode("end");
        BytecodeBlock block = new BytecodeBlock()
                .setDescription("invoke " + name);

        List<Class<?>> stackTypes = new ArrayList<>();
        if (function.getInstanceFactory().isPresent()) {
            checkArgument(instance.isPresent());
        }

        // Index of current parameter in the MethodHandle
        int currentParameterIndex = 0;

        // Index of parameter (without @IsNull) in Presto function
        int realParameterIndex = 0;

        boolean boundInstance = false;
        while (currentParameterIndex < methodType.parameterArray().length) {
            Class<?> type = methodType.parameterArray()[currentParameterIndex];
            stackTypes.add(type);
            if (function.getInstanceFactory().isPresent() && !boundInstance) {
                checkState(type.equals(function.getInstanceFactory().get().type().returnType()), "Mismatched type for instance parameter");
                block.append(instance.get());
                boundInstance = true;
            }
            else if (type == ConnectorSession.class) {
                block.append(scope.getVariable("session"));
            }
            else {
                block.append(arguments.get(realParameterIndex));
                if (!function.getNullableArguments().get(realParameterIndex)) {
                    checkArgument(!Primitives.isWrapperType(type), "Non-nullable argument must not be primitive wrapper type");
                    block.append(ifWasNullPopAndGoto(scope, end, unboxedReturnType, Lists.reverse(stackTypes)));
                }
                else {
                    if (function.getNullFlags().get(realParameterIndex)) {
                        if (type == Void.class) {
                            block.append(boxPrimitiveIfNecessary(scope, type));
                        }
                        block.append(scope.getVariable("wasNull"));
                        stackTypes.add(boolean.class);
                        currentParameterIndex++;
                    }
                    else {
                        block.append(boxPrimitiveIfNecessary(scope, type));
                    }
                    block.append(scope.getVariable("wasNull").set(constantFalse()));
                }
                realParameterIndex++;
            }
            currentParameterIndex++;
        }
        block.append(invoke(binding, name));

        if (function.isNullable()) {
            block.append(unboxPrimitiveIfNecessary(scope, returnType));
        }
        block.visitLabel(end);

        return block;
    }

    public static BytecodeBlock unboxPrimitiveIfNecessary(Scope scope, Class<?> boxedType)
    {
        BytecodeBlock block = new BytecodeBlock();
        LabelNode end = new LabelNode("end");
        Class<?> unboxedType = Primitives.unwrap(boxedType);
        Variable wasNull = scope.getVariable("wasNull");

        if (unboxedType == void.class) {
            block.pop(boxedType)
                    .append(wasNull.set(constantTrue()));
        }
        else if (unboxedType.isPrimitive()) {
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

    public static BytecodeNode boxPrimitiveIfNecessary(Scope scope, Class<?> type)
    {
        checkArgument(!type.isPrimitive(), "cannot box into primitive type");
        if (!Primitives.isWrapperType(type)) {
            return NOP;
        }
        BytecodeBlock notNull = new BytecodeBlock().comment("box primitive");
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
            return notNull;
        }
        else {
            throw new UnsupportedOperationException("not yet implemented: " + type);
        }

        BytecodeBlock condition = new BytecodeBlock().append(scope.getVariable("wasNull"));

        BytecodeBlock wasNull = new BytecodeBlock()
                .pop(expectedCurrentStackType)
                .pushNull()
                .checkCast(type);

        return new IfStatement()
                .condition(condition)
                .ifTrue(wasNull)
                .ifFalse(notNull);
    }

    public static BytecodeNode invoke(Binding binding, String name)
    {
        return invokeDynamic(BOOTSTRAP_METHOD, ImmutableList.of(binding.getBindingId()), name, binding.getType());
    }

    public static BytecodeNode invoke(Binding binding, Signature signature)
    {
        return invoke(binding, signature.getName());
    }

    public static BytecodeNode generateWrite(CallSiteBinder callSiteBinder, Scope scope, Variable wasNullVariable, Type type)
    {
        if (type.getJavaType() == void.class) {
            return new BytecodeBlock().comment("output.appendNull();")
                    .invokeInterface(BlockBuilder.class, "appendNull", BlockBuilder.class)
                    .pop();
        }

        Class<?> valueJavaType = type.getJavaType();
        if (!valueJavaType.isPrimitive() && valueJavaType != Slice.class) {
            valueJavaType = Object.class;
        }
        String methodName = "write" + Primitives.wrap(valueJavaType).getSimpleName();

        // the stack contains [output, value]

        // We should be able to insert the code to get the output variable and compute the value
        // at the right place instead of assuming they are in the stack. We should also not need to
        // use temp variables to re-shuffle the stack to the right shape before Type.writeXXX is called
        // Unfortunately, because of the assumptions made by try_cast, we can't get around it yet.
        // TODO: clean up once try_cast is fixed
        Variable tempValue = scope.createTempVariable(valueJavaType);
        Variable tempOutput = scope.createTempVariable(BlockBuilder.class);
        return new BytecodeBlock()
                .comment("if (wasNull)")
                .append(new IfStatement()
                        .condition(wasNullVariable)
                        .ifTrue(new BytecodeBlock()
                                .comment("output.appendNull();")
                                .pop(valueJavaType)
                                .invokeInterface(BlockBuilder.class, "appendNull", BlockBuilder.class)
                                .pop())
                        .ifFalse(new BytecodeBlock()
                                .comment("%s.%s(output, %s)", type.getTypeSignature(), methodName, valueJavaType.getSimpleName())
                                .putVariable(tempValue)
                                .putVariable(tempOutput)
                                .append(loadConstant(callSiteBinder.bind(type, Type.class)))
                                .getVariable(tempOutput)
                                .getVariable(tempValue)
                                .invokeInterface(Type.class, methodName, void.class, BlockBuilder.class, valueJavaType)));
    }
}
