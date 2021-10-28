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

import com.facebook.presto.bytecode.Binding;
import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.CallSiteBinder;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ScalarImplementationChoice;
import com.facebook.presto.sql.gen.InputReferenceCompiler.InputReferenceNode;
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
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentType.VALUE_TYPE;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ReturnPlaceConvention.PROVIDED_BLOCKBUILDER;
import static com.facebook.presto.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class BytecodeUtils
{
    private BytecodeUtils()
    {
    }

    public static BytecodeNode ifWasNullPopAndGoto(Scope scope, LabelNode label, Class<?> returnType, Class<?>... stackArgsToPop)
    {
        return handleNullValue(scope, label, returnType, ImmutableList.copyOf(stackArgsToPop), Optional.empty(), false);
    }

    public static BytecodeNode ifWasNullPopAndGoto(Scope scope, LabelNode label, Class<?> methodReturnType, Iterable<? extends Class<?>> stackArgsToPop)
    {
        return handleNullValue(scope, label, methodReturnType, ImmutableList.copyOf(stackArgsToPop), Optional.empty(), false);
    }

    public static BytecodeNode ifWasNullClearPopAndGoto(Scope scope, LabelNode label, Class<?> methodReturnType, Class<?>... stackArgsToPop)
    {
        return handleNullValue(scope, label, methodReturnType, ImmutableList.copyOf(stackArgsToPop), Optional.empty(), true);
    }

    public static BytecodeNode ifWasNullClearPopAppendAndGoto(Scope scope, LabelNode label, Class<?> methodReturnType, Variable outputBlockVariable, Iterable<? extends Class<?>> stackArgsToPop)
    {
        return handleNullValue(scope, label, methodReturnType, ImmutableList.copyOf(stackArgsToPop), Optional.of(outputBlockVariable), true);
    }

    public static BytecodeNode handleNullValue(Scope scope,
            LabelNode label,
            Class<?> methodReturnType,
            List<Class<?>> stackArgsToPop,
            Optional<Variable> outputBlockVariable,
            boolean clearNullFlag)
    {
        if (outputBlockVariable.isPresent()) {
            checkArgument(methodReturnType == void.class);
        }

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

        String loadDefaultOrAppendNullComment;
        if (!outputBlockVariable.isPresent()) {
            isNull.pushJavaDefault(methodReturnType);
            loadDefaultOrAppendNullComment = format("loadJavaDefault(%s)", methodReturnType.getName());
        }
        else {
            isNull.append(outputBlockVariable.get()
                    .invoke("appendNull", BlockBuilder.class)
                    .pop());
            loadDefaultOrAppendNullComment = "appendNullToOutputBlock";
        }

        isNull.gotoLabel(label);

        String popComment = null;
        if (!stackArgsToPop.isEmpty()) {
            popComment = format("pop(%s)", Joiner.on(", ").join(stackArgsToPop));
        }

        return new IfStatement("if wasNull then %s", Joiner.on(", ").skipNulls().join(clearComment, popComment, loadDefaultOrAppendNullComment, "goto " + label.getLabel()))
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

    public static BytecodeNode generateInvocation(
            Scope scope,
            String name,
            BuiltInScalarFunctionImplementation function,
            Optional<BytecodeNode> instance,
            List<BytecodeNode> arguments,
            CallSiteBinder binder)
    {
        return generateInvocation(
                scope,
                name,
                function,
                instance,
                arguments,
                binder,
                Optional.empty());
    }

    public static BytecodeNode generateInvocation(
            Scope scope,
            String name,
            BuiltInScalarFunctionImplementation function,
            Optional<BytecodeNode> instance,
            List<BytecodeNode> arguments,
            CallSiteBinder binder,
            Optional<OutputBlockVariableAndType> outputBlockVariableAndType)
    {
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

        // Go through all the choices in the function and then pick the best one
        List<ScalarImplementationChoice> choices = function.getAllChoices();
        ScalarImplementationChoice bestChoice = null;
        for (ScalarImplementationChoice currentChoice : choices) {
            boolean isValid = true;
            for (int i = 0; i < arguments.size(); i++) {
                if (currentChoice.getArgumentProperty(i).getArgumentType() != VALUE_TYPE) {
                    continue;
                }
                if (currentChoice.getArgumentProperty(i).getNullConvention() == NullConvention.BLOCK_AND_POSITION && !(arguments.get(i) instanceof InputReferenceNode)
                        || currentChoice.getReturnPlaceConvention() == PROVIDED_BLOCKBUILDER && (!outputBlockVariableAndType.isPresent())) {
                    isValid = false;
                    break;
                }
            }
            if (isValid) {
                bestChoice = currentChoice;
            }
        }

        checkState(bestChoice != null, "None of the scalar function implementation choices are valid");
        Binding binding = binder.bind(bestChoice.getMethodHandle());

        MethodType methodType = binding.getType();
        Class<?> returnType = methodType.returnType();
        Class<?> unboxedReturnType = Primitives.unwrap(returnType);

        boolean boundInstance = false;
        while (currentParameterIndex < methodType.parameterArray().length) {
            Class<?> type = methodType.parameterArray()[currentParameterIndex];
            stackTypes.add(type);
            if (bestChoice.getInstanceFactory().isPresent() && !boundInstance) {
                checkState(type.equals(bestChoice.getInstanceFactory().get().type().returnType()), "Mismatched type for instance parameter");
                block.append(instance.get());
                boundInstance = true;
            }
            else if (type == SqlFunctionProperties.class) {
                block.append(scope.getVariable("properties"));
            }
            else if (type == BlockBuilder.class) {
                block.append(outputBlockVariableAndType.get().getOutputBlockVariable());
            }
            else {
                ArgumentProperty argumentProperty = bestChoice.getArgumentProperty(realParameterIndex);
                switch (argumentProperty.getArgumentType()) {
                    case VALUE_TYPE:
                        // Apply null convention for value type argument
                        switch (argumentProperty.getNullConvention()) {
                            case RETURN_NULL_ON_NULL:
                                block.append(arguments.get(realParameterIndex));
                                checkArgument(!Primitives.isWrapperType(type), "Non-nullable argument must not be primitive wrapper type");
                                switch (bestChoice.getReturnPlaceConvention()) {
                                    case STACK:
                                        block.append(ifWasNullPopAndGoto(scope, end, unboxedReturnType, Lists.reverse(stackTypes)));
                                        break;
                                    case PROVIDED_BLOCKBUILDER:
                                        checkArgument(unboxedReturnType == void.class);
                                        block.append(ifWasNullClearPopAppendAndGoto(scope, end, unboxedReturnType, outputBlockVariableAndType.get().getOutputBlockVariable(), Lists.reverse(stackTypes)));
                                        break;
                                    default:
                                        throw new UnsupportedOperationException(format("Unsupported return place convention: %s", bestChoice.getReturnPlaceConvention()));
                                }
                                break;
                            case USE_NULL_FLAG:
                                block.append(arguments.get(realParameterIndex));
                                block.append(scope.getVariable("wasNull"));
                                block.append(scope.getVariable("wasNull").set(constantFalse()));
                                stackTypes.add(boolean.class);
                                currentParameterIndex++;
                                break;
                            case USE_BOXED_TYPE:
                                block.append(arguments.get(realParameterIndex));
                                block.append(boxPrimitiveIfNecessary(scope, type));
                                block.append(scope.getVariable("wasNull").set(constantFalse()));
                                break;
                            case BLOCK_AND_POSITION:
                                InputReferenceNode inputReferenceNode = (InputReferenceNode) arguments.get(realParameterIndex);
                                block.append(inputReferenceNode.produceBlockAndPosition());
                                stackTypes.add(int.class);
                                currentParameterIndex++;
                                break;
                            default:
                                throw new UnsupportedOperationException(format("Unsupported null convention: %s", argumentProperty.getNullConvention()));
                        }
                        break;
                    case FUNCTION_TYPE:
                        block.append(arguments.get(realParameterIndex));
                        break;
                    default:
                        throw new UnsupportedOperationException(format("Unsupported argument type: %s", argumentProperty.getArgumentType()));
                }
                realParameterIndex++;
            }
            currentParameterIndex++;
        }
        block.append(invoke(binding, name));

        if (function.isNullable()) {
            switch (bestChoice.getReturnPlaceConvention()) {
                case STACK:
                    block.append(unboxPrimitiveIfNecessary(scope, returnType));
                    break;
                case PROVIDED_BLOCKBUILDER:
                    // no-op
                    break;
                default:
                    throw new UnsupportedOperationException(format("Unsupported return place convention: %s", bestChoice.getReturnPlaceConvention()));
            }
        }
        block.visitLabel(end);

        if (outputBlockVariableAndType.isPresent()) {
            switch (bestChoice.getReturnPlaceConvention()) {
                case STACK:
                    block.append(generateWrite(binder, scope, scope.getVariable("wasNull"), outputBlockVariableAndType.get().getType(), outputBlockVariableAndType.get().getOutputBlockVariable()));
                    break;
                case PROVIDED_BLOCKBUILDER:
                    // no-op
                    break;
                default:
                    throw new UnsupportedOperationException(format("Unsupported return place convention: %s", bestChoice.getReturnPlaceConvention()));
            }
        }
        return block;
    }

    public static BytecodeBlock unboxPrimitiveIfNecessary(Scope scope, Class<?> boxedType)
    {
        BytecodeBlock block = new BytecodeBlock();
        LabelNode end = new LabelNode("end");
        Class<?> unboxedType = Primitives.unwrap(boxedType);
        Variable wasNull = scope.getVariable("wasNull");

        if (unboxedType.isPrimitive()) {
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

    public static BytecodeExpression invoke(Binding binding, String name)
    {
        return invokeDynamic(BOOTSTRAP_METHOD, ImmutableList.of(binding.getBindingId()), name, binding.getType());
    }

    public static BytecodeNode generateWrite(
            CallSiteBinder callSiteBinder,
            Scope scope,
            Variable wasNullVariable,
            Type type,
            Variable outputBlockVariable)
    {
        Class<?> valueJavaType = type.getJavaType();
        if (!valueJavaType.isPrimitive() && valueJavaType != Slice.class) {
            valueJavaType = Object.class;
        }
        String methodName = "write" + Primitives.wrap(valueJavaType).getSimpleName();

        // the value to be written is at the top of stack
        Variable tempValue = scope.createTempVariable(valueJavaType);
        return new BytecodeBlock()
                .comment("if (wasNull)")
                .append(new IfStatement()
                        .condition(wasNullVariable)
                        .ifTrue(new BytecodeBlock()
                                .comment("output.appendNull();")
                                .pop(valueJavaType)
                                .getVariable(outputBlockVariable)
                                .invokeInterface(BlockBuilder.class, "appendNull", BlockBuilder.class)
                                .pop())
                        .ifFalse(new BytecodeBlock()
                                .comment("%s.%s(output, %s)", type.getTypeSignature(), methodName, valueJavaType.getSimpleName())
                                .putVariable(tempValue)
                                .append(loadConstant(callSiteBinder.bind(type, Type.class)))
                                .getVariable(outputBlockVariable)
                                .getVariable(tempValue)
                                .invokeInterface(Type.class, methodName, void.class, BlockBuilder.class, valueJavaType)));
    }

    public static class OutputBlockVariableAndType
    {
        private final Variable outputBlockVariable;
        private final Type type;

        public OutputBlockVariableAndType(Variable outputBlockVariable, Type type)
        {
            this.outputBlockVariable = requireNonNull(outputBlockVariable);
            this.type = requireNonNull(type);
        }

        public Variable getOutputBlockVariable()
        {
            return outputBlockVariable;
        }

        public Type getType()
        {
            return type;
        }
    }
}
