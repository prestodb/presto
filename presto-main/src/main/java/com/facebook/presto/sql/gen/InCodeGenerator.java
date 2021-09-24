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
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.control.SwitchStatement.SwitchBuilder;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.util.FastutilSetHelper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;

import java.lang.invoke.MethodHandle;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantTrue;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.bytecode.instruction.JumpInstruction.jump;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.HASH_CODE;
import static com.facebook.presto.common.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.gen.BytecodeUtils.ifWasNullPopAndGoto;
import static com.facebook.presto.sql.gen.BytecodeUtils.invoke;
import static com.facebook.presto.sql.gen.BytecodeUtils.loadConstant;
import static com.facebook.presto.sql.gen.SpecialFormBytecodeGenerator.generateWrite;
import static com.facebook.presto.util.FastutilSetHelper.toFastutilHashSet;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class InCodeGenerator
        implements SpecialFormBytecodeGenerator
{
    private final FunctionAndTypeManager functionAndTypeManager;

    public InCodeGenerator(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
    }

    enum SwitchGenerationCase
    {
        DIRECT_SWITCH,
        HASH_SWITCH,
        SET_CONTAINS
    }

    @VisibleForTesting
    static SwitchGenerationCase checkSwitchGenerationCase(Type type, List<RowExpression> values)
    {
        if (values.size() > 32) {
            // 32 is chosen because
            // * SET_CONTAINS performs worst when smaller than but close to power of 2
            // * Benchmark shows performance of SET_CONTAINS is better at 50, but similar at 25.
            return SwitchGenerationCase.SET_CONTAINS;
        }

        if (!(type instanceof IntegerType || type instanceof BigintType || type instanceof DateType)) {
            return SwitchGenerationCase.HASH_SWITCH;
        }
        for (RowExpression expression : values) {
            // For non-constant expressions, they will be added to the default case in the generated switch code. They do not affect any of
            // the cases other than the default one. Therefore, it's okay to skip them when choosing between DIRECT_SWITCH and HASH_SWITCH.
            // Same argument applies for nulls.
            if (!(expression instanceof ConstantExpression)) {
                continue;
            }
            Object constant = ((ConstantExpression) expression).getValue();
            if (constant == null) {
                continue;
            }
            long longConstant = ((Number) constant).longValue();
            if (longConstant < Integer.MIN_VALUE || longConstant > Integer.MAX_VALUE) {
                return SwitchGenerationCase.HASH_SWITCH;
            }
        }
        return SwitchGenerationCase.DIRECT_SWITCH;
    }

    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext generatorContext, Type returnType, List<RowExpression> arguments, Optional<Variable> outputBlockVariable)
    {
        List<RowExpression> values = arguments.subList(1, arguments.size());
        // empty IN statements are not allowed by the standard, and not possible here
        // the implementation assumes this condition is always met
        checkArgument(values.size() > 0, "values must not be empty");

        Type type = arguments.get(0).getType();
        Class<?> javaType = type.getJavaType();

        SwitchGenerationCase switchGenerationCase = checkSwitchGenerationCase(type, values);

        FunctionHandle hashCodeHandle = generatorContext.getFunctionManager().resolveOperator(HASH_CODE, fromTypes(type));
        MethodHandle hashCodeFunction = generatorContext.getFunctionManager().getBuiltInScalarFunctionImplementation(hashCodeHandle).getMethodHandle();
        FunctionHandle isIndeterminateHandle = generatorContext.getFunctionManager().resolveOperator(INDETERMINATE, fromTypes(type));
        BuiltInScalarFunctionImplementation isIndeterminateFunction = generatorContext.getFunctionManager().getBuiltInScalarFunctionImplementation(isIndeterminateHandle);

        ImmutableListMultimap.Builder<Integer, BytecodeNode> hashBucketsBuilder = ImmutableListMultimap.builder();
        ImmutableList.Builder<BytecodeNode> defaultBucket = ImmutableList.builder();
        ImmutableSet.Builder<Object> constantValuesBuilder = ImmutableSet.builder();

        for (RowExpression testValue : values) {
            BytecodeNode testBytecode = generatorContext.generate(testValue, Optional.empty());

            if (isDeterminateConstant(testValue, isIndeterminateFunction.getMethodHandle())) {
                ConstantExpression constant = (ConstantExpression) testValue;
                Object object = constant.getValue();
                switch (switchGenerationCase) {
                    case DIRECT_SWITCH:
                    case SET_CONTAINS:
                        constantValuesBuilder.add(object);
                        break;
                    case HASH_SWITCH:
                        try {
                            int hashCode = toIntExact(Long.hashCode((Long) hashCodeFunction.invoke(object)));
                            hashBucketsBuilder.put(hashCode, testBytecode);
                        }
                        catch (Throwable throwable) {
                            throw new IllegalArgumentException("Error processing IN statement: error calculating hash code for " + object, throwable);
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Not supported switch generation case: " + switchGenerationCase);
                }
            }
            else {
                defaultBucket.add(testBytecode);
            }
        }
        ImmutableListMultimap<Integer, BytecodeNode> hashBuckets = hashBucketsBuilder.build();
        ImmutableSet<Object> constantValues = constantValuesBuilder.build();

        LabelNode end = new LabelNode("end");
        LabelNode match = new LabelNode("match");
        LabelNode noMatch = new LabelNode("noMatch");

        LabelNode defaultLabel = new LabelNode("default");

        Scope scope = generatorContext.getScope();
        Variable value = scope.createTempVariable(javaType);

        BytecodeNode switchBlock;
        Variable expression = scope.createTempVariable(int.class);
        SwitchBuilder switchBuilder = new SwitchBuilder().expression(expression);

        switch (switchGenerationCase) {
            case DIRECT_SWITCH:
                // A white-list is used to select types eligible for DIRECT_SWITCH.
                // For these types, it's safe to not use presto HASH_CODE and EQUAL operator.
                for (Object constantValue : constantValues) {
                    switchBuilder.addCase(toIntExact((Long) constantValue), jump(match));
                }
                switchBuilder.defaultCase(jump(defaultLabel));
                switchBlock = new BytecodeBlock()
                        .comment("lookupSwitch(<stackValue>))")
                        .append(new IfStatement()
                                .condition(invokeStatic(InCodeGenerator.class, "isInteger", boolean.class, value))
                                .ifFalse(new BytecodeBlock()
                                        .gotoLabel(defaultLabel)))
                        .append(expression.set(value.cast(int.class)))
                        .append(switchBuilder.build());
                break;
            case HASH_SWITCH:
                for (Map.Entry<Integer, Collection<BytecodeNode>> bucket : hashBuckets.asMap().entrySet()) {
                    Collection<BytecodeNode> testValues = bucket.getValue();
                    BytecodeBlock caseBlock = buildInCase(
                            generatorContext,
                            scope,
                            type,
                            match,
                            defaultLabel,
                            value,
                            testValues,
                            false,
                            isIndeterminateFunction);
                    switchBuilder.addCase(bucket.getKey(), caseBlock);
                }
                switchBuilder.defaultCase(jump(defaultLabel));
                Binding hashCodeBinding = generatorContext
                        .getCallSiteBinder()
                        .bind(hashCodeFunction);
                switchBlock = new BytecodeBlock()
                        .comment("lookupSwitch(hashCode(<stackValue>))")
                        .getVariable(value)
                        .append(invoke(hashCodeBinding, HASH_CODE.name()))
                        .invokeStatic(Long.class, "hashCode", int.class, long.class)
                        .putVariable(expression)
                        .append(switchBuilder.build());
                break;
            case SET_CONTAINS:
                Set<?> constantValuesSet = toFastutilHashSet(constantValues, type, functionAndTypeManager);
                Binding constant = generatorContext.getCallSiteBinder().bind(constantValuesSet, constantValuesSet.getClass());

                switchBlock = new BytecodeBlock()
                        .comment("inListSet.contains(<stackValue>)")
                        .append(new IfStatement()
                                .condition(new BytecodeBlock()
                                        .comment("value")
                                        .getVariable(value)
                                        .comment("set")
                                        .append(loadConstant(constant))
                                        // TODO: use invokeVirtual on the set instead. This requires swapping the two elements in the stack
                                        .invokeStatic(FastutilSetHelper.class, "in", boolean.class, javaType.isPrimitive() ? javaType : Object.class, constantValuesSet.getClass()))
                                .ifTrue(jump(match)));
                break;
            default:
                throw new IllegalArgumentException("Not supported switch generation case: " + switchGenerationCase);
        }

        BytecodeBlock defaultCaseBlock = buildInCase(
                generatorContext,
                scope,
                type,
                match,
                noMatch,
                value,
                defaultBucket.build(),
                true,
                isIndeterminateFunction)
                .setDescription("default");

        BytecodeBlock block = new BytecodeBlock()
                .comment("IN")
                .append(generatorContext.generate(arguments.get(0), Optional.empty()))
                .append(ifWasNullPopAndGoto(scope, end, boolean.class, javaType))
                .putVariable(value)
                .append(switchBlock)
                .visitLabel(defaultLabel)
                .append(defaultCaseBlock);

        BytecodeBlock matchBlock = new BytecodeBlock()
                .setDescription("match")
                .visitLabel(match)
                .append(generatorContext.wasNull().set(constantFalse()))
                .push(true)
                .gotoLabel(end);
        block.append(matchBlock);

        BytecodeBlock noMatchBlock = new BytecodeBlock()
                .setDescription("noMatch")
                .visitLabel(noMatch)
                .push(false)
                .gotoLabel(end);
        block.append(noMatchBlock);

        block.visitLabel(end);

        outputBlockVariable.ifPresent(output -> block.append(generateWrite(generatorContext, returnType, output)));
        return block;
    }

    public static boolean isInteger(long value)
    {
        return value == (int) value;
    }

    private static BytecodeBlock buildInCase(
            BytecodeGeneratorContext generatorContext,
            Scope scope,
            Type type,
            LabelNode matchLabel,
            LabelNode noMatchLabel,
            Variable value,
            Collection<BytecodeNode> testValues,
            boolean checkForNulls,
            BuiltInScalarFunctionImplementation isIndeterminateFunction)
    {
        Variable caseWasNull = null; // caseWasNull is set to true the first time a null in `testValues` is encountered
        if (checkForNulls) {
            caseWasNull = scope.createTempVariable(boolean.class);
        }

        BytecodeBlock caseBlock = new BytecodeBlock();

        if (checkForNulls) {
            caseBlock.putVariable(caseWasNull, false);
        }

        LabelNode elseLabel = new LabelNode("else");
        BytecodeBlock elseBlock = new BytecodeBlock()
                .visitLabel(elseLabel);

        Variable wasNull = generatorContext.wasNull();
        if (checkForNulls) {
            // Consider following expression: "ARRAY[null] IN (ARRAY[1], ARRAY[2], ARRAY[3]) => NULL"
            // All lookup values will go to the SET_CONTAINS, since neither of them is indeterminate.
            // As ARRAY[null] is not among them, the code will fall through to the defaultCaseBlock.
            // Since there is no values in the defaultCaseBlock, the defaultCaseBlock will return FALSE.
            // That is incorrect. Doing an explicit check for indeterminate is required to correctly return NULL.
            if (testValues.isEmpty()) {
                elseBlock.append(new BytecodeBlock()
                        .append(generatorContext.generateCall(INDETERMINATE.name(), isIndeterminateFunction, ImmutableList.of(value)))
                        .putVariable(wasNull));
            }
            else {
                elseBlock.append(wasNull.set(caseWasNull));
            }
        }

        elseBlock.gotoLabel(noMatchLabel);

        FunctionHandle equalsHandle = generatorContext.getFunctionManager().resolveOperator(EQUAL, fromTypes(type, type));
        BuiltInScalarFunctionImplementation equalsFunction = generatorContext.getFunctionManager().getBuiltInScalarFunctionImplementation(equalsHandle);

        BytecodeNode elseNode = elseBlock;
        for (BytecodeNode testNode : testValues) {
            LabelNode testLabel = new LabelNode("test");
            IfStatement test = new IfStatement();

            BytecodeNode equalsCall = generatorContext.generateCall(
                    EQUAL.name(),
                    equalsFunction,
                    ImmutableList.of(value, testNode));

            test.condition()
                    .visitLabel(testLabel)
                    .append(equalsCall);

            if (checkForNulls) {
                IfStatement wasNullCheck = new IfStatement("if wasNull, set caseWasNull to true, clear wasNull, pop boolean, and goto next test value");
                wasNullCheck.condition(wasNull);
                wasNullCheck.ifTrue(new BytecodeBlock()
                        .append(caseWasNull.set(constantTrue()))
                        .append(wasNull.set(constantFalse()))
                        .pop(boolean.class)
                        .gotoLabel(elseLabel));
                test.condition().append(wasNullCheck);
            }

            test.ifTrue().gotoLabel(matchLabel);
            test.ifFalse(elseNode);

            elseNode = test;
            elseLabel = testLabel;
        }
        caseBlock.append(elseNode);
        return caseBlock;
    }

    private static boolean isDeterminateConstant(RowExpression expression, MethodHandle isIndeterminateFunction)
    {
        if (!(expression instanceof ConstantExpression)) {
            return false;
        }
        ConstantExpression constantExpression = (ConstantExpression) expression;
        Object value = constantExpression.getValue();
        boolean isNull = value == null;
        if (isNull) {
            return false;
        }
        try {
            return !(boolean) isIndeterminateFunction.invoke(value, false);
        }
        catch (Throwable t) {
            throwIfUnchecked(t);
            throw new RuntimeException(t);
        }
    }
}
