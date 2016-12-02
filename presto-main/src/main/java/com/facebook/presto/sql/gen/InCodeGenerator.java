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
import com.facebook.presto.bytecode.control.LookupSwitch;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.util.FastutilSetHelper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;

import java.lang.invoke.MethodHandle;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.bytecode.control.LookupSwitch.lookupSwitchBuilder;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantTrue;
import static com.facebook.presto.bytecode.instruction.JumpInstruction.jump;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.gen.BytecodeUtils.ifWasNullPopAndGoto;
import static com.facebook.presto.sql.gen.BytecodeUtils.invoke;
import static com.facebook.presto.sql.gen.BytecodeUtils.loadConstant;
import static com.facebook.presto.util.FastutilSetHelper.toFastutilHashSet;
import static java.util.Objects.requireNonNull;

public class InCodeGenerator
        implements BytecodeGenerator
{
    private final FunctionRegistry registry;

    public InCodeGenerator(FunctionRegistry registry)
    {
        this.registry = requireNonNull(registry, "registry is null");
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
    public BytecodeNode generateExpression(Signature signature, BytecodeGeneratorContext generatorContext, Type returnType, List<RowExpression> arguments)
    {
        BytecodeNode value = generatorContext.generate(arguments.get(0));

        List<RowExpression> values = arguments.subList(1, arguments.size());

        ImmutableList.Builder<BytecodeNode> valuesBytecode = ImmutableList.builder();
        for (int i = 1; i < arguments.size(); i++) {
            BytecodeNode testNode = generatorContext.generate(arguments.get(i));
            valuesBytecode.add(testNode);
        }

        Type type = arguments.get(0).getType();
        Class<?> javaType = type.getJavaType();

        SwitchGenerationCase switchGenerationCase  = checkSwitchGenerationCase(type, values);

        Signature hashCodeSignature = internalOperator(HASH_CODE, BIGINT, ImmutableList.of(type));
        MethodHandle hashCodeFunction = generatorContext.getRegistry().getScalarFunctionImplementation(hashCodeSignature).getMethodHandle();

        ImmutableListMultimap.Builder<Integer, BytecodeNode> hashBucketsBuilder = ImmutableListMultimap.builder();
        ImmutableList.Builder<BytecodeNode> defaultBucket = ImmutableList.builder();
        ImmutableSet.Builder<Object> constantValuesBuilder = ImmutableSet.builder();

        for (RowExpression testValue : values) {
            BytecodeNode testBytecode = generatorContext.generate(testValue);

            if (testValue instanceof ConstantExpression && ((ConstantExpression) testValue).getValue() != null) {
                ConstantExpression constant = (ConstantExpression) testValue;
                Object object = constant.getValue();
                switch (switchGenerationCase) {
                    case DIRECT_SWITCH:
                    case SET_CONTAINS:
                        constantValuesBuilder.add(object);
                        break;
                    case HASH_SWITCH:
                        try {
                            int hashCode = Ints.checkedCast(Long.hashCode((Long) hashCodeFunction.invoke(object)));
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

        BytecodeNode switchBlock;
        BytecodeBlock switchCaseBlocks = new BytecodeBlock();
        LookupSwitch.LookupSwitchBuilder switchBuilder = lookupSwitchBuilder();
        switch (switchGenerationCase) {
            case DIRECT_SWITCH:
                // A white-list is used to select types eligible for DIRECT_SWITCH.
                // For these types, it's safe to not use presto HASH_CODE and EQUAL operator.
                for (Object constantValue : constantValues) {
                    switchBuilder.addCase(Ints.checkedCast((Long) constantValue), match);
                }
                switchBuilder.defaultCase(defaultLabel);
                switchBlock = new BytecodeBlock()
                        .comment("lookupSwitch(<stackValue>))")
                        .dup(javaType)
                        .append(new IfStatement()
                                .condition(new BytecodeBlock()
                                        .dup(javaType)
                                        .invokeStatic(InCodeGenerator.class, "isInteger", boolean.class, long.class))
                                .ifFalse(new BytecodeBlock()
                                        .pop(javaType)
                                        .gotoLabel(defaultLabel)))
                        .longToInt()
                        .append(switchBuilder.build());
                break;
            case HASH_SWITCH:
                for (Map.Entry<Integer, Collection<BytecodeNode>> bucket : hashBuckets.asMap().entrySet()) {
                    LabelNode label = new LabelNode("inHash" + bucket.getKey());
                    switchBuilder.addCase(bucket.getKey(), label);
                    Collection<BytecodeNode> testValues = bucket.getValue();

                    BytecodeBlock caseBlock = buildInCase(generatorContext, scope, type, label, match, defaultLabel, testValues, false);
                    switchCaseBlocks.append(caseBlock.setDescription("case " + bucket.getKey()));
                }
                switchBuilder.defaultCase(defaultLabel);
                Binding hashCodeBinding = generatorContext
                        .getCallSiteBinder()
                        .bind(hashCodeFunction);
                switchBlock = new BytecodeBlock()
                        .comment("lookupSwitch(hashCode(<stackValue>))")
                        .dup(javaType)
                        .append(invoke(hashCodeBinding, hashCodeSignature))
                        .invokeStatic(Long.class, "hashCode", int.class, long.class)
                        .append(switchBuilder.build())
                        .append(switchCaseBlocks);
                break;
            case SET_CONTAINS:
                Set<?> constantValuesSet = toFastutilHashSet(constantValues, type, registry);
                Binding constant = generatorContext.getCallSiteBinder().bind(constantValuesSet, constantValuesSet.getClass());

                switchBlock = new BytecodeBlock()
                        .comment("inListSet.contains(<stackValue>)")
                        .append(new IfStatement()
                                .condition(new BytecodeBlock()
                                        .comment("value")
                                        .dup(javaType)
                                        .comment("set")
                                        .append(loadConstant(constant))
                                        // TODO: use invokeVirtual on the set instead. This requires swapping the two elements in the stack
                                        .invokeStatic(FastutilSetHelper.class, "in", boolean.class, javaType.isPrimitive() ? javaType : Object.class, constantValuesSet.getClass()))
                                .ifTrue(jump(match)));
                break;
            default:
                throw new IllegalArgumentException("Not supported switch generation case: " + switchGenerationCase);
        }

        BytecodeBlock defaultCaseBlock = buildInCase(generatorContext, scope, type, defaultLabel, match, noMatch, defaultBucket.build(), true).setDescription("default");

        BytecodeBlock block = new BytecodeBlock()
                .comment("IN")
                .append(value)
                .append(ifWasNullPopAndGoto(scope, end, boolean.class, javaType))
                .append(switchBlock)
                .append(defaultCaseBlock);

        BytecodeBlock matchBlock = new BytecodeBlock()
                .setDescription("match")
                .visitLabel(match)
                .pop(javaType)
                .append(generatorContext.wasNull().set(constantFalse()))
                .push(true)
                .gotoLabel(end);
        block.append(matchBlock);

        BytecodeBlock noMatchBlock = new BytecodeBlock()
                .setDescription("noMatch")
                .visitLabel(noMatch)
                .pop(javaType)
                .push(false)
                .gotoLabel(end);
        block.append(noMatchBlock);

        block.visitLabel(end);

        return block;
    }

    public static boolean isInteger(long value)
    {
        return value == (int) value;
    }

    private static BytecodeBlock buildInCase(BytecodeGeneratorContext generatorContext,
            Scope scope,
            Type type,
            LabelNode caseLabel,
            LabelNode matchLabel,
            LabelNode noMatchLabel,
            Collection<BytecodeNode> testValues,
            boolean checkForNulls)
    {
        Variable caseWasNull = null; // caseWasNull is set to true the first time a null in `testValues` is encountered
        if (checkForNulls) {
            caseWasNull = scope.createTempVariable(boolean.class);
        }

        BytecodeBlock caseBlock = new BytecodeBlock()
                .visitLabel(caseLabel);

        if (checkForNulls) {
            caseBlock.putVariable(caseWasNull, false);
        }

        LabelNode elseLabel = new LabelNode("else");
        BytecodeBlock elseBlock = new BytecodeBlock()
                .visitLabel(elseLabel);

        Variable wasNull = generatorContext.wasNull();
        if (checkForNulls) {
            elseBlock.append(wasNull.set(caseWasNull));
        }

        elseBlock.gotoLabel(noMatchLabel);

        ScalarFunctionImplementation operator = generatorContext.getRegistry().getScalarFunctionImplementation(internalOperator(EQUAL, BOOLEAN, ImmutableList.of(type, type)));

        Binding equalsFunction = generatorContext
                .getCallSiteBinder()
                .bind(operator.getMethodHandle());

        BytecodeNode elseNode = elseBlock;
        for (BytecodeNode testNode : testValues) {
            LabelNode testLabel = new LabelNode("test");
            IfStatement test = new IfStatement();

            test.condition()
                    .visitLabel(testLabel)
                    .dup(type.getJavaType())
                    .append(testNode);

            if (checkForNulls) {
                IfStatement wasNullCheck = new IfStatement("if wasNull, set caseWasNull to true, clear wasNull, pop 2 values of type, and goto next test value");
                wasNullCheck.condition(wasNull);
                wasNullCheck.ifTrue(new BytecodeBlock()
                        .append(caseWasNull.set(constantTrue()))
                        .append(wasNull.set(constantFalse()))
                        .pop(type.getJavaType())
                        .pop(type.getJavaType())
                        .gotoLabel(elseLabel));
                test.condition().append(wasNullCheck);
            }
            test.condition()
                    .append(invoke(equalsFunction, EQUAL.name()));

            test.ifTrue().gotoLabel(matchLabel);
            test.ifFalse(elseNode);

            elseNode = test;
            elseLabel = testLabel;
        }
        caseBlock.append(elseNode);
        return caseBlock;
    }
}
