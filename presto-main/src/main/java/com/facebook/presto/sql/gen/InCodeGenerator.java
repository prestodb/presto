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

import com.facebook.presto.byteCode.ByteCodeBlock;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.control.LookupSwitch;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DateType;
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

import static com.facebook.presto.byteCode.control.LookupSwitch.lookupSwitchBuilder;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantFalse;
import static com.facebook.presto.byteCode.instruction.JumpInstruction.jump;
import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.gen.ByteCodeUtils.ifWasNullPopAndGoto;
import static com.facebook.presto.sql.gen.ByteCodeUtils.invoke;
import static com.facebook.presto.sql.gen.ByteCodeUtils.loadConstant;
import static com.facebook.presto.util.FastutilSetHelper.toFastutilHashSet;
import static java.util.Objects.requireNonNull;

public class InCodeGenerator
        implements ByteCodeGenerator
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

        if (!(type instanceof BigintType || type instanceof DateType)) {
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
            long longConstant = (Long) constant;
            if (longConstant < Integer.MIN_VALUE || longConstant > Integer.MAX_VALUE) {
                return SwitchGenerationCase.HASH_SWITCH;
            }
        }
        return SwitchGenerationCase.DIRECT_SWITCH;
    }

    @Override
    public ByteCodeNode generateExpression(Signature signature, ByteCodeGeneratorContext generatorContext, Type returnType, List<RowExpression> arguments)
    {
        ByteCodeNode value = generatorContext.generate(arguments.get(0));

        List<RowExpression> values = arguments.subList(1, arguments.size());

        ImmutableList.Builder<ByteCodeNode> valuesByteCode = ImmutableList.builder();
        for (int i = 1; i < arguments.size(); i++) {
            ByteCodeNode testNode = generatorContext.generate(arguments.get(i));
            valuesByteCode.add(testNode);
        }

        Type type = arguments.get(0).getType();
        Class<?> javaType = type.getJavaType();

        SwitchGenerationCase switchGenerationCase  = checkSwitchGenerationCase(type, values);

        Signature hashCodeSignature = internalOperator(HASH_CODE, BIGINT, ImmutableList.of(type));
        MethodHandle hashCodeFunction = generatorContext.getRegistry().getScalarFunctionImplementation(hashCodeSignature).getMethodHandle();

        ImmutableListMultimap.Builder<Integer, ByteCodeNode> hashBucketsBuilder = ImmutableListMultimap.builder();
        ImmutableList.Builder<ByteCodeNode> defaultBucket = ImmutableList.builder();
        ImmutableSet.Builder<Object> constantValuesBuilder = ImmutableSet.builder();

        for (RowExpression testValue : values) {
            ByteCodeNode testByteCode = generatorContext.generate(testValue);

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
                            int hashCode = Ints.checkedCast((Long) hashCodeFunction.invoke(object));
                            hashBucketsBuilder.put(hashCode, testByteCode);
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
                defaultBucket.add(testByteCode);
            }
        }
        ImmutableListMultimap<Integer, ByteCodeNode> hashBuckets = hashBucketsBuilder.build();
        ImmutableSet<Object> constantValues = constantValuesBuilder.build();

        LabelNode end = new LabelNode("end");
        LabelNode match = new LabelNode("match");
        LabelNode noMatch = new LabelNode("noMatch");

        LabelNode defaultLabel = new LabelNode("default");

        Scope scope = generatorContext.getScope();

        ByteCodeNode switchBlock;
        ByteCodeBlock switchCaseBlocks = new ByteCodeBlock();
        LookupSwitch.LookupSwitchBuilder switchBuilder = lookupSwitchBuilder();
        switch (switchGenerationCase) {
            case DIRECT_SWITCH:
                // A white-list is used to select types eligible for DIRECT_SWITCH.
                // For these types, it's safe to not use presto HASH_CODE and EQUAL operator.
                for (Object constantValue : constantValues) {
                    switchBuilder.addCase(Ints.checkedCast((Long) constantValue), match);
                }
                switchBuilder.defaultCase(defaultLabel);
                switchBlock = new ByteCodeBlock()
                        .comment("lookupSwitch(<stackValue>))")
                        .dup(javaType)
                        .append(new IfStatement()
                                .condition(new ByteCodeBlock()
                                        .dup(javaType)
                                        .invokeStatic(InCodeGenerator.class, "isInteger", boolean.class, long.class))
                                .ifFalse(new ByteCodeBlock()
                                        .pop(javaType)
                                        .gotoLabel(defaultLabel)))
                        .longToInt()
                        .append(switchBuilder.build());
                break;
            case HASH_SWITCH:
                for (Map.Entry<Integer, Collection<ByteCodeNode>> bucket : hashBuckets.asMap().entrySet()) {
                    LabelNode label = new LabelNode("inHash" + bucket.getKey());
                    switchBuilder.addCase(bucket.getKey(), label);
                    Collection<ByteCodeNode> testValues = bucket.getValue();

                    ByteCodeBlock caseBlock = buildInCase(generatorContext, scope, type, label, match, defaultLabel, testValues, false);
                    switchCaseBlocks.append(caseBlock.setDescription("case " + bucket.getKey()));
                }
                switchBuilder.defaultCase(defaultLabel);
                Binding hashCodeBinding = generatorContext
                        .getCallSiteBinder()
                        .bind(hashCodeFunction);
                switchBlock = new ByteCodeBlock()
                        .comment("lookupSwitch(hashCode(<stackValue>))")
                        .dup(javaType)
                        .append(invoke(hashCodeBinding, hashCodeSignature))
                        .longToInt()
                        .append(switchBuilder.build())
                        .append(switchCaseBlocks);
                break;
            case SET_CONTAINS:
                Set<?> constantValuesSet = toFastutilHashSet(constantValues, type, registry);
                Binding constant = generatorContext.getCallSiteBinder().bind(constantValuesSet, constantValuesSet.getClass());

                switchBlock = new ByteCodeBlock()
                        .comment("inListSet.contains(<stackValue>)")
                        .append(new IfStatement()
                                .condition(new ByteCodeBlock()
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

        ByteCodeBlock defaultCaseBlock = buildInCase(generatorContext, scope, type, defaultLabel, match, noMatch, defaultBucket.build(), true).setDescription("default");

        ByteCodeBlock block = new ByteCodeBlock()
                .comment("IN")
                .append(value)
                .append(ifWasNullPopAndGoto(scope, end, boolean.class, javaType))
                .append(switchBlock)
                .append(defaultCaseBlock);

        ByteCodeBlock matchBlock = new ByteCodeBlock()
                .setDescription("match")
                .visitLabel(match)
                .pop(javaType)
                .append(generatorContext.wasNull().set(constantFalse()))
                .push(true)
                .gotoLabel(end);
        block.append(matchBlock);

        ByteCodeBlock noMatchBlock = new ByteCodeBlock()
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

    private ByteCodeBlock buildInCase(ByteCodeGeneratorContext generatorContext,
            Scope scope,
            Type type,
            LabelNode caseLabel,
            LabelNode matchLabel,
            LabelNode noMatchLabel,
            Collection<ByteCodeNode> testValues,
            boolean checkForNulls)
    {
        Variable caseWasNull = null;
        if (checkForNulls) {
            caseWasNull = scope.createTempVariable(boolean.class);
        }

        ByteCodeBlock caseBlock = new ByteCodeBlock()
                .visitLabel(caseLabel);

        if (checkForNulls) {
            caseBlock.putVariable(caseWasNull, false);
        }

        LabelNode elseLabel = new LabelNode("else");
        ByteCodeBlock elseBlock = new ByteCodeBlock()
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

        ByteCodeNode elseNode = elseBlock;
        for (ByteCodeNode testNode : testValues) {
            LabelNode testLabel = new LabelNode("test");
            IfStatement test = new IfStatement();

            test.condition()
                    .visitLabel(testLabel)
                    .dup(type.getJavaType())
                    .append(testNode);

            if (checkForNulls) {
                test.condition()
                        .append(wasNull)
                        .putVariable(caseWasNull)
                        .append(ifWasNullPopAndGoto(scope, elseLabel, void.class, type.getJavaType(), type.getJavaType()));
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
