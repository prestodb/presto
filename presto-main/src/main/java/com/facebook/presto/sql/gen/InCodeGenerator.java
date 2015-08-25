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
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.byteCode.control.LookupSwitch.lookupSwitchBuilder;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantFalse;
import static com.facebook.presto.byteCode.instruction.JumpInstruction.jump;
import static com.facebook.presto.sql.gen.ByteCodeUtils.ifWasNullPopAndGoto;
import static com.facebook.presto.sql.gen.ByteCodeUtils.invoke;
import static com.facebook.presto.sql.gen.ByteCodeUtils.loadConstant;

public class InCodeGenerator
        implements ByteCodeGenerator
{
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

        FunctionInfo hashCodeFunction = generatorContext.getRegistry().resolveOperator(OperatorType.HASH_CODE, ImmutableList.of(type));

        ImmutableListMultimap.Builder<Integer, ByteCodeNode> hashBucketsBuilder = ImmutableListMultimap.builder();
        ImmutableList.Builder<ByteCodeNode> defaultBucket = ImmutableList.builder();
        ImmutableSet.Builder<Object> constantValuesBuilder = ImmutableSet.builder();

        for (RowExpression testValue : values) {
            ByteCodeNode testByteCode = generatorContext.generate(testValue);

            if (testValue instanceof ConstantExpression && ((ConstantExpression) testValue).getValue() != null) {
                ConstantExpression constant = (ConstantExpression) testValue;
                Object object = constant.getValue();
                constantValuesBuilder.add(object);

                try {
                    int hashCode = ((Long) hashCodeFunction.getMethodHandle().invoke(object)).intValue();
                    hashBucketsBuilder.put(hashCode, testByteCode);
                }
                catch (Throwable throwable) {
                    throw new IllegalArgumentException("Error processing IN statement: error calculating hash code for " + object, throwable);
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
        if (constantValues.size() < 1000) {
            ByteCodeBlock switchCaseBlocks = new ByteCodeBlock();
            LookupSwitch.LookupSwitchBuilder switchBuilder = lookupSwitchBuilder();
            for (Map.Entry<Integer, Collection<ByteCodeNode>> bucket : hashBuckets.asMap().entrySet()) {
                LabelNode label = new LabelNode("inHash" + bucket.getKey());
                switchBuilder.addCase(bucket.getKey(), label);
                Collection<ByteCodeNode> testValues = bucket.getValue();

                ByteCodeBlock caseBlock = buildInCase(generatorContext, scope, type, label, match, defaultLabel, testValues, false);
                switchCaseBlocks
                        .append(caseBlock.setDescription("case " + bucket.getKey()));
            }
            switchBuilder.defaultCase(defaultLabel);

            Binding hashCodeBinding = generatorContext
                    .getCallSiteBinder()
                    .bind(hashCodeFunction.getMethodHandle());

            switchBlock = new ByteCodeBlock()
                    .comment("lookupSwitch(hashCode(<stackValue>))")
                    .dup(javaType)
                    .append(invoke(hashCodeBinding, hashCodeFunction.getSignature()))
                    .longToInt()
                    .append(switchBuilder.build())
                    .append(switchCaseBlocks);
        }
        else {
            // TODO: replace Set with fastutils (or similar) primitive sets if types are primitive
            // for huge IN lists, use a Set
            Binding constant = generatorContext.getCallSiteBinder().bind(constantValues, Set.class);

            switchBlock = new ByteCodeBlock()
                    .comment("inListSet.contains(<stackValue>)")
                    .append(new IfStatement()
                            .condition(new ByteCodeBlock()
                                    .comment("value (+boxing if necessary)")
                                    .dup(javaType)
                                    .append(ByteCodeUtils.boxPrimitive(javaType))
                                    .comment("set")
                                    .append(loadConstant(constant))
                                    // TODO: use invokeVirtual on the set instead. This requires swapping the two elements in the stack
                                    .invokeStatic(CompilerOperations.class, "in", boolean.class, Object.class, Set.class))
                            .ifTrue(jump(match)));
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

        FunctionInfo operator = generatorContext.getRegistry().resolveOperator(OperatorType.EQUAL, ImmutableList.of(type, type));

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
                    .append(invoke(equalsFunction, operator.getSignature()));

            test.ifTrue().gotoLabel(matchLabel);
            test.ifFalse(elseNode);

            elseNode = test;
            elseLabel = testLabel;
        }
        caseBlock.append(elseNode);
        return caseBlock;
    }
}
