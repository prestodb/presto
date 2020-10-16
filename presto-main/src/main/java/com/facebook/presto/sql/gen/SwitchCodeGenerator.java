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
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.facebook.presto.bytecode.instruction.VariableInstruction;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantTrue;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.WHEN;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.gen.SpecialFormBytecodeGenerator.generateWrite;
import static com.google.common.base.Preconditions.checkArgument;

public class SwitchCodeGenerator
        implements SpecialFormBytecodeGenerator
{
    private static final String CASE_LABEL_PREFIX = "_case_";
    private static final String RESULT_LABEL_PREFIX = "_result_";

    // TODO - move this to a RowExpressionUtil class
    private static boolean isEqualsExpression(RowExpression expression)
    {
        return expression instanceof CallExpression
                && ((CallExpression) expression).getDisplayName().equals(EQUAL.getFunctionName().getObjectName())
                && ((CallExpression) expression).getArguments().size() == 2;
    }

    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext generatorContext, Type returnType, List<RowExpression> arguments, Optional<Variable> outputBlockVariable)
    {
        Scope scope = generatorContext.getScope();
        BytecodeNode elseValue;
        List<RowExpression> whenClauses;
        RowExpression last = arguments.get(arguments.size() - 1);

        if (last instanceof SpecialFormExpression && ((SpecialFormExpression) last).getForm().equals(WHEN)) {
            whenClauses = arguments.subList(1, arguments.size());
            elseValue = new BytecodeBlock()
                    .append(generatorContext.wasNull().set(constantTrue()))
                    .pushJavaDefault(returnType.getJavaType());
        }
        else {
            whenClauses = arguments.subList(1, arguments.size() - 1);
            elseValue = generatorContext.generate(last, Optional.empty());
        }

        // determine the type of the value and result

        RowExpression value = arguments.get(0);
        Class<?> valueType = value.getType().getJavaType();

        // We generate SearchedCase as CASE TRUE WHEN p1 THEN v1 WHEN p2 THEN p2...
        boolean searchedCase = (value instanceof ConstantExpression && ((ConstantExpression) value).getType() == BOOLEAN &&
                ((ConstantExpression) value).getValue() == Boolean.TRUE);

        // evaluate the value and store it in a variable
        LabelNode elseLabel = new LabelNode("else");
        LabelNode endLabel = new LabelNode("end");

        BytecodeBlock block = new BytecodeBlock();
        Optional<BytecodeNode> getTempVariableNode;
        if (!searchedCase) {
            BytecodeNode valueBytecode = generatorContext.generate(value, Optional.empty());
            Variable tempVariable = scope.createTempVariable(valueType);
            block.append(valueBytecode)
                    .append(BytecodeUtils.ifWasNullClearPopAndGoto(scope, elseLabel, void.class, valueType))
                    .putVariable(tempVariable);
            getTempVariableNode = Optional.of(VariableInstruction.loadVariable(tempVariable));
        }
        else {
            getTempVariableNode = Optional.empty();
        }

        Variable wasNull = generatorContext.wasNull();
        block.putVariable(wasNull, false);

        Map<RowExpression, LabelNode> resultLabels = new HashMap<RowExpression, LabelNode>();
        // We already know the P1 .. Pn are all boolean just call them and search for true (false/null don't matter).
        for (RowExpression clause : whenClauses) {
            checkArgument(clause instanceof SpecialFormExpression && ((SpecialFormExpression) clause).getForm().equals(WHEN));

            RowExpression operand = ((SpecialFormExpression) clause).getArguments().get(0);
            BytecodeNode operandBytecode;

            if (searchedCase) {
                operandBytecode = generatorContext.generate(operand, Optional.empty());
            }
            else {
                // call equals(value, operandBytecode)
                FunctionHandle equalsFunction = generatorContext.getFunctionManager().resolveOperator(EQUAL, fromTypes(value.getType(), operand.getType()));
                operandBytecode = generatorContext.generateCall(
                        EQUAL.name(),
                        generatorContext.getFunctionManager().getBuiltInScalarFunctionImplementation(equalsFunction),
                        ImmutableList.of(
                                generatorContext.generate(operand,
                                Optional.empty()),
                                getTempVariableNode.get()));
            }

            block.append(operandBytecode);

            IfStatement ifWasNull = new IfStatement().condition(wasNull);

            ifWasNull.ifTrue()
                    .putVariable(wasNull, false)
                    .pop(Boolean.class); // pop the result of the predicate eval

            // Here the TOS  is the result of the predicate.
            RowExpression result = ((SpecialFormExpression) clause).getArguments().get(1);
            LabelNode target = resultLabels.get(result);
            if (target == null) {
                target = new LabelNode(RESULT_LABEL_PREFIX + resultLabels.size());
                resultLabels.put(result, target);
            }

            ifWasNull.ifFalse().ifTrueGoto(target);
            block.append(ifWasNull);
        }

        // Here we evaluate the else result.
        block.visitLabel(elseLabel)
                .append(elseValue)
                .gotoLabel(endLabel);

        // Now generate the result expression code.
        for (Map.Entry<RowExpression, LabelNode> resultLabel : resultLabels.entrySet()) {
            block.visitLabel(resultLabel.getValue())
                    .append(generatorContext.generate(resultLabel.getKey(), Optional.empty()))
                    .gotoLabel(endLabel);
        }

        block.visitLabel(endLabel);
        outputBlockVariable.ifPresent(output -> block.append(generateWrite(generatorContext, returnType, output)));
        return block;
    }
}
