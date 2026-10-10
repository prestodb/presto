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
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.gen.BytecodeUtils.ifWasNullPopAndGoto;
import static com.facebook.presto.sql.gen.SpecialFormBytecodeGenerator.generateWrite;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Generates {@code value BETWEEN min AND max} as {@code value >= min AND value <= max},
 * evaluating {@code value} only once. A NULL bound therefore does not make the result
 * NULL when the comparison against the other bound is false.
 */
public class BetweenCodeGenerator
        implements SpecialFormBytecodeGenerator
{
    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext generatorContext, Type returnType, List<RowExpression> arguments, Optional<Variable> outputBlockVariable)
    {
        checkArgument(arguments.size() == 3, "BETWEEN expects 3 arguments, got %s", arguments.size());
        Scope scope = generatorContext.getScope();

        RowExpression value = arguments.get(0);
        RowExpression min = arguments.get(1);
        RowExpression max = arguments.get(2);

        Variable valueVariable = scope.createTempVariable(value.getType().getJavaType());
        VariableReferenceExpression valueReference = generatorContext.createTempVariableReferenceExpression(valueVariable, value.getType());

        SpecialFormExpression expandedBetween = new SpecialFormExpression(
                AND,
                BOOLEAN,
                comparison(generatorContext.getFunctionManager(), GREATER_THAN_OR_EQUAL, valueReference, min),
                comparison(generatorContext.getFunctionManager(), LESS_THAN_OR_EQUAL, valueReference, max));

        LabelNode done = new LabelNode("done");

        BytecodeBlock block = new BytecodeBlock()
                .comment("check if value is null")
                .append(generatorContext.generate(value, Optional.empty()))
                .append(ifWasNullPopAndGoto(scope, done, boolean.class, value.getType().getJavaType()))
                .putVariable(valueVariable)
                .append(generatorContext.generate(expandedBetween, Optional.empty()))
                .visitLabel(done);

        outputBlockVariable.ifPresent(output -> block.append(generateWrite(generatorContext, returnType, output)));
        return block;
    }

    private static RowExpression comparison(FunctionAndTypeManager functionAndTypeManager, OperatorType operator, RowExpression left, RowExpression right)
    {
        return call(
                operator.name(),
                functionAndTypeManager.resolveOperator(operator, fromTypes(left.getType(), right.getType())),
                BOOLEAN,
                left,
                right);
    }
}
