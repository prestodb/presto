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
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.byteCode.instruction.VariableInstruction;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantFalse;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantTrue;

public class SwitchCodeGenerator
        implements ByteCodeGenerator
{
    @Override
    public ByteCodeNode generateExpression(Signature signature, ByteCodeGeneratorContext generatorContext, Type returnType, List<RowExpression> arguments)
    {
        // TODO: compile as
        /*
            hashCode = hashCode(<value>)

            // all constant expressions before a non-constant
            switch (hashCode) {
                case ...:
                    if (<value> == <constant1>) {
                       ...
                    }
                    else if (<value> == <constant2>) {
                       ...
                    }
                    else if (...) {
                    }
                case ...:
                    ...
            }

            if (<value> == <non-constant1>) {
                ...
            }
            else if (<value> == <non-constant2>) {
                ...
            }
            ...

            // repeat with next sequence of constant expressions
         */

        Scope scope = generatorContext.getScope();

        // process value, else, and all when clauses
        RowExpression value = arguments.get(0);
        ByteCodeNode valueBytecode = generatorContext.generate(value);
        ByteCodeNode elseValue;

        List<RowExpression> whenClauses;
        RowExpression last = arguments.get(arguments.size() - 1);
        if (last instanceof CallExpression && ((CallExpression) last).getSignature().getName().equals("WHEN")) {
            whenClauses = arguments.subList(1, arguments.size());
            elseValue = new ByteCodeBlock()
                    .append(generatorContext.wasNull().set(constantTrue()))
                    .pushJavaDefault(returnType.getJavaType());
        }
        else {
            whenClauses = arguments.subList(1, arguments.size() - 1);
            elseValue = generatorContext.generate(last);
        }

        // determine the type of the value and result
        Class<?> valueType = value.getType().getJavaType();

        // evaluate the value and store it in a variable
        LabelNode nullValue = new LabelNode("nullCondition");
        Variable tempVariable = scope.createTempVariable(valueType);
        ByteCodeBlock block = new ByteCodeBlock()
                .append(valueBytecode)
                .append(ByteCodeUtils.ifWasNullClearPopAndGoto(scope, nullValue, void.class, valueType))
                .putVariable(tempVariable);

        ByteCodeNode getTempVariableNode = VariableInstruction.loadVariable(tempVariable);

        // build the statements
        elseValue = new ByteCodeBlock().visitLabel(nullValue).append(elseValue);
        // reverse list because current if statement builder doesn't support if/else so we need to build the if statements bottom up
        for (RowExpression clause : Lists.reverse(whenClauses)) {
            Preconditions.checkArgument(clause instanceof CallExpression && ((CallExpression) clause).getSignature().getName().equals("WHEN"));

            RowExpression operand = ((CallExpression) clause).getArguments().get(0);
            RowExpression result = ((CallExpression) clause).getArguments().get(1);

            // call equals(value, operand)
            Signature equalsFunction = generatorContext.getRegistry().resolveOperator(OperatorType.EQUAL, ImmutableList.of(value.getType(), operand.getType()));

            // TODO: what if operand is null? It seems that the call will return "null" (which is cleared below)
            // and the code only does the right thing because the value in the stack for that scenario is
            // Java's default for boolean == false
            // This code should probably be checking for wasNull after the call and "failing" the equality
            // check if wasNull is true
            ByteCodeNode equalsCall = generatorContext.generateCall(
                    equalsFunction.getName(),
                    generatorContext.getRegistry().getScalarFunctionImplementation(equalsFunction),
                    ImmutableList.of(generatorContext.generate(operand), getTempVariableNode));

            ByteCodeBlock condition = new ByteCodeBlock()
                    .append(equalsCall)
                    .append(generatorContext.wasNull().set(constantFalse()));

            elseValue = new IfStatement("when")
                    .condition(condition)
                    .ifTrue(generatorContext.generate(result))
                    .ifFalse(elseValue);
        }

        return block.append(elseValue);
    }
}
