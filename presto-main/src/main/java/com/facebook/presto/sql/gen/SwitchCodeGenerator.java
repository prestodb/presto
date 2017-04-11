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
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantTrue;

public class SwitchCodeGenerator
        implements BytecodeGenerator
{
    @Override
    public BytecodeNode generateExpression(Signature signature, BytecodeGeneratorContext generatorContext, Type returnType, List<RowExpression> arguments)
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
        BytecodeNode valueBytecode = generatorContext.generate(value);
        BytecodeNode elseValue;

        List<RowExpression> whenClauses;
        RowExpression last = arguments.get(arguments.size() - 1);
        if (last instanceof CallExpression && ((CallExpression) last).getSignature().getName().equals("WHEN")) {
            whenClauses = arguments.subList(1, arguments.size());
            elseValue = new BytecodeBlock()
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
        BytecodeBlock block = new BytecodeBlock()
                .append(valueBytecode)
                .append(BytecodeUtils.ifWasNullClearPopAndGoto(scope, nullValue, void.class, valueType))
                .putVariable(tempVariable);

        BytecodeNode getTempVariableNode = VariableInstruction.loadVariable(tempVariable);

        // build the statements
        elseValue = new BytecodeBlock().visitLabel(nullValue).append(elseValue);
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
            BytecodeNode equalsCall = generatorContext.generateCall(
                    equalsFunction.getName(),
                    generatorContext.getRegistry().getScalarFunctionImplementation(equalsFunction),
                    ImmutableList.of(generatorContext.generate(operand), getTempVariableNode));

            BytecodeBlock condition = new BytecodeBlock()
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
