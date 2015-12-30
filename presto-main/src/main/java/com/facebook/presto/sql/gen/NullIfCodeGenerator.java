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
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantTrue;
import static com.facebook.presto.type.TypeRegistry.getCommonSuperTypeSignature;

public class NullIfCodeGenerator
        implements BytecodeGenerator
{
    @Override
    public BytecodeNode generateExpression(Signature signature, BytecodeGeneratorContext generatorContext, Type returnType, List<RowExpression> arguments)
    {
        Scope scope = generatorContext.getScope();

        RowExpression first = arguments.get(0);
        RowExpression second = arguments.get(1);

        LabelNode notMatch = new LabelNode("notMatch");

        // push first arg on the stack
        BytecodeBlock block = new BytecodeBlock()
                .comment("check if first arg is null")
                .append(generatorContext.generate(first))
                .append(BytecodeUtils.ifWasNullPopAndGoto(scope, notMatch, void.class));

        Type firstType = first.getType();
        Type secondType = second.getType();

        // this is a hack! We shouldn't be determining type coercions at this point, but there's no way
        // around it in the current expression AST
        TypeSignature commonType = getCommonSuperTypeSignature(firstType.getTypeSignature(), secondType.getTypeSignature()).get();

        // if (equal(cast(first as <common type>), cast(second as <common type>))
        Signature operatorSignature = generatorContext.getRegistry().resolveOperator(OperatorType.EQUAL, ImmutableList.of(firstType, secondType));
        ScalarFunctionImplementation equalsFunction = generatorContext.getRegistry().getScalarFunctionImplementation(operatorSignature);
        BytecodeNode equalsCall = generatorContext.generateCall(
                operatorSignature.getName(),
                equalsFunction,
                ImmutableList.of(
                        cast(generatorContext, new BytecodeBlock().dup(firstType.getJavaType()), firstType.getTypeSignature(), commonType),
                        cast(generatorContext, generatorContext.generate(second), secondType.getTypeSignature(), commonType)));

        BytecodeBlock conditionBlock = new BytecodeBlock()
                .append(equalsCall)
                .append(BytecodeUtils.ifWasNullClearPopAndGoto(scope, notMatch, void.class, boolean.class));

        // if first and second are equal, return null
        BytecodeBlock trueBlock = new BytecodeBlock()
                .append(generatorContext.wasNull().set(constantTrue()))
                .pop(first.getType().getJavaType())
                .pushJavaDefault(first.getType().getJavaType());

        // else return first (which is still on the stack
        block.append(new IfStatement()
                .condition(conditionBlock)
                .ifTrue(trueBlock)
                .ifFalse(notMatch));

        return block;
    }

    private BytecodeNode cast(BytecodeGeneratorContext generatorContext, BytecodeNode argument, TypeSignature fromType, TypeSignature toType)
    {
        Signature function = generatorContext
            .getRegistry()
            .getCoercion(fromType, toType);

        // TODO: do we need a full function call? (nullability checks, etc)
        return generatorContext.generateCall(function.getName(), generatorContext.getRegistry().getScalarFunctionImplementation(function), ImmutableList.of(argument));
    }
}
