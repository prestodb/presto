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
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantTrue;

public class NullIfCodeGenerator
        implements ByteCodeGenerator
{
    @Override
    public ByteCodeNode generateExpression(Signature signature, ByteCodeGeneratorContext generatorContext, Type returnType, List<RowExpression> arguments)
    {
        Scope scope = generatorContext.getScope();

        RowExpression first = arguments.get(0);
        RowExpression second = arguments.get(1);

        LabelNode notMatch = new LabelNode("notMatch");

        // push first arg on the stack
        ByteCodeBlock block = new ByteCodeBlock()
                .comment("check if first arg is null")
                .append(generatorContext.generate(first))
                .append(ByteCodeUtils.ifWasNullPopAndGoto(scope, notMatch, void.class));

        Type firstType = first.getType();
        Type secondType = second.getType();

        // this is a hack! We shouldn't be determining type coercions at this point, but there's no way
        // around it in the current expression AST
        Type commonType = FunctionRegistry.getCommonSuperType(firstType, secondType).get();

        // if (equal(cast(first as <common type>), cast(second as <common type>))
        Signature operatorSignature = generatorContext.getRegistry().resolveOperator(OperatorType.EQUAL, ImmutableList.of(firstType, secondType));
        ScalarFunctionImplementation equalsFunction = generatorContext.getRegistry().getScalarFunctionImplementation(operatorSignature);
        ByteCodeNode equalsCall = generatorContext.generateCall(
                operatorSignature.getName(),
                equalsFunction,
                ImmutableList.of(
                        cast(generatorContext, new ByteCodeBlock().dup(firstType.getJavaType()), firstType, commonType),
                        cast(generatorContext, generatorContext.generate(second), secondType, commonType)));

        ByteCodeBlock conditionBlock = new ByteCodeBlock()
                .append(equalsCall)
                .append(ByteCodeUtils.ifWasNullClearPopAndGoto(scope, notMatch, void.class, boolean.class));

        // if first and second are equal, return null
        ByteCodeBlock trueBlock = new ByteCodeBlock()
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

    private ByteCodeNode cast(ByteCodeGeneratorContext generatorContext, ByteCodeNode argument, Type fromType, Type toType)
    {
        Signature function = generatorContext
            .getRegistry()
            .getCoercion(fromType, toType);

        // TODO: do we need a full function call? (nullability checks, etc)
        return generatorContext.generateCall(function.getName(), generatorContext.getRegistry().getScalarFunctionImplementation(function), ImmutableList.of(argument));
    }
}
