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

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.sql.gen.ByteCodeUtils.invoke;

public class IsDistinctFromCodeGenerator
        implements ByteCodeGenerator
{
    @Override
    public ByteCodeNode generateExpression(Signature signature, ByteCodeGeneratorContext generatorContext, Type returnType, List<RowExpression> arguments)
    {
        Preconditions.checkArgument(arguments.size() == 2);

        CompilerContext context = generatorContext.getContext();

        RowExpression left = arguments.get(0);
        RowExpression right = arguments.get(1);

        Type leftType = left.getType();
        Type rightType = right.getType();

        FunctionInfo operator = generatorContext
                .getRegistry()
                .resolveOperator(OperatorType.EQUAL, ImmutableList.of(leftType, rightType));

        Binding binding = generatorContext
                .getCallSiteBinder()
                .bind(operator.getMethodHandle());

        ByteCodeNode equalsCall = new Block(context)
                .comment("equals(%s, %s)", leftType, rightType)
                .append(invoke(generatorContext.getContext(), binding, operator.getSignature()));

        Block block = new Block(context)
                .comment("IS DISTINCT FROM")
                .comment("left")
                .append(generatorContext.generate(left))
                .append(new IfStatement(context,
                        new Block(context).getVariable("wasNull"),
                        new Block(context)
                                .pop(leftType.getJavaType())
                                .putVariable("wasNull", false)
                                .comment("right is not null")
                                .append(generatorContext.generate(right))
                                .pop(rightType.getJavaType())
                                .getVariable("wasNull")
                                .invokeStatic(CompilerOperations.class, "not", boolean.class, boolean.class),
                        new Block(context)
                                .comment("right")
                                .append(generatorContext.generate(right))
                                .append(new IfStatement(context,
                                        new Block(context).getVariable("wasNull"),
                                        new Block(context)
                                                .pop(leftType.getJavaType())
                                                .pop(rightType.getJavaType())
                                                .push(true),
                                        new Block(context)
                                                .append(equalsCall)
                                                .invokeStatic(CompilerOperations.class, "not", boolean.class, boolean.class)))))
                .putVariable("wasNull", false);

        return block;
    }
}
