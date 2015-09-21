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
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.type.UnknownType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantFalse;
import static com.facebook.presto.sql.gen.ByteCodeUtils.invoke;

public class IsDistinctFromCodeGenerator
        implements ByteCodeGenerator
{
    @Override
    public ByteCodeNode generateExpression(Signature signature, ByteCodeGeneratorContext generatorContext, Type returnType, List<RowExpression> arguments)
    {
        Preconditions.checkArgument(arguments.size() == 2);

        Variable wasNull = generatorContext.wasNull();

        RowExpression left = arguments.get(0);
        RowExpression right = arguments.get(1);

        Type leftType = left.getType();
        Type rightType = right.getType();

        Signature equalsSignature = generatorContext.getRegistry().resolveOperator(OperatorType.EQUAL, ImmutableList.of(leftType, rightType));
        MethodHandle methodHandle = generatorContext
                .getRegistry()
                .getScalarFunctionImplementation(equalsSignature)
                .getMethodHandle();

        Binding binding = generatorContext
                .getCallSiteBinder()
                .bind(methodHandle);

        ByteCodeNode equalsCall = new ByteCodeBlock()
                .comment("equals(%s, %s)", leftType, rightType)
                .append(invoke(binding, equalsSignature));

        ByteCodeNode neitherSideIsNull;
        if (leftType instanceof UnknownType || rightType instanceof UnknownType) {
            // the generated block should be unreachable. However, a boolean need to be pushed to balance the stack
            neitherSideIsNull = new ByteCodeBlock()
                    .comment("unreachable code")
                    .push(false);
        }
        else {
            // This code assumes that argument and return type are not @Nullable.
            // It is not the case for UnknownType, and introduces Verification Error.
            // And it is hard to imagine that making it work with @Nullable will be useful in any other cases;
            neitherSideIsNull = new ByteCodeBlock()
                    .append(equalsCall)
                    .invokeStatic(CompilerOperations.class, "not", boolean.class, boolean.class);
        }

        ByteCodeBlock block = new ByteCodeBlock()
                .comment("IS DISTINCT FROM")
                .comment("left")
                .append(generatorContext.generate(left))
                .append(new IfStatement()
                        .condition(wasNull)
                        .ifTrue(new ByteCodeBlock()
                                .pop(leftType.getJavaType())
                                .append(wasNull.set(constantFalse()))
                                .comment("right is not null")
                                .append(generatorContext.generate(right))
                                .pop(rightType.getJavaType())
                                .append(wasNull)
                                .invokeStatic(CompilerOperations.class, "not", boolean.class, boolean.class))
                        .ifFalse(new ByteCodeBlock()
                                .comment("right")
                                .append(generatorContext.generate(right))
                                .append(new IfStatement()
                                        .condition(wasNull)
                                        .ifTrue(new ByteCodeBlock()
                                                .pop(leftType.getJavaType())
                                                .pop(rightType.getJavaType())
                                                .push(true))
                                        .ifFalse(neitherSideIsNull))))
                .append(wasNull.set(constantFalse()));

        return block;
    }
}
