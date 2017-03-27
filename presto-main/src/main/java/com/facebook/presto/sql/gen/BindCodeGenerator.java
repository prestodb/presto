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
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.List;

import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newArray;
import static com.facebook.presto.sql.gen.BytecodeUtils.boxPrimitiveIfNecessary;

public class BindCodeGenerator
        implements BytecodeGenerator
{
    @Override
    public BytecodeNode generateExpression(Signature signature, BytecodeGeneratorContext context, Type returnType, List<RowExpression> arguments)
    {
        BytecodeBlock block = new BytecodeBlock().setDescription("Partial apply");
        Scope scope = context.getScope();

        Variable wasNull = scope.getVariable("wasNull");

        Class<?> valueType = Primitives.wrap(arguments.get(0).getType().getJavaType());
        Variable valueVariable = scope.createTempVariable(valueType);
        block.append(context.generate(arguments.get(0)));
        block.append(boxPrimitiveIfNecessary(scope, valueType));
        block.putVariable(valueVariable);
        block.append(wasNull.set(constantFalse()));

        Variable functionVariable = scope.createTempVariable(MethodHandle.class);
        block.append(context.generate(arguments.get(1)));
        block.append(
                new IfStatement()
                        .condition(wasNull)
                        // ifTrue: do nothing i.e. Leave the null MethodHandle on the stack, and leave the wasNull variable set to true
                        .ifFalse(
                                new BytecodeBlock()
                                        .putVariable(functionVariable)
                                        .append(invokeStatic(
                                                MethodHandles.class,
                                                "insertArguments",
                                                MethodHandle.class,
                                                functionVariable,
                                                constantInt(0),
                                                newArray(type(Object[].class), ImmutableList.of(valueVariable.cast(Object.class)))))));

        return block;
    }
}
