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

import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.base.Preconditions;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.Variable;

import java.util.List;

import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.instruction.Constant.loadBoolean;

public class IsNullCodeGenerator
        implements BytecodeGenerator
{
    @Override
    public BytecodeNode generateExpression(Signature signature, BytecodeGeneratorContext generatorContext, Type returnType, List<RowExpression> arguments)
    {
        Preconditions.checkArgument(arguments.size() == 1);

        RowExpression argument = arguments.get(0);
        if (argument.getType().equals(UNKNOWN)) {
            return loadBoolean(true);
        }

        BytecodeNode value = generatorContext.generate(argument);

        // evaluate the expression, pop the produced value, and load the null flag
        Variable wasNull = generatorContext.wasNull();
        BytecodeBlock block = new BytecodeBlock()
                .comment("is null")
                .append(value)
                .pop(argument.getType().getJavaType())
                .append(wasNull);

        // clear the null flag
        block.append(wasNull.set(constantFalse()));

        return block;
    }
}
