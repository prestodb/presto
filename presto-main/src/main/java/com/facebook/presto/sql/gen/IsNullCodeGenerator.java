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
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.instruction.Constant.loadBoolean;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.sql.gen.SpecialFormBytecodeGenerator.generateWrite;

public class IsNullCodeGenerator
        implements SpecialFormBytecodeGenerator
{
    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext generatorContext, Type returnType, List<RowExpression> arguments, Optional<Variable> outputBlockVariable)
    {
        Preconditions.checkArgument(arguments.size() == 1);

        RowExpression argument = arguments.get(0);
        if (argument.getType().equals(UNKNOWN)) {
            return loadBoolean(true);
        }

        BytecodeNode value = generatorContext.generate(argument, Optional.empty());

        // evaluate the expression, pop the produced value, and load the null flag
        Variable wasNull = generatorContext.wasNull();
        BytecodeBlock block = new BytecodeBlock()
                .comment("is null")
                .append(value)
                .pop(argument.getType().getJavaType())
                .append(wasNull);

        // clear the null flag
        block.append(wasNull.set(constantFalse()));

        outputBlockVariable.ifPresent(output -> block.append(generateWrite(generatorContext, returnType, output)));
        return block;
    }
}
