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
package io.prestosql.sql.gen;

import com.google.common.base.Preconditions;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.prestosql.metadata.Signature;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.relational.RowExpression;

import java.util.List;

import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;

public class IfCodeGenerator
        implements BytecodeGenerator
{
    @Override
    public BytecodeNode generateExpression(Signature signature, BytecodeGeneratorContext context, Type returnType, List<RowExpression> arguments)
    {
        Preconditions.checkArgument(arguments.size() == 3);

        Variable wasNull = context.wasNull();
        BytecodeBlock condition = new BytecodeBlock()
                .append(context.generate(arguments.get(0)))
                .comment("... and condition value was not null")
                .append(wasNull)
                .invokeStatic(CompilerOperations.class, "not", boolean.class, boolean.class)
                .invokeStatic(CompilerOperations.class, "and", boolean.class, boolean.class, boolean.class)
                .append(wasNull.set(constantFalse()));

        return new IfStatement()
                .condition(condition)
                .ifTrue(context.generate(arguments.get(1)))
                .ifFalse(context.generate(arguments.get(2)));
    }
}
