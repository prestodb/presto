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

import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.type.Type;

import java.util.List;
import java.util.Optional;

public interface SpecialFormBytecodeGenerator
{
    BytecodeNode generateExpression(BytecodeGeneratorContext context, Type returnType, List<RowExpression> arguments, Optional<Variable> outputBlockVariable);

    static BytecodeNode generateWrite(BytecodeGeneratorContext context, Type returnType, Variable outputBlock)
    {
        return BytecodeUtils.generateWrite(
                context.getCallSiteBinder(),
                context.getScope(),
                context.getScope().getVariable("wasNull"),
                returnType,
                outputBlock);
    }
}
