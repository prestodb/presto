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
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.type.UnknownType;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class CastCodeGenerator
        implements ByteCodeGenerator
{
    @Override
    public ByteCodeNode generateExpression(Signature signature, ByteCodeGeneratorContext generatorContext, Type returnType, List<RowExpression> arguments)
    {
        RowExpression argument = arguments.get(0);

        CompilerContext context = generatorContext.getContext();
        if (argument.getType().equals(UnknownType.UNKNOWN)) {
            return new Block(context)
                    .putVariable("wasNull", true)
                    .pushJavaDefault(returnType.getJavaType());
        }

        FunctionInfo function = generatorContext
                .getRegistry()
                .getCoercion(argument.getType(), returnType);

        return generatorContext.generateCall(function, ImmutableList.of(generatorContext.generate(argument)));
    }
}
