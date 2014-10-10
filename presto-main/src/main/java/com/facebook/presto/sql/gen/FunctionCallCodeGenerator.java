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

import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

public class FunctionCallCodeGenerator
        implements ByteCodeGenerator
{
    @Override
    public ByteCodeNode generateExpression(Signature signature, ByteCodeGeneratorContext context, Type returnType, List<RowExpression> arguments)
    {
        FunctionRegistry registry = context.getRegistry();

        FunctionInfo function = registry.getExactFunction(signature);
        if (function == null) {
            // TODO: temporary hack to deal with magic timestamp literal functions which don't have an "exact" form and need to be "resolved"
            function = registry.resolveFunction(QualifiedName.of(signature.getName()), signature.getArgumentTypes(), false);
        }

        Preconditions.checkArgument(function != null, "Function %s not found", signature);

        List<ByteCodeNode> argumentsByteCode = new ArrayList<>();
        for (RowExpression argument : arguments) {
            argumentsByteCode.add(context.generate(argument));
        }

        return context.generateCall(function, argumentsByteCode);
    }
}
