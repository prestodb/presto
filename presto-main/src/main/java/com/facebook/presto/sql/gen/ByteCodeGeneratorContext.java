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
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.sql.relational.RowExpression;

import static com.google.common.base.Preconditions.checkNotNull;

public class ByteCodeGeneratorContext
{
    private final NewByteCodeExpressionVisitor byteCodeGenerator;
    private final CompilerContext context;
    private final BootstrapFunctionBinder bootstrapFunctionBinder;
    private final ByteCodeNode getSessionByteCode;

    public ByteCodeGeneratorContext(
            NewByteCodeExpressionVisitor byteCodeGenerator,
            CompilerContext context,
            BootstrapFunctionBinder bootstrapFunctionBinder,
            ByteCodeNode getSessionByteCode)
    {
        checkNotNull(byteCodeGenerator, "byteCodeGenerator is null");
        checkNotNull(context, "context is null");
        checkNotNull(bootstrapFunctionBinder, "bootstrapFunctionBinder is null");
        checkNotNull(getSessionByteCode, "getSessionByteCode is null");

        this.byteCodeGenerator = byteCodeGenerator;
        this.context = context;
        this.bootstrapFunctionBinder = bootstrapFunctionBinder;
        this.getSessionByteCode = getSessionByteCode;
    }

    public CompilerContext getContext()
    {
        return context;
    }

    public BootstrapFunctionBinder getBootstrapBinder()
    {
        return bootstrapFunctionBinder;
    }

    public ByteCodeNode generate(RowExpression expression)
    {
        return expression.accept(byteCodeGenerator, context);
    }

    public ByteCodeNode generateGetSession()
    {
        return getSessionByteCode;
    }
}
