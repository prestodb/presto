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
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;

import java.util.function.BiFunction;

import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;
import static java.util.Objects.requireNonNull;

class InputReferenceCompiler
        implements RowExpressionVisitor<Scope, BytecodeNode>
{
    private final BiFunction<Scope, Integer, BytecodeExpression> blockResolver;
    private final BiFunction<Scope, Integer, BytecodeExpression> positionResolver;
    private final Variable wasNullVariable;
    private final CallSiteBinder callSiteBinder;

    public InputReferenceCompiler(
            BiFunction<Scope, Integer, BytecodeExpression> blockResolver,
            BiFunction<Scope, Integer, BytecodeExpression> positionResolver,
            Variable wasNullVariable,
            CallSiteBinder callSiteBinder)
    {
        this.blockResolver = requireNonNull(blockResolver, "blockResolver is null");
        this.positionResolver = requireNonNull(positionResolver, "positionResolver is null");
        this.wasNullVariable = requireNonNull(wasNullVariable, "wasNullVariable is null");
        this.callSiteBinder = requireNonNull(callSiteBinder, "callSiteBinder is null");
    }

    @Override
    public BytecodeNode visitInputReference(InputReferenceExpression node, Scope scope)
    {
        int field = node.getField();
        Type type = node.getType();

        BytecodeExpression block = blockResolver.apply(scope, field);
        BytecodeExpression position = positionResolver.apply(scope, field);

        Class<?> javaType = type.getJavaType();
        if (!javaType.isPrimitive() && javaType != Slice.class) {
            javaType = Object.class;
        }

        IfStatement ifStatement = new IfStatement();
        ifStatement.condition(block.invoke("isNull", boolean.class, position));

        ifStatement.ifTrue()
                .putVariable(wasNullVariable, true)
                .pushJavaDefault(javaType);

        String methodName = "get" + Primitives.wrap(javaType).getSimpleName();
        ifStatement.ifFalse(constantType(callSiteBinder, type).invoke(methodName, javaType, block, position));

        return ifStatement;
    }

    @Override
    public BytecodeNode visitCall(CallExpression call, Scope scope)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public BytecodeNode visitConstant(ConstantExpression literal, Scope scope)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
