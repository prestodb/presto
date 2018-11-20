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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.LambdaDefinitionExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.facebook.presto.sql.relational.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.BytecodeVisitor;
import io.airlift.bytecode.MethodGenerationContext;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.slice.Slice;
import org.objectweb.asm.MethodVisitor;

import java.util.List;
import java.util.function.BiFunction;

import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;
import static java.util.Objects.requireNonNull;

class InputReferenceCompiler
        implements RowExpressionVisitor<BytecodeNode, Scope>
{
    private final BiFunction<Scope, Integer, BytecodeExpression> blockResolver;
    private final BiFunction<Scope, Integer, BytecodeExpression> positionResolver;
    private final CallSiteBinder callSiteBinder;

    public InputReferenceCompiler(
            BiFunction<Scope, Integer, BytecodeExpression> blockResolver,
            BiFunction<Scope, Integer, BytecodeExpression> positionResolver,
            CallSiteBinder callSiteBinder)
    {
        this.blockResolver = requireNonNull(blockResolver, "blockResolver is null");
        this.positionResolver = requireNonNull(positionResolver, "positionResolver is null");
        this.callSiteBinder = requireNonNull(callSiteBinder, "callSiteBinder is null");
    }

    public static BytecodeNode generateInputReference(CallSiteBinder callSiteBinder, Scope scope, Type type, BytecodeExpression block, BytecodeExpression position)
    {
        return new InputReferenceNode(callSiteBinder, scope, type, block, position);
    }

    @Override
    public BytecodeNode visitInputReference(InputReferenceExpression node, Scope scope)
    {
        int field = node.getField();
        Type type = node.getType();

        BytecodeExpression block = blockResolver.apply(scope, field);
        BytecodeExpression position = positionResolver.apply(scope, field);

        return generateInputReference(callSiteBinder, scope, type, block, position);
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

    @Override
    public BytecodeNode visitLambda(LambdaDefinitionExpression lambda, Scope context)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BytecodeNode visitVariableReference(VariableReferenceExpression reference, Scope context)
    {
        throw new UnsupportedOperationException();
    }

    static class InputReferenceNode
            implements BytecodeNode
    {
        private final BytecodeNode body;
        private final BytecodeExpression block;
        private final BytecodeExpression position;

        private InputReferenceNode(CallSiteBinder callSiteBinder, Scope scope, Type type, BytecodeExpression block, BytecodeExpression position)
        {
            // Generate body based on block and position
            Variable wasNullVariable = scope.getVariable("wasNull");
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

            this.body = ifStatement;
            this.block = block;
            this.position = position;
        }

        @Override
        public List<BytecodeNode> getChildNodes()
        {
            return ImmutableList.of();
        }

        @Override
        public void accept(MethodVisitor visitor, MethodGenerationContext generationContext)
        {
            body.accept(visitor, generationContext);
        }

        @Override
        public <T> T accept(BytecodeNode parent, BytecodeVisitor<T> visitor)
        {
            return visitor.visitIf(parent, (IfStatement) body);
        }

        public BytecodeNode produceBlockAndPosition()
        {
            BytecodeBlock blockAndPosition = new BytecodeBlock();
            blockAndPosition.append(block);
            blockAndPosition.append(position);
            return blockAndPosition;
        }
    }
}
