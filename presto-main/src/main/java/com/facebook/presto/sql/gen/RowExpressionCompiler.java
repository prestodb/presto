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
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.LambdaDefinitionExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.facebook.presto.sql.relational.VariableReferenceExpression;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantTrue;
import static com.facebook.presto.bytecode.instruction.Constant.loadBoolean;
import static com.facebook.presto.bytecode.instruction.Constant.loadDouble;
import static com.facebook.presto.bytecode.instruction.Constant.loadFloat;
import static com.facebook.presto.bytecode.instruction.Constant.loadInt;
import static com.facebook.presto.bytecode.instruction.Constant.loadLong;
import static com.facebook.presto.bytecode.instruction.Constant.loadString;
import static com.facebook.presto.sql.gen.BytecodeUtils.loadConstant;
import static com.facebook.presto.sql.relational.Signatures.BIND;
import static com.facebook.presto.sql.relational.Signatures.CAST;
import static com.facebook.presto.sql.relational.Signatures.COALESCE;
import static com.facebook.presto.sql.relational.Signatures.DEREFERENCE;
import static com.facebook.presto.sql.relational.Signatures.IF;
import static com.facebook.presto.sql.relational.Signatures.IN;
import static com.facebook.presto.sql.relational.Signatures.IS_NULL;
import static com.facebook.presto.sql.relational.Signatures.NULL_IF;
import static com.facebook.presto.sql.relational.Signatures.ROW_CONSTRUCTOR;
import static com.facebook.presto.sql.relational.Signatures.SWITCH;
import static com.facebook.presto.sql.relational.Signatures.TRY;
import static com.google.common.base.Preconditions.checkState;

public class RowExpressionCompiler
{
    private final CallSiteBinder callSiteBinder;
    private final CachedInstanceBinder cachedInstanceBinder;
    private final RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompiler;
    private final FunctionRegistry registry;
    private final PreGeneratedExpressions preGeneratedExpressions;

    RowExpressionCompiler(
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompiler,
            FunctionRegistry registry,
            PreGeneratedExpressions preGeneratedExpressions)
    {
        this.callSiteBinder = callSiteBinder;
        this.cachedInstanceBinder = cachedInstanceBinder;
        this.fieldReferenceCompiler = fieldReferenceCompiler;
        this.registry = registry;
        this.preGeneratedExpressions = preGeneratedExpressions;
    }

    public BytecodeNode compile(RowExpression rowExpression, Scope scope)
    {
        return rowExpression.accept(new Visitor(), new Context(scope));
    }

    private class Visitor
            implements RowExpressionVisitor<BytecodeNode, Context>
    {
        @Override
        public BytecodeNode visitCall(CallExpression call, Context context)
        {
            BytecodeGenerator generator;
            // special-cased in function registry
            if (call.getSignature().getName().equals(CAST)) {
                generator = new CastCodeGenerator();
            }
            else {
                switch (call.getSignature().getName()) {
                    // lazy evaluation
                    case IF:
                        generator = new IfCodeGenerator();
                        break;
                    case NULL_IF:
                        generator = new NullIfCodeGenerator();
                        break;
                    case SWITCH:
                        // (SWITCH <expr> (WHEN <expr> <expr>) (WHEN <expr> <expr>) <expr>)
                        generator = new SwitchCodeGenerator();
                        break;
                    case TRY:
                        generator = new TryCodeGenerator(preGeneratedExpressions.getTryMethodMap());
                        break;
                    // functions that take null as input
                    case IS_NULL:
                        generator = new IsNullCodeGenerator();
                        break;
                    case COALESCE:
                        generator = new CoalesceCodeGenerator();
                        break;
                    // functions that require varargs and/or complex types (e.g., lists)
                    case IN:
                        generator = new InCodeGenerator(registry);
                        break;
                    // optimized implementations (shortcircuiting behavior)
                    case "AND":
                        generator = new AndCodeGenerator();
                        break;
                    case "OR":
                        generator = new OrCodeGenerator();
                        break;
                    case DEREFERENCE:
                        generator = new DereferenceCodeGenerator();
                        break;
                    case ROW_CONSTRUCTOR:
                        generator = new RowConstructorCodeGenerator();
                        break;
                    case BIND:
                        generator = new BindCodeGenerator();
                        break;
                    default:
                        generator = new FunctionCallCodeGenerator();
                }
            }

            BytecodeGeneratorContext generatorContext = new BytecodeGeneratorContext(
                    RowExpressionCompiler.this,
                    context.getScope(),
                    callSiteBinder,
                    cachedInstanceBinder,
                    registry);

            return generator.generateExpression(call.getSignature(), generatorContext, call.getType(), call.getArguments());
        }

        @Override
        public BytecodeNode visitConstant(ConstantExpression constant, Context context)
        {
            Object value = constant.getValue();
            Class<?> javaType = constant.getType().getJavaType();

            BytecodeBlock block = new BytecodeBlock();
            if (value == null) {
                return block.comment("constant null")
                        .append(context.getScope().getVariable("wasNull").set(constantTrue()))
                        .pushJavaDefault(javaType);
            }

            // use LDC for primitives (boolean, short, int, long, float, double)
            block.comment("constant " + constant.getType().getTypeSignature());
            if (javaType == boolean.class) {
                return block.append(loadBoolean((Boolean) value));
            }
            if (javaType == byte.class || javaType == short.class || javaType == int.class) {
                return block.append(loadInt(((Number) value).intValue()));
            }
            if (javaType == long.class) {
                return block.append(loadLong((Long) value));
            }
            if (javaType == float.class) {
                return block.append(loadFloat((Float) value));
            }
            if (javaType == double.class) {
                return block.append(loadDouble((Double) value));
            }
            if (javaType == String.class) {
                return block.append(loadString((String) value));
            }
            if (javaType == void.class) {
                return block;
            }

            // bind constant object directly into the call-site using invoke dynamic
            Binding binding = callSiteBinder.bind(value, constant.getType().getJavaType());

            return new BytecodeBlock()
                    .setDescription("constant " + constant.getType())
                    .comment(constant.toString())
                    .append(loadConstant(binding));
        }

        @Override
        public BytecodeNode visitInputReference(InputReferenceExpression node, Context context)
        {
            return fieldReferenceCompiler.visitInputReference(node, context.getScope());
        }

        @Override
        public BytecodeNode visitLambda(LambdaDefinitionExpression lambda, Context context)
        {
            checkState(preGeneratedExpressions.getLambdaFieldMap().containsKey(lambda), "lambda expressions map does not contain this lambda definition");

            return context.getScope().getThis().getField(preGeneratedExpressions.getLambdaFieldMap().get(lambda).getInstanceField())
                    .invoke("bindTo", MethodHandle.class, context.getScope().getVariable("session").cast(Object.class));
        }

        @Override
        public BytecodeNode visitVariableReference(VariableReferenceExpression reference, Context context)
        {
            return fieldReferenceCompiler.visitVariableReference(reference, context.getScope());
        }
    }

    private static class Context
    {
        private final Scope scope;

        public Context(Scope scope)
        {
            this.scope = scope;
        }

        public Scope getScope()
        {
            return scope;
        }
    }
}
