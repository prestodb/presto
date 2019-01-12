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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.Scope;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.sql.gen.LambdaBytecodeGenerator.CompiledLambda;
import io.prestosql.sql.relational.CallExpression;
import io.prestosql.sql.relational.ConstantExpression;
import io.prestosql.sql.relational.InputReferenceExpression;
import io.prestosql.sql.relational.LambdaDefinitionExpression;
import io.prestosql.sql.relational.RowExpression;
import io.prestosql.sql.relational.RowExpressionVisitor;
import io.prestosql.sql.relational.VariableReferenceExpression;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantTrue;
import static io.airlift.bytecode.instruction.Constant.loadBoolean;
import static io.airlift.bytecode.instruction.Constant.loadDouble;
import static io.airlift.bytecode.instruction.Constant.loadFloat;
import static io.airlift.bytecode.instruction.Constant.loadInt;
import static io.airlift.bytecode.instruction.Constant.loadLong;
import static io.airlift.bytecode.instruction.Constant.loadString;
import static io.prestosql.sql.gen.BytecodeUtils.loadConstant;
import static io.prestosql.sql.gen.LambdaBytecodeGenerator.generateLambda;
import static io.prestosql.sql.relational.Signatures.BIND;
import static io.prestosql.sql.relational.Signatures.CAST;
import static io.prestosql.sql.relational.Signatures.COALESCE;
import static io.prestosql.sql.relational.Signatures.DEREFERENCE;
import static io.prestosql.sql.relational.Signatures.IF;
import static io.prestosql.sql.relational.Signatures.IN;
import static io.prestosql.sql.relational.Signatures.IS_NULL;
import static io.prestosql.sql.relational.Signatures.NULL_IF;
import static io.prestosql.sql.relational.Signatures.ROW_CONSTRUCTOR;
import static io.prestosql.sql.relational.Signatures.SWITCH;

public class RowExpressionCompiler
{
    private final CallSiteBinder callSiteBinder;
    private final CachedInstanceBinder cachedInstanceBinder;
    private final RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompiler;
    private final FunctionRegistry registry;
    private final Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap;

    RowExpressionCompiler(
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompiler,
            FunctionRegistry registry,
            Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap)
    {
        this.callSiteBinder = callSiteBinder;
        this.cachedInstanceBinder = cachedInstanceBinder;
        this.fieldReferenceCompiler = fieldReferenceCompiler;
        this.registry = registry;
        this.compiledLambdaMap = compiledLambdaMap;
    }

    public BytecodeNode compile(RowExpression rowExpression, Scope scope)
    {
        return compile(rowExpression, scope, Optional.empty());
    }

    public BytecodeNode compile(RowExpression rowExpression, Scope scope, Optional<Class> lambdaInterface)
    {
        return rowExpression.accept(new Visitor(), new Context(scope, lambdaInterface));
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
                        generator = new BindCodeGenerator(compiledLambdaMap, context.getLambdaInterface().get());
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
            checkState(compiledLambdaMap.containsKey(lambda), "lambda expressions map does not contain this lambda definition");
            if (!context.lambdaInterface.get().isAnnotationPresent(FunctionalInterface.class)) {
                // lambdaInterface is checked to be annotated with FunctionalInterface when generating ScalarFunctionImplementation
                throw new VerifyException("lambda should be generated as class annotated with FunctionalInterface");
            }

            BytecodeGeneratorContext generatorContext = new BytecodeGeneratorContext(
                    RowExpressionCompiler.this,
                    context.getScope(),
                    callSiteBinder,
                    cachedInstanceBinder,
                    registry);

            return generateLambda(
                    generatorContext,
                    ImmutableList.of(),
                    compiledLambdaMap.get(lambda),
                    context.getLambdaInterface().get());
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
        private final Optional<Class> lambdaInterface;

        public Context(Scope scope, Optional<Class> lambdaInterface)
        {
            this.scope = scope;
            this.lambdaInterface = lambdaInterface;
        }

        public Scope getScope()
        {
            return scope;
        }

        public Optional<Class> getLambdaInterface()
        {
            return lambdaInterface;
        }
    }
}
