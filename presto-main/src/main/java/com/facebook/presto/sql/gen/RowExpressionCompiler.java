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

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.sql.gen.LambdaBytecodeGenerator.CompiledLambda;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.LambdaDefinitionExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.facebook.presto.sql.relational.SpecialFormExpression;
import com.facebook.presto.sql.relational.VariableReferenceExpression;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.sql.gen.BytecodeUtils.generateWrite;
import static com.facebook.presto.sql.gen.BytecodeUtils.loadConstant;
import static com.facebook.presto.sql.gen.LambdaBytecodeGenerator.generateLambda;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantTrue;
import static io.airlift.bytecode.instruction.Constant.loadBoolean;
import static io.airlift.bytecode.instruction.Constant.loadDouble;
import static io.airlift.bytecode.instruction.Constant.loadFloat;
import static io.airlift.bytecode.instruction.Constant.loadInt;
import static io.airlift.bytecode.instruction.Constant.loadLong;
import static io.airlift.bytecode.instruction.Constant.loadString;

public class RowExpressionCompiler
{
    private final CallSiteBinder callSiteBinder;
    private final CachedInstanceBinder cachedInstanceBinder;
    private final RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompiler;
    private final FunctionManager functionManager;
    private final Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap;

    RowExpressionCompiler(
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompiler,
            FunctionManager functionManager,
            Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap)
    {
        this.callSiteBinder = callSiteBinder;
        this.cachedInstanceBinder = cachedInstanceBinder;
        this.fieldReferenceCompiler = fieldReferenceCompiler;
        this.functionManager = functionManager;
        this.compiledLambdaMap = compiledLambdaMap;
    }

    public BytecodeNode compile(RowExpression rowExpression, Scope scope, Optional<Variable> outputBlockVariable)
    {
        return compile(rowExpression, scope, outputBlockVariable, Optional.empty());
    }

    // When outputBlockVariable is presented, the generated bytecode will write the evaluated value into the outputBlockVariable,
    // otherwise the value will be left on stack.
    public BytecodeNode compile(RowExpression rowExpression, Scope scope, Optional<Variable> outputBlockVariable, Optional<Class> lambdaInterface)
    {
        return rowExpression.accept(new Visitor(), new Context(scope, outputBlockVariable, lambdaInterface));
    }

    private class Visitor
            implements RowExpressionVisitor<BytecodeNode, Context>
    {
        @Override
        public BytecodeNode visitCall(CallExpression call, Context context)
        {
            BytecodeGeneratorContext generatorContext = new BytecodeGeneratorContext(
                    RowExpressionCompiler.this,
                    context.getScope(),
                    callSiteBinder,
                    cachedInstanceBinder,
                    functionManager);

            // special-cased in function manager
            if (call.getFunctionHandle().getSignature().getName().equals(CAST)) {
                return (new CastCodeGenerator()).generateExpression(generatorContext, call.getType(), call.getArguments(), context.getOutputBlockVariable());
            }
            else {
                return (new FunctionCallCodeGenerator()).generateCall(call.getFunctionHandle().getSignature(), generatorContext, call.getType(), call.getArguments(), context.getOutputBlockVariable());
            }
        }

        @Override
        public BytecodeNode visitConstant(ConstantExpression constant, Context context)
        {
            Object value = constant.getValue();
            Class<?> javaType = constant.getType().getJavaType();

            BytecodeBlock block = new BytecodeBlock();
            if (value == null) {
                block.comment("constant null")
                        .append(context.getScope().getVariable("wasNull").set(constantTrue()))
                        .pushJavaDefault(javaType);
            }
            else {
                // use LDC for primitives (boolean, short, int, long, float, double)
                block.comment("constant " + constant.getType().getTypeSignature());
                if (javaType == boolean.class) {
                    block.append(loadBoolean((Boolean) value));
                }
                else if (javaType == byte.class || javaType == short.class || javaType == int.class) {
                    block.append(loadInt(((Number) value).intValue()));
                }
                else if (javaType == long.class) {
                    block.append(loadLong((Long) value));
                }
                else if (javaType == float.class) {
                    block.append(loadFloat((Float) value));
                }
                else if (javaType == double.class) {
                    block.append(loadDouble((Double) value));
                }
                else if (javaType == String.class) {
                    block.append(loadString((String) value));
                }
                else {
                    // bind constant object directly into the call-site using invoke dynamic
                    Binding binding = callSiteBinder.bind(value, constant.getType().getJavaType());

                    block = new BytecodeBlock()
                            .setDescription("constant " + constant.getType())
                            .comment(constant.toString())
                            .append(loadConstant(binding));
                }
            }

            if (context.getOutputBlockVariable().isPresent()) {
                block.append(generateWrite(
                        callSiteBinder,
                        context.getScope(),
                        context.getScope().getVariable("wasNull"),
                        constant.getType(),
                        context.getOutputBlockVariable().get()));
            }

            return block;
        }

        @Override
        public BytecodeNode visitInputReference(InputReferenceExpression node, Context context)
        {
            BytecodeNode inputReferenceBytecode = fieldReferenceCompiler.visitInputReference(node, context.getScope());
            if (!context.getOutputBlockVariable().isPresent()) {
                return inputReferenceBytecode;
            }

            return new BytecodeBlock()
                    .append(inputReferenceBytecode)
                    .append(generateWrite(
                            callSiteBinder,
                            context.getScope(),
                            context.getScope().getVariable("wasNull"),
                            node.getType(),
                            context.getOutputBlockVariable().get()));
        }

        @Override
        public BytecodeNode visitLambda(LambdaDefinitionExpression lambda, Context context)
        {
            checkArgument(!context.getOutputBlockVariable().isPresent(), "lambda definition expression does not support writing to block");
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
                    functionManager);

            return generateLambda(
                    generatorContext,
                    ImmutableList.of(),
                    compiledLambdaMap.get(lambda),
                    context.getLambdaInterface().get());
        }

        @Override
        public BytecodeNode visitVariableReference(VariableReferenceExpression reference, Context context)
        {
            BytecodeNode variableReferenceByteCode = fieldReferenceCompiler.visitVariableReference(reference, context.getScope());
            if (!context.getOutputBlockVariable().isPresent()) {
                return variableReferenceByteCode;
            }

            return new BytecodeBlock()
                    .append(variableReferenceByteCode)
                    .append(generateWrite(
                            callSiteBinder,
                            context.getScope(),
                            context.getScope().getVariable("wasNull"),
                            reference.getType(),
                            context.getOutputBlockVariable().get()));
        }

        @Override
        public BytecodeNode visitSpecialForm(SpecialFormExpression specialForm, Context context)
        {
            SpecialFormBytecodeGenerator generator;
            switch (specialForm.getForm()) {
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
                    generator = new InCodeGenerator(functionManager);
                    break;
                // optimized implementations (shortcircuiting behavior)
                case AND:
                    generator = new AndCodeGenerator();
                    break;
                case OR:
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
                    throw new IllegalStateException("Can not compile special form: " + specialForm.getForm());
            }
            BytecodeGeneratorContext generatorContext = new BytecodeGeneratorContext(
                    RowExpressionCompiler.this,
                    context.getScope(),
                    callSiteBinder,
                    cachedInstanceBinder,
                    functionManager);

            return generator.generateExpression(generatorContext, specialForm.getType(), specialForm.getArguments(), context.getOutputBlockVariable());
        }
    }

    private static class Context
    {
        private final Scope scope;
        private final Optional<Variable> outputBlockVariable;
        private final Optional<Class> lambdaInterface;

        public Context(Scope scope, Optional<Variable> outputBlockVariable, Optional<Class> lambdaInterface)
        {
            this.scope = scope;
            this.outputBlockVariable = outputBlockVariable;
            this.lambdaInterface = lambdaInterface;
        }

        public Scope getScope()
        {
            return scope;
        }

        public Optional<Variable> getOutputBlockVariable()
        {
            return outputBlockVariable;
        }

        public Optional<Class> getLambdaInterface()
        {
            return lambdaInterface;
        }
    }
}
