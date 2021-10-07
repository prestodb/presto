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

import com.facebook.presto.bytecode.Binding;
import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.CallSiteBinder;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.function.SqlInvokedScalarFunctionImplementation;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.gen.LambdaBytecodeGenerator.CompiledLambda;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantTrue;
import static com.facebook.presto.bytecode.instruction.Constant.loadBoolean;
import static com.facebook.presto.bytecode.instruction.Constant.loadDouble;
import static com.facebook.presto.bytecode.instruction.Constant.loadFloat;
import static com.facebook.presto.bytecode.instruction.Constant.loadInt;
import static com.facebook.presto.bytecode.instruction.Constant.loadLong;
import static com.facebook.presto.bytecode.instruction.Constant.loadString;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static com.facebook.presto.sql.gen.BytecodeUtils.generateWrite;
import static com.facebook.presto.sql.gen.BytecodeUtils.loadConstant;
import static com.facebook.presto.sql.gen.LambdaBytecodeGenerator.generateLambda;
import static com.facebook.presto.sql.gen.LambdaBytecodeGenerator.generateMethodsForLambda;
import static com.facebook.presto.sql.relational.SqlFunctionUtils.getSqlFunctionRowExpression;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

public class RowExpressionCompiler
{
    private final ClassDefinition classDefinition;
    private final CallSiteBinder callSiteBinder;
    private final CachedInstanceBinder cachedInstanceBinder;
    private final RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompiler;
    private final Metadata metadata;
    private final SqlFunctionProperties sqlFunctionProperties;
    private final Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions;
    private final Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap;

    RowExpressionCompiler(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompiler,
            Metadata metadata,
            SqlFunctionProperties sqlFunctionProperties,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions,
            Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap)
    {
        this.classDefinition = classDefinition;
        this.callSiteBinder = callSiteBinder;
        this.cachedInstanceBinder = cachedInstanceBinder;
        this.fieldReferenceCompiler = fieldReferenceCompiler;
        this.metadata = metadata;
        this.sqlFunctionProperties = sqlFunctionProperties;
        this.sessionFunctions = sessionFunctions;
        this.compiledLambdaMap = new HashMap<>(compiledLambdaMap);
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
            FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
            FunctionMetadata functionMetadata = functionAndTypeManager.getFunctionMetadata(call.getFunctionHandle());
            BytecodeGeneratorContext generatorContext;
            switch (functionMetadata.getImplementationType()) {
                case BUILTIN:
                    generatorContext = new BytecodeGeneratorContext(
                            RowExpressionCompiler.this,
                            context.getScope(),
                            callSiteBinder,
                            cachedInstanceBinder,
                            functionAndTypeManager);
                    return (new FunctionCallCodeGenerator()).generateCall(call.getFunctionHandle(), generatorContext, call.getType(), call.getArguments(), context.getOutputBlockVariable());
                case SQL:
                    SqlInvokedScalarFunctionImplementation functionImplementation = (SqlInvokedScalarFunctionImplementation) functionAndTypeManager.getScalarFunctionImplementation(call.getFunctionHandle());
                    RowExpression function = getSqlFunctionRowExpression(
                            functionMetadata,
                            functionImplementation,
                            metadata,
                            sqlFunctionProperties,
                            sessionFunctions,
                            call.getArguments());

                    // Pre-compile lambda bytecode and update compiled lambda map
                    compiledLambdaMap.putAll(generateMethodsForLambda(
                            classDefinition,
                            callSiteBinder,
                            cachedInstanceBinder,
                            function,
                            metadata,
                            sqlFunctionProperties,
                            sessionFunctions,
                            "sql",
                            compiledLambdaMap.keySet()));

                    // generate bytecode for SQL function
                    RowExpressionCompiler newRowExpressionCompiler = new RowExpressionCompiler(
                            classDefinition,
                            callSiteBinder,
                            cachedInstanceBinder,
                            fieldReferenceCompiler,
                            metadata,
                            sqlFunctionProperties,
                            sessionFunctions,
                            compiledLambdaMap);
                    // If called on null input, directly use the generated bytecode
                    if (functionMetadata.isCalledOnNullInput() || call.getArguments().isEmpty()) {
                        return newRowExpressionCompiler.compile(
                                function,
                                context.getScope(),
                                context.getOutputBlockVariable(),
                                context.getLambdaInterface());
                    }

                    // If returns null on null input, generate if(any input is null, null, generated bytecode)
                    generatorContext = new BytecodeGeneratorContext(
                            newRowExpressionCompiler,
                            context.getScope(),
                            callSiteBinder,
                            cachedInstanceBinder,
                            functionAndTypeManager);

                    return (new IfCodeGenerator()).generateExpression(
                            generatorContext,
                            call.getType(),
                            ImmutableList.of(
                                    call.getArguments().stream()
                                            .map(argument -> new SpecialFormExpression(IS_NULL, BOOLEAN, argument))
                                            .reduce((a, b) -> new SpecialFormExpression(OR, BOOLEAN, a, b)).get(),
                                    new ConstantExpression(null, call.getType()),
                                    function),
                            context.getOutputBlockVariable());
                default:
                    throw new IllegalArgumentException(format("Unsupported function implementation type: %s", functionMetadata.getImplementationType()));
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
                    metadata.getFunctionAndTypeManager());

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
                    generator = new InCodeGenerator(metadata.getFunctionAndTypeManager());
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
                    throw new IllegalStateException("Cannot compile special form: " + specialForm.getForm());
            }
            BytecodeGeneratorContext generatorContext = new BytecodeGeneratorContext(
                    RowExpressionCompiler.this,
                    context.getScope(),
                    callSiteBinder,
                    cachedInstanceBinder,
                    metadata.getFunctionAndTypeManager());

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
