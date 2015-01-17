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
package com.facebook.presto.sql.routine;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.control.WhileLoop;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.BytecodeExpressionVisitor;
import com.facebook.presto.sql.gen.CachedInstanceBinder;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.CompilerUtils.defineClass;
import static com.facebook.presto.bytecode.CompilerUtils.makeClassName;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.sql.gen.BytecodeUtils.boxPrimitive;
import static com.facebook.presto.sql.gen.BytecodeUtils.unboxPrimitive;
import static com.facebook.presto.sql.gen.BytecodeUtils.unboxPrimitiveIfNecessary;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Maps.toMap;
import static com.google.common.primitives.Primitives.wrap;
import static java.util.Objects.requireNonNull;

public final class RoutineCompiler
{
    private final FunctionRegistry functionRegistry;

    public RoutineCompiler(FunctionRegistry functionRegistry)
    {
        this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry is null");
    }

    public Class<?> compile(SqlRoutine routine)
    {
        List<Parameter> parameters = routine.getParameters().stream()
                .map(variable -> arg(name(variable), compilerType(variable.getType())))
                .collect(toImmutableList());

        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("SqlRoutine"),
                type(Object.class));

        classDefinition.declareDefaultConstructor(a(PRIVATE));

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC, STATIC),
                "run",
                compilerType(routine.getReturnType()),
                parameters);

        Scope scope = method.getScope();

        scope.declareVariable(boolean.class, "wasNull");

        Map<SqlVariable, Variable> variables = toMap(VariableExtractor.extract(routine),
                variable -> getOrDeclareVariable(scope, compilerType(variable.getType()), name(variable)));

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        CachedInstanceBinder instanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        ByteCodeVisitor visitor = new ByteCodeVisitor(variables, callSiteBinder, instanceBinder, functionRegistry);
        method.getBody().append(visitor.process(routine, scope));

        return defineClass(classDefinition, Object.class, callSiteBinder.getBindings(), new DynamicClassLoader());
    }

    private static Variable getOrDeclareVariable(Scope scope, ParameterizedType type, String name)
    {
        if (scope.variableExists(name)) {
            return scope.getVariable(name);
        }
        return scope.declareVariable(type, name);
    }

    private static ParameterizedType compilerType(Type type)
    {
        return type(wrap(type.getJavaType()));
    }

    private static String name(SqlVariable variable)
    {
        return name(variable.getField());
    }

    private static String name(int field)
    {
        return "v" + field;
    }

    private static class ByteCodeVisitor
            implements SqlNodeVisitor<Scope, BytecodeNode>
    {
        private final Map<SqlVariable, Variable> variables;
        private final CallSiteBinder callSiteBinder;
        private final CachedInstanceBinder instanceBinder;
        private final Map<SqlLabel, LabelNode> continueLabels = new IdentityHashMap<>();
        private final Map<SqlLabel, LabelNode> breakLabels = new IdentityHashMap<>();
        private final FunctionRegistry functionRegistry;

        public ByteCodeVisitor(
                Map<SqlVariable, Variable> variables,
                CallSiteBinder callSiteBinder,
                CachedInstanceBinder instanceBinder,
                FunctionRegistry functionRegistry)
        {
            this.variables = requireNonNull(variables, "variables is null");
            this.callSiteBinder = requireNonNull(callSiteBinder, "callSiteBinder is null");
            this.instanceBinder = requireNonNull(instanceBinder, "instanceBinder is null");
            this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry is null");
        }

        @Override
        public BytecodeNode visitRoutine(SqlRoutine node, Scope scope)
        {
            return process(node.getBody(), scope);
        }

        @Override
        public BytecodeNode visitSet(SqlSet node, Scope scope)
        {
            return new BytecodeBlock()
                    .append(compile(node.getValue(), scope))
                    .putVariable(variables.get(node.getTarget()));
        }

        @Override
        public BytecodeNode visitBlock(SqlBlock node, Scope scope)
        {
            BytecodeBlock block = new BytecodeBlock();

            for (SqlVariable sqlVariable : node.getVariables()) {
                block.append(compile(sqlVariable.getDefaultValue(), scope))
                        .putVariable(variables.get(sqlVariable));
            }

            LabelNode continueLabel = new LabelNode("continue");
            LabelNode breakLabel = new LabelNode("break");

            Optional<SqlLabel> label = node.getLabel();
            if (label.isPresent()) {
                continueLabels.put(label.get(), continueLabel);
                breakLabels.put(label.get(), breakLabel);
                block.visitLabel(continueLabel);
            }

            for (SqlStatement statement : node.getStatements()) {
                block.append(process(statement, scope));
            }

            if (label.isPresent()) {
                block.visitLabel(breakLabel);
            }

            return block;
        }

        @Override
        public BytecodeNode visitReturn(SqlReturn node, Scope scope)
        {
            return new BytecodeBlock()
                    .append(compile(node.getValue(), scope))
                    .ret(wrap(node.getValue().getType().getJavaType()));
        }

        @Override
        public BytecodeNode visitContinue(SqlContinue node, Scope scope)
        {
            LabelNode label = continueLabels.get(node.getTarget());
            verify(label != null, "continue target does not exist");
            return new BytecodeBlock()
                    .gotoLabel(label);
        }

        @Override
        public BytecodeNode visitBreak(SqlBreak node, Scope scope)
        {
            LabelNode label = breakLabels.get(node.getTarget());
            verify(label != null, "break target does not exist");
            return new BytecodeBlock()
                    .gotoLabel(label);
        }

        @Override
        public BytecodeNode visitIf(SqlIf node, Scope scope)
        {
            IfStatement ifStatement = new IfStatement()
                    .condition(compileBoolean(node.getCondition(), scope))
                    .ifTrue(process(node.getIfTrue(), scope));

            if (node.getIfFalse().isPresent()) {
                ifStatement.ifFalse(process(node.getIfFalse().get(), scope));
            }

            return ifStatement;
        }

        @Override
        public BytecodeNode visitCase(SqlCase node, Scope scope)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytecodeNode visitSwitch(SqlSwitch node, Scope scope)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytecodeNode visitWhile(SqlWhile node, Scope scope)
        {
            BytecodeBlock block = new BytecodeBlock();

            LabelNode continueLabel = new LabelNode("continue");
            LabelNode breakLabel = new LabelNode("break");

            if (node.getLabel().isPresent()) {
                continueLabels.put(node.getLabel().get(), continueLabel);
                breakLabels.put(node.getLabel().get(), breakLabel);
                block.visitLabel(continueLabel);
            }

            block.append(new WhileLoop()
                    .condition(compileBoolean(node.getCondition(), scope))
                    .body(process(node.getBody(), scope)));

            if (node.getLabel().isPresent()) {
                block.visitLabel(breakLabel);
            }

            return block;
        }

        @Override
        public BytecodeNode visitRepeat(SqlRepeat node, Scope scope)
        {
            throw new UnsupportedOperationException();
        }

        private BytecodeNode compile(RowExpression expression, Scope scope)
        {
            if (expression instanceof InputReferenceExpression) {
                InputReferenceExpression input = (InputReferenceExpression) expression;
                return scope.getVariable(name(input.getField()));
            }

            Map<CallExpression, MethodDefinition> tryMethodMap = ImmutableMap.of(); // TODO
            BytecodeExpressionVisitor visitor = new BytecodeExpressionVisitor(
                    callSiteBinder,
                    instanceBinder,
                    new FieldReferenceCompiler(),
                    functionRegistry,
                    tryMethodMap);

            Type type = expression.getType();
            Variable wasNull = scope.getVariable("wasNull");

            return new BytecodeBlock()
                    .comment("wasNull = false;")
                    .putVariable(wasNull, type.getJavaType() == void.class)
                    .comment("expression: " + expression)
                    .append(expression.accept(visitor, scope))
                    .append(boxPrimitive(type.getJavaType()))
                    .comment("if (wasNull)")
                    .append(new IfStatement()
                            .condition(wasNull)
                            .ifTrue(new BytecodeBlock()
                                    .pop()
                                    .pushNull()));
        }

        private BytecodeNode compileBoolean(RowExpression expression, Scope scope)
        {
            checkArgument(expression.getType().equals(BooleanType.BOOLEAN), "type must be boolean");

            LabelNode notNull = new LabelNode("notNull");
            LabelNode done = new LabelNode("done");

            return new BytecodeBlock()
                    .append(compile(expression, scope))
                    .comment("if value is null, return false, otherwise unbox")
                    .dup()
                    .ifNotNullGoto(notNull)
                    .pop()
                    .push(false)
                    .gotoLabel(done)
                    .visitLabel(notNull)
                    .append(unboxPrimitive(expression.getType().getJavaType()))
                    .visitLabel(done);
        }
    }

    private static class FieldReferenceCompiler
            implements RowExpressionVisitor<Scope, BytecodeNode>
    {
        @Override
        public BytecodeNode visitInputReference(InputReferenceExpression node, Scope scope)
        {
            Class<?> boxedType = wrap(node.getType().getJavaType());
            return new BytecodeBlock()
                    .append(scope.getVariable(name(node.getField())))
                    .append(unboxPrimitiveIfNecessary(scope, boxedType));
        }

        @Override
        public BytecodeNode visitCall(CallExpression call, Scope scope)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytecodeNode visitConstant(ConstantExpression literal, Scope scope)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class VariableExtractor
            extends DefaultSqlNodeVisitor
    {
        private final List<SqlVariable> variables = new ArrayList<>();

        @Override
        public Void visitVariable(SqlVariable node, Void context)
        {
            variables.add(node);
            return null;
        }

        public static List<SqlVariable> extract(SqlNode node)
        {
            VariableExtractor extractor = new VariableExtractor();
            extractor.process(node, null);
            return extractor.variables;
        }
    }
}
