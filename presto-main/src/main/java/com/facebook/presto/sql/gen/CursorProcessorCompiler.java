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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.CallSiteBinder;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.control.WhileLoop;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.project.CursorProcessorOutput;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.gen.LambdaBytecodeGenerator.CompiledLambda;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantBoolean;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantTrue;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newInstance;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.or;
import static com.facebook.presto.bytecode.instruction.JumpInstruction.jump;
import static com.facebook.presto.sql.gen.BytecodeUtils.boxPrimitiveIfNecessary;
import static com.facebook.presto.sql.gen.BytecodeUtils.unboxPrimitiveIfNecessary;
import static com.facebook.presto.sql.gen.CommonSubExpressionRewriter.CommonSubExpressionFields;
import static com.facebook.presto.sql.gen.CommonSubExpressionRewriter.CommonSubExpressionFields.declareCommonSubExpressionFields;
import static com.facebook.presto.sql.gen.CommonSubExpressionRewriter.CommonSubExpressionFields.initializeCommonSubExpressionFields;
import static com.facebook.presto.sql.gen.CommonSubExpressionRewriter.collectCSEByLevel;
import static com.facebook.presto.sql.gen.CommonSubExpressionRewriter.rewriteExpressionWithCSE;
import static com.facebook.presto.sql.gen.LambdaBytecodeGenerator.generateMethodsForLambda;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;

public class CursorProcessorCompiler
        implements BodyCompiler
{
    private static Logger log = Logger.get(CursorProcessorCompiler.class);

    private final Metadata metadata;
    private final boolean isOptimizeCommonSubExpressions;
    private final Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions;

    public CursorProcessorCompiler(Metadata metadata, boolean isOptimizeCommonSubExpressions, Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions)
    {
        this.metadata = metadata;
        this.isOptimizeCommonSubExpressions = isOptimizeCommonSubExpressions;
        this.sessionFunctions = sessionFunctions;
    }

    @Override
    public void generateMethods(SqlFunctionProperties sqlFunctionProperties, ClassDefinition classDefinition, CallSiteBinder callSiteBinder, RowExpression filter, List<RowExpression> projections)
    {
        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        List<RowExpression> rowExpressions = ImmutableList.<RowExpression>builder()
                .addAll(projections)
                .add(filter)
                .build();

        Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap = generateMethodsForLambda(classDefinition,
                callSiteBinder,
                cachedInstanceBinder,
                rowExpressions,
                metadata,
                sqlFunctionProperties,
                sessionFunctions,
                "");
        Map<VariableReferenceExpression, CommonSubExpressionFields> cseFields = ImmutableMap.of();
        RowExpressionCompiler compiler = new RowExpressionCompiler(
                classDefinition,
                callSiteBinder,
                cachedInstanceBinder,
                fieldReferenceCompiler(cseFields),
                metadata,
                sqlFunctionProperties,
                sessionFunctions,
                compiledLambdaMap);

        if (isOptimizeCommonSubExpressions) {
            Map<Integer, Map<RowExpression, VariableReferenceExpression>> commonSubExpressionsByLevel = collectCSEByLevel(rowExpressions);

            if (!commonSubExpressionsByLevel.isEmpty()) {
                cseFields = declareCommonSubExpressionFields(classDefinition, commonSubExpressionsByLevel);
                compiler = new RowExpressionCompiler(
                        classDefinition,
                        callSiteBinder,
                        cachedInstanceBinder,
                        fieldReferenceCompiler(cseFields),
                        metadata,
                        sqlFunctionProperties,
                        sessionFunctions,
                        compiledLambdaMap);
                generateCommonSubExpressionMethods(classDefinition, compiler, commonSubExpressionsByLevel, cseFields);

                Map<RowExpression, VariableReferenceExpression> commonSubExpressions = commonSubExpressionsByLevel.values().stream()
                        .flatMap(m -> m.entrySet().stream())
                        .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

                projections = rewriteRowExpressionsWithCSE(projections, commonSubExpressions);
                filter = rewriteRowExpressionsWithCSE(ImmutableList.of(filter), commonSubExpressions).get(0);
            }
        }

        generateProcessMethod(classDefinition, projections.size(), cseFields);

        generateFilterMethod(classDefinition, compiler, filter);

        for (int i = 0; i < projections.size(); i++) {
            String methodName = "project_" + i;
            generateProjectMethod(classDefinition, compiler, methodName, projections.get(i));
        }

        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC));
        BytecodeBlock constructorBody = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();
        constructorBody.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        initializeCommonSubExpressionFields(cseFields.values(), thisVariable, constructorBody);

        cachedInstanceBinder.generateInitializations(thisVariable, constructorBody);
        constructorBody.ret();
    }

    List<RowExpression> rewriteRowExpressionsWithCSE(
            List<RowExpression> rows,
            Map<RowExpression, VariableReferenceExpression> commonSubExpressions)
    {
        if (!commonSubExpressions.isEmpty()) {
            rows = rows.stream()
                    .map(p -> rewriteExpressionWithCSE(p, commonSubExpressions))
                    .collect(toImmutableList());

            if (log.isDebugEnabled()) {
                log.debug("Extracted %d common sub-expressions", commonSubExpressions.size());
                commonSubExpressions.entrySet().forEach(entry -> log.debug("\t%s = %s", entry.getValue(), entry.getKey()));
                log.debug("Rewrote Rows: %s", rows);
            }
        }
        return rows;
    }

    private static void generateProcessMethod(ClassDefinition classDefinition, int projections, Map<VariableReferenceExpression, CommonSubExpressionFields> cseFields)
    {
        Parameter properties = arg("properties", SqlFunctionProperties.class);
        Parameter yieldSignal = arg("yieldSignal", DriverYieldSignal.class);
        Parameter cursor = arg("cursor", RecordCursor.class);
        Parameter pageBuilder = arg("pageBuilder", PageBuilder.class);
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "process", type(CursorProcessorOutput.class), properties, yieldSignal, cursor, pageBuilder);

        Scope scope = method.getScope();
        Variable completedPositionsVariable = scope.declareVariable(int.class, "completedPositions");
        Variable finishedVariable = scope.declareVariable(boolean.class, "finished");

        method.getBody()
                .comment("int completedPositions = 0;")
                .putVariable(completedPositionsVariable, 0)
                .comment("boolean finished = false;")
                .putVariable(finishedVariable, false);

        // while loop loop body
        LabelNode done = new LabelNode("done");

        BytecodeBlock whileFunctionBlock = new BytecodeBlock()
                .comment("if (pageBuilder.isFull() || yieldSignal.isSet()) return new CursorProcessorOutput(completedPositions, false);")
                .append(new IfStatement()
                        .condition(or(
                                pageBuilder.invoke("isFull", boolean.class),
                                yieldSignal.invoke("isSet", boolean.class)))
                        .ifTrue(jump(done)))
                .comment("if (!cursor.advanceNextPosition()) return new CursorProcessorOutput(completedPositions, true);")
                .append(new IfStatement()
                        .condition(cursor.invoke("advanceNextPosition", boolean.class))
                        .ifFalse(new BytecodeBlock()
                                .putVariable(finishedVariable, true)
                                .gotoLabel(done)));

        // reset the CSE evaluatedField = false for every row
        cseFields.values().forEach(field -> whileFunctionBlock.append(scope.getThis().setField(field.getEvaluatedField(), constantBoolean(false))));

        whileFunctionBlock.comment("do the projection")
            .append(createProjectIfStatement(classDefinition, method, properties, cursor, pageBuilder, projections))
            .comment("completedPositions++;")
            .incrementVariable(completedPositionsVariable, (byte) 1);

        WhileLoop whileLoop = new WhileLoop()
                .condition(constantTrue())
                .body(whileFunctionBlock);

        method.getBody()
                .append(whileLoop)
                .visitLabel(done)
                .append(newInstance(CursorProcessorOutput.class, completedPositionsVariable, finishedVariable)
                        .ret());
    }

    private static IfStatement createProjectIfStatement(
            ClassDefinition classDefinition,
            MethodDefinition method,
            Parameter properties,
            Parameter cursor,
            Parameter pageBuilder,
            int projections)
    {
        // if (filter(cursor))
        IfStatement ifStatement = new IfStatement();
        ifStatement.condition()
                .append(method.getThis())
                .getVariable(properties)
                .getVariable(cursor)
                .invokeVirtual(classDefinition.getType(), "filter", type(boolean.class), type(SqlFunctionProperties.class), type(RecordCursor.class));

        // pageBuilder.declarePosition();
        ifStatement.ifTrue()
                .getVariable(pageBuilder)
                .invokeVirtual(PageBuilder.class, "declarePosition", void.class);

        // this.project_43(session, cursor, pageBuilder.getBlockBuilder(42)));
        for (int projectionIndex = 0; projectionIndex < projections; projectionIndex++) {
            ifStatement.ifTrue()
                    .append(method.getThis())
                    .getVariable(properties)
                    .getVariable(cursor);

            // pageBuilder.getBlockBuilder(0)
            ifStatement.ifTrue()
                    .getVariable(pageBuilder)
                    .push(projectionIndex)
                    .invokeVirtual(PageBuilder.class, "getBlockBuilder", BlockBuilder.class, int.class);

            // project(block..., blockBuilder)gen
            ifStatement.ifTrue()
                    .invokeVirtual(classDefinition.getType(),
                            "project_" + projectionIndex,
                            type(void.class),
                            type(SqlFunctionProperties.class),
                            type(RecordCursor.class),
                            type(BlockBuilder.class));
        }
        return ifStatement;
    }

    private void generateFilterMethod(
            ClassDefinition classDefinition,
            RowExpressionCompiler compiler,
            RowExpression filter)
    {
        Parameter properties = arg("properties", SqlFunctionProperties.class);
        Parameter cursor = arg("cursor", RecordCursor.class);
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "filter", type(boolean.class), properties, cursor);

        method.comment("Filter: %s", filter);

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        Variable wasNullVariable = scope.declareVariable(type(boolean.class), "wasNull");

        LabelNode end = new LabelNode("end");
        body.comment("boolean wasNull = false;")
            .putVariable(wasNullVariable, false)
            .comment("evaluate filter: " + filter)
            .append(compiler.compile(filter, scope, Optional.empty()))
            .comment("if (wasNull) return false;")
            .getVariable(wasNullVariable)
            .ifFalseGoto(end)
            .pop(boolean.class)
            .push(false)
            .visitLabel(end)
            .retBoolean();
    }

    private void generateProjectMethod(
            ClassDefinition classDefinition,
            RowExpressionCompiler compiler,
            String methodName,
            RowExpression projection)
    {
        Parameter properties = arg("properties", SqlFunctionProperties.class);
        Parameter cursor = arg("cursor", RecordCursor.class);
        Parameter output = arg("output", BlockBuilder.class);
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), methodName, type(void.class), properties, cursor, output);

        method.comment("Projection: %s", projection.toString());

        Scope scope = method.getScope();

        Variable wasNullVariable = scope.declareVariable(type(boolean.class), "wasNull");
        method.getBody()
                .comment("boolean wasNull = false;")
                .putVariable(wasNullVariable, false)
                .comment("evaluate projection: " + projection.toString())
                .append(compiler.compile(projection, scope, Optional.of(output)))
                .ret();
    }

    private List<MethodDefinition> generateCommonSubExpressionMethods(
            ClassDefinition classDefinition,
            RowExpressionCompiler compiler,
            Map<Integer, Map<RowExpression, VariableReferenceExpression>> commonSubExpressionsByLevel,
            Map<VariableReferenceExpression, CommonSubExpressionFields> commonSubExpressionFieldsMap)
    {
        Parameter properties = arg("properties", SqlFunctionProperties.class);
        Parameter cursor = arg("cursor", RecordCursor.class);

        ImmutableList.Builder<MethodDefinition> methods = ImmutableList.builder();
        Map<VariableReferenceExpression, CommonSubExpressionFields> cseMap = new HashMap<>();
        int startLevel = commonSubExpressionsByLevel.keySet().stream().reduce(Math::min).get();
        int maxLevel = commonSubExpressionsByLevel.keySet().stream().reduce(Math::max).get();
        for (int i = startLevel; i <= maxLevel; i++) {
            if (commonSubExpressionsByLevel.containsKey(i)) {
                for (Map.Entry<RowExpression, VariableReferenceExpression> entry : commonSubExpressionsByLevel.get(i).entrySet()) {
                    RowExpression cse = entry.getKey();
                    Class<?> type = Primitives.wrap(cse.getType().getJavaType());
                    VariableReferenceExpression cseVariable = entry.getValue();
                    CommonSubExpressionFields cseFields = commonSubExpressionFieldsMap.get(cseVariable);
                    MethodDefinition method = classDefinition.declareMethod(
                            a(PRIVATE),
                            "get" + cseVariable.getName(),
                            type(cseFields.getResultType()),
                            properties,
                            cursor);

                    method.comment("cse: %s", cse);

                    Scope scope = method.getScope();
                    BytecodeBlock body = method.getBody();
                    Variable thisVariable = method.getThis();

                    scope.declareVariable("wasNull", body, constantFalse());

                    IfStatement ifStatement = new IfStatement()
                            .condition(thisVariable.getField(cseFields.getEvaluatedField()))
                            .ifFalse(new BytecodeBlock()
                                    .append(thisVariable)
                                    .append(compiler.compile(cse, scope, Optional.empty()))
                                    .append(boxPrimitiveIfNecessary(scope, type))
                                    .putField(cseFields.getResultField())
                                    .append(thisVariable.setField(cseFields.getEvaluatedField(), constantBoolean(true))));

                    body.append(ifStatement)
                            .append(thisVariable)
                            .getField(cseFields.getResultField())
                            .retObject();

                    methods.add(method);
                    cseMap.put(cseVariable, cseFields);
                }
            }
        }
        return methods.build();
    }

    static RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompiler(
            Map<VariableReferenceExpression, CommonSubExpressionFields> variableMap)
    {
        return new RowExpressionVisitor<BytecodeNode, Scope>()
        {
            @Override
            public BytecodeNode visitInputReference(InputReferenceExpression node, Scope scope)
            {
                int field = node.getField();
                Type type = node.getType();
                Variable wasNullVariable = scope.getVariable("wasNull");
                Variable cursorVariable = scope.getVariable("cursor");

                Class<?> javaType = type.getJavaType();
                if (!javaType.isPrimitive() && javaType != Slice.class) {
                    javaType = Object.class;
                }

                IfStatement ifStatement = new IfStatement();
                ifStatement.condition()
                        .setDescription(format("cursor.get%s(%d)", type, field))
                        .getVariable(cursorVariable)
                        .push(field)
                        .invokeInterface(RecordCursor.class, "isNull", boolean.class, int.class);

                ifStatement.ifTrue()
                        .putVariable(wasNullVariable, true)
                        .pushJavaDefault(javaType);

                ifStatement.ifFalse()
                        .getVariable(cursorVariable)
                        .push(field)
                        .invokeInterface(RecordCursor.class, "get" + Primitives.wrap(javaType).getSimpleName(), javaType, int.class);

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

            @Override
            public BytecodeNode visitLambda(LambdaDefinitionExpression lambda, Scope context)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public BytecodeNode visitVariableReference(VariableReferenceExpression reference, Scope context)
            {
                CommonSubExpressionFields fields = variableMap.get(reference);
                return new BytecodeBlock()
                        .append(context.getThis().invoke(
                                fields.getMethodName(),
                                fields.getResultType(),
                                context.getVariable("properties"),
                                context.getVariable("cursor")))
                        .append(unboxPrimitiveIfNecessary(context, Primitives.wrap(reference.getType().getJavaType())));
            }

            @Override
            public BytecodeNode visitSpecialForm(SpecialFormExpression specialForm, Scope context)
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}
