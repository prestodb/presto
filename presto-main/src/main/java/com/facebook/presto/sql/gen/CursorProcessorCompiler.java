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

import com.facebook.presto.byteCode.ByteCodeBlock;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.Parameter;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.control.ForLoop;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.CursorProcessor;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;

import java.util.List;

import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.OpCode.NOP;
import static com.facebook.presto.byteCode.Parameter.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.sql.gen.ByteCodeUtils.generateWrite;
import static java.lang.String.format;

public class CursorProcessorCompiler
        implements BodyCompiler<CursorProcessor>
{
    private final Metadata metadata;

    public CursorProcessorCompiler(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public void generateMethods(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, RowExpression filter, List<RowExpression> projections)
    {
        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);
        generateProcessMethod(classDefinition, projections.size());
        generateFilterMethod(classDefinition, callSiteBinder, cachedInstanceBinder, filter);

        for (int i = 0; i < projections.size(); i++) {
            generateProjectMethod(classDefinition, callSiteBinder, cachedInstanceBinder, "project_" + i, projections.get(i));
        }

        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC));
        ByteCodeBlock constructorBody = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();
        constructorBody.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);
        cachedInstanceBinder.generateInitializations(thisVariable, constructorBody);
        constructorBody.ret();
    }

    private void generateProcessMethod(ClassDefinition classDefinition, int projections)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter cursor = arg("cursor", RecordCursor.class);
        Parameter count = arg("count", int.class);
        Parameter pageBuilder = arg("pageBuilder", PageBuilder.class);
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "process", type(int.class), session, cursor, count, pageBuilder);

        Scope scope = method.getScope();
        Variable completedPositionsVariable = scope.declareVariable(int.class, "completedPositions");

        method.getBody()
                .comment("int completedPositions = 0;")
                .putVariable(completedPositionsVariable, 0);

        //
        // for loop loop body
        //
        LabelNode done = new LabelNode("done");
        ForLoop forLoop = new ForLoop()
                .initialize(NOP)
                .condition(new ByteCodeBlock()
                                .comment("completedPositions < count")
                                .getVariable(completedPositionsVariable)
                                .getVariable(count)
                                .invokeStatic(CompilerOperations.class, "lessThan", boolean.class, int.class, int.class)
                )
                .update(new ByteCodeBlock()
                                .comment("completedPositions++")
                                .incrementVariable(completedPositionsVariable, (byte) 1)
                );

        ByteCodeBlock forLoopBody = new ByteCodeBlock()
                .comment("if (pageBuilder.isFull()) break;")
                .append(new ByteCodeBlock()
                        .getVariable(pageBuilder)
                        .invokeVirtual(PageBuilder.class, "isFull", boolean.class)
                        .ifTrueGoto(done))
                .comment("if (!cursor.advanceNextPosition()) break;")
                .append(new ByteCodeBlock()
                        .getVariable(cursor)
                        .invokeInterface(RecordCursor.class, "advanceNextPosition", boolean.class)
                        .ifFalseGoto(done));

        forLoop.body(forLoopBody);

        // if (filter(cursor))
        IfStatement ifStatement = new IfStatement();
        ifStatement.condition()
                .append(method.getThis())
                .getVariable(session)
                .getVariable(cursor)
                .invokeVirtual(classDefinition.getType(), "filter", type(boolean.class), type(ConnectorSession.class), type(RecordCursor.class));

        // pageBuilder.declarePosition();
        ifStatement.ifTrue()
                .getVariable(pageBuilder)
                .invokeVirtual(PageBuilder.class, "declarePosition", void.class);

        // this.project_43(session, cursor, pageBuilder.getBlockBuilder(42)));
        for (int projectionIndex = 0; projectionIndex < projections; projectionIndex++) {
            ifStatement.ifTrue()
                    .append(method.getThis())
                    .getVariable(session)
                    .getVariable(cursor);

            // pageBuilder.getBlockBuilder(0)
            ifStatement.ifTrue()
                    .getVariable(pageBuilder)
                    .push(projectionIndex)
                    .invokeVirtual(PageBuilder.class, "getBlockBuilder", BlockBuilder.class, int.class);

            // project(block..., blockBuilder)
            ifStatement.ifTrue()
                    .invokeVirtual(classDefinition.getType(),
                    "project_" + projectionIndex,
                    type(void.class),
                    type(ConnectorSession.class),
                    type(RecordCursor.class),
                    type(BlockBuilder.class));
        }
        forLoopBody.append(ifStatement);

        method.getBody()
                .append(forLoop)
                .visitLabel(done)
                .comment("return completedPositions;")
                .getVariable(completedPositionsVariable)
                .retInt();
    }

    private void generateFilterMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, CachedInstanceBinder cachedInstanceBinder, RowExpression filter)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter cursor = arg("cursor", RecordCursor.class);
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "filter", type(boolean.class), session, cursor);

        method.comment("Filter: %s", filter);

        Scope scope = method.getScope();
        Variable wasNullVariable = scope.declareVariable(type(boolean.class), "wasNull");

        ByteCodeExpressionVisitor visitor = new ByteCodeExpressionVisitor(callSiteBinder, cachedInstanceBinder, fieldReferenceCompiler(cursor, wasNullVariable), metadata.getFunctionRegistry());

        LabelNode end = new LabelNode("end");
        method.getBody()
                .comment("boolean wasNull = false;")
                .putVariable(wasNullVariable, false)
                .comment("evaluate filter: " + filter)
                .append(filter.accept(visitor, scope))
                .comment("if (wasNull) return false;")
                .getVariable(wasNullVariable)
                .ifFalseGoto(end)
                .pop(boolean.class)
                .push(false)
                .visitLabel(end)
                .retBoolean();
    }

    private void generateProjectMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, CachedInstanceBinder cachedInstanceBinder, String methodName, RowExpression projection)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter cursor = arg("cursor", RecordCursor.class);
        Parameter output = arg("output", BlockBuilder.class);
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), methodName, type(void.class), session, cursor, output);

        method.comment("Projection: %s", projection.toString());

        Scope scope = method.getScope();
        Variable wasNullVariable = scope.declareVariable(type(boolean.class), "wasNull");

        ByteCodeBlock body = method.getBody()
                .comment("boolean wasNull = false;")
                .putVariable(wasNullVariable, false);

        ByteCodeExpressionVisitor visitor = new ByteCodeExpressionVisitor(callSiteBinder, cachedInstanceBinder, fieldReferenceCompiler(cursor, wasNullVariable), metadata.getFunctionRegistry());

        body.getVariable(output)
                .comment("evaluate projection: " + projection.toString())
                .append(projection.accept(visitor, scope))
                .append(generateWrite(callSiteBinder, scope, wasNullVariable, projection.getType()))
                .ret();
    }

    private RowExpressionVisitor<Scope, ByteCodeNode> fieldReferenceCompiler(final Variable cursorVariable, final Variable wasNullVariable)
    {
        return new RowExpressionVisitor<Scope, ByteCodeNode>()
        {
            @Override
            public ByteCodeNode visitInputReference(InputReferenceExpression node, Scope scope)
            {
                int field = node.getField();
                Type type = node.getType();

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
            public ByteCodeNode visitCall(CallExpression call, Scope scope)
            {
                throw new UnsupportedOperationException("not yet implemented");
            }

            @Override
            public ByteCodeNode visitConstant(ConstantExpression literal, Scope scope)
            {
                throw new UnsupportedOperationException("not yet implemented");
            }
        };
    }
}
