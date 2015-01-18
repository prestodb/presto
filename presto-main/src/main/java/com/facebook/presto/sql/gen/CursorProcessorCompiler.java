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

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.MethodDefinition;
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

import java.util.List;

import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.NamedParameterDefinition.arg;
import static com.facebook.presto.byteCode.OpCode.NOP;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.control.ForLoop.ForLoopBuilder;
import static com.facebook.presto.byteCode.control.IfStatement.IfStatementBuilder;
import static com.facebook.presto.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
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
        generateProcessMethod(classDefinition, projections.size());
        generateFilterMethod(classDefinition, callSiteBinder, filter);

        for (int i = 0; i < projections.size(); i++) {
            generateProjectMethod(classDefinition, callSiteBinder, "project_" + i, projections.get(i));
        }
    }

    private void generateProcessMethod(ClassDefinition classDefinition, int projections)
    {
        CompilerContext context = new CompilerContext(BOOTSTRAP_METHOD);
        MethodDefinition method = classDefinition.declareMethod(context,
                a(PUBLIC),
                "process",
                type(int.class),
                arg("session", ConnectorSession.class),
                arg("cursor", RecordCursor.class),
                arg("count", int.class),
                arg("pageBuilder", PageBuilder.class));

        Variable sessionVariable = context.getVariable("session");
        Variable cursorVariable = context.getVariable("cursor");
        Variable countVariable = context.getVariable("count");
        Variable pageBuilderVariable = context.getVariable("pageBuilder");

        Variable completedPositionsVariable = context.declareVariable(int.class, "completedPositions");

        method.getBody()
                .comment("int completedPositions = 0;")
                .putVariable(completedPositionsVariable, 0);

        //
        // for loop loop body
        //
        LabelNode done = new LabelNode("done");
        ForLoopBuilder forLoop = ForLoop.forLoopBuilder(context)
                .initialize(NOP)
                .condition(new Block(context)
                                .comment("completedPositions < count")
                                .getVariable(completedPositionsVariable)
                                .getVariable(countVariable)
                                .invokeStatic(CompilerOperations.class, "lessThan", boolean.class, int.class, int.class)
                )
                .update(new Block(context)
                                .comment("completedPositions++")
                                .incrementVariable(completedPositionsVariable, (byte) 1)
                );

        Block forLoopBody = new Block(context)
                .comment("if (pageBuilder.isFull()) break;")
                .append(new Block(context)
                        .getVariable(pageBuilderVariable)
                        .invokeVirtual(PageBuilder.class, "isFull", boolean.class)
                        .ifTrueGoto(done))
                .comment("if (!cursor.advanceNextPosition()) break;")
                .append(new Block(context)
                        .getVariable(cursorVariable)
                        .invokeInterface(RecordCursor.class, "advanceNextPosition", boolean.class)
                        .ifFalseGoto(done));

        forLoop.body(forLoopBody);

        // if (filter(cursor))
        IfStatementBuilder ifStatement = new IfStatementBuilder(context);
        ifStatement.condition(new Block(context)
                .pushThis()
                .getVariable(sessionVariable)
                .getVariable(cursorVariable)
                .invokeVirtual(classDefinition.getType(), "filter", type(boolean.class), type(ConnectorSession.class), type(RecordCursor.class)));

        Block trueBlock = new Block(context);
        ifStatement.ifTrue(trueBlock);

        // pageBuilder.declarePosition();
        trueBlock.getVariable(pageBuilderVariable)
                .invokeVirtual(PageBuilder.class, "declarePosition", void.class);

        // this.project_43(session, cursor, pageBuilder.getBlockBuilder(42)));
        for (int projectionIndex = 0; projectionIndex < projections; projectionIndex++) {
            trueBlock.pushThis()
                    .getVariable(sessionVariable)
                    .getVariable(cursorVariable);

            // pageBuilder.getBlockBuilder(0)
            trueBlock.getVariable(pageBuilderVariable)
                    .push(projectionIndex)
                    .invokeVirtual(PageBuilder.class, "getBlockBuilder", BlockBuilder.class, int.class);

            // project(block..., blockBuilder)
            trueBlock.invokeVirtual(classDefinition.getType(),
                    "project_" + projectionIndex,
                    type(void.class),
                    type(ConnectorSession.class),
                    type(RecordCursor.class),
                    type(BlockBuilder.class));
        }
        forLoopBody.append(ifStatement.build());

        method.getBody()
                .append(forLoop.build())
                .visitLabel(done)
                .comment("return completedPositions;")
                .getVariable(completedPositionsVariable)
                .retInt();
    }

    private void generateFilterMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, RowExpression filter)
    {
        CompilerContext context = new CompilerContext(BOOTSTRAP_METHOD);
        MethodDefinition method = classDefinition.declareMethod(
                context,
                a(PUBLIC),
                "filter",
                type(boolean.class),
                arg("session", ConnectorSession.class),
                arg("cursor", RecordCursor.class));

        method.comment("Filter: %s", filter);

        Variable wasNullVariable = context.declareVariable(type(boolean.class), "wasNull");
        Variable cursorVariable = context.getVariable("cursor");

        ByteCodeExpressionVisitor visitor = new ByteCodeExpressionVisitor(callSiteBinder, fieldReferenceCompiler(cursorVariable, wasNullVariable), metadata.getFunctionRegistry());

        LabelNode end = new LabelNode("end");
        method.getBody()
                .comment("boolean wasNull = false;")
                .putVariable(wasNullVariable, false)
                .comment("evaluate filter: " + filter)
                .append(filter.accept(visitor, context))
                .comment("if (wasNull) return false;")
                .getVariable(wasNullVariable)
                .ifFalseGoto(end)
                .pop(boolean.class)
                .push(false)
                .visitLabel(end)
                .retBoolean();
    }

    private void generateProjectMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, String methodName, RowExpression projection)
    {
        CompilerContext context = new CompilerContext(BOOTSTRAP_METHOD);
        MethodDefinition method = classDefinition.declareMethod(context,
                a(PUBLIC),
                methodName,
                type(void.class),
                arg("session", ConnectorSession.class),
                arg("cursor", RecordCursor.class),
                arg("output", BlockBuilder.class));

        method.comment("Projection: %s", projection.toString());

        Variable outputVariable = context.getVariable("output");

        Variable cursorVariable = context.getVariable("cursor");
        Variable wasNullVariable = context.declareVariable(type(boolean.class), "wasNull");

        Block body = method.getBody()
                .comment("boolean wasNull = false;")
                .putVariable(wasNullVariable, false);

        ByteCodeExpressionVisitor visitor = new ByteCodeExpressionVisitor(callSiteBinder, fieldReferenceCompiler(cursorVariable, wasNullVariable), metadata.getFunctionRegistry());

        body.getVariable(outputVariable)
                .comment("evaluate projection: " + projection.toString())
                .append(projection.accept(visitor, context))
                .append(generateWrite(callSiteBinder, context, wasNullVariable, projection.getType()))
                .ret();
    }

    private RowExpressionVisitor<CompilerContext, ByteCodeNode> fieldReferenceCompiler(final Variable cursorVariable, final Variable wasNullVariable)
    {
        return new RowExpressionVisitor<CompilerContext, ByteCodeNode>()
        {
            @Override
            public ByteCodeNode visitInputReference(InputReferenceExpression node, CompilerContext context)
            {
                int field = node.getField();
                Type type = node.getType();

                Class<?> javaType = type.getJavaType();

                Block isNullCheck = new Block(context)
                        .setDescription(format("cursor.get%s(%d)", type, field))
                        .getVariable(cursorVariable)
                        .push(field)
                        .invokeInterface(RecordCursor.class, "isNull", boolean.class, int.class);

                Block isNull = new Block(context)
                        .putVariable(wasNullVariable, true)
                        .pushJavaDefault(javaType);

                Block isNotNull = new Block(context)
                        .getVariable(cursorVariable)
                        .push(field);

                String methodName = "get" + Primitives.wrap(javaType).getSimpleName();
                isNotNull.invokeInterface(RecordCursor.class, methodName, javaType, int.class);

                return new IfStatement(context, isNullCheck, isNull, isNotNull);
            }

            @Override
            public ByteCodeNode visitCall(CallExpression call, CompilerContext context)
            {
                throw new UnsupportedOperationException("not yet implemented");
            }

            @Override
            public ByteCodeNode visitConstant(ConstantExpression literal, CompilerContext context)
            {
                throw new UnsupportedOperationException("not yet implemented");
            }
        };
    }
}
