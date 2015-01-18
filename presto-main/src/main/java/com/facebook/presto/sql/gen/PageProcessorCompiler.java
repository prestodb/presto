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
import com.facebook.presto.byteCode.NamedParameterDefinition;
import com.facebook.presto.byteCode.ParameterizedType;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.control.ForLoop;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.PageProcessor;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.Expressions;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Primitives;

import java.util.List;
import java.util.TreeSet;

import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.NamedParameterDefinition.arg;
import static com.facebook.presto.byteCode.OpCode.NOP;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.control.ForLoop.ForLoopBuilder;
import static com.facebook.presto.byteCode.control.IfStatement.IfStatementBuilder;
import static com.facebook.presto.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static com.facebook.presto.sql.gen.ByteCodeUtils.generateWrite;
import static com.facebook.presto.sql.gen.ByteCodeUtils.loadConstant;
import static java.lang.String.format;
import static java.util.Collections.nCopies;

public class PageProcessorCompiler
        implements BodyCompiler<PageProcessor>
{
    private final Metadata metadata;

    public PageProcessorCompiler(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public void generateMethods(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, RowExpression filter, List<RowExpression> projections)
    {
        generateProcessMethod(classDefinition, filter, projections);
        generateFilterMethod(classDefinition, callSiteBinder, filter);

        for (int i = 0; i < projections.size(); i++) {
            generateProjectMethod(classDefinition, callSiteBinder, "project_" + i, projections.get(i));
        }
    }

    private void generateProcessMethod(ClassDefinition classDefinition, RowExpression filter, List<RowExpression> projections)
    {
        CompilerContext context = new CompilerContext(BOOTSTRAP_METHOD);
        MethodDefinition method = classDefinition.declareMethod(context,
                a(PUBLIC),
                "process",
                type(int.class),
                arg("session", ConnectorSession.class),
                arg("page", Page.class),
                arg("start", int.class),
                arg("end", int.class),
                arg("pageBuilder", PageBuilder.class));

        Variable sessionVariable = context.getVariable("session");
        Variable pageVariable = context.getVariable("page");
        Variable startVariable = context.getVariable("start");
        Variable endVariable = context.getVariable("end");
        Variable pageBuilderVariable = context.getVariable("pageBuilder");

        Variable positionVariable = context.declareVariable(int.class, "position");

        method.getBody()
                .comment("int position = start;")
                .getVariable(startVariable)
                .putVariable(positionVariable);

        List<Integer> allInputChannels = getInputChannels(Iterables.concat(projections, ImmutableList.of(filter)));
        for (int channel : allInputChannels) {
            Variable blockVariable = context.declareVariable(com.facebook.presto.spi.block.Block.class, "block_" + channel);
            method.getBody()
                    .comment("Block %s = page.getBlock(%s);", blockVariable.getName(), channel)
                    .getVariable(pageVariable)
                    .push(channel)
                    .invokeVirtual(Page.class, "getBlock", com.facebook.presto.spi.block.Block.class, int.class)
                    .putVariable(blockVariable);
        }

        //
        // for loop loop body
        //
        LabelNode done = new LabelNode("done");

        Block loopBody = new Block(context);

        ForLoopBuilder loop = ForLoop.forLoopBuilder(context)
                .initialize(NOP)
                .condition(new Block(context)
                                .comment("position < end")
                                .getVariable(positionVariable)
                                .getVariable(endVariable)
                                .invokeStatic(CompilerOperations.class, "lessThan", boolean.class, int.class, int.class)
                )
                .update(new Block(context)
                        .comment("position++")
                        .incrementVariable(positionVariable, (byte) 1))
                .body(loopBody);

        loopBody.comment("if (pageBuilder.isFull()) break;")
                .getVariable(pageBuilderVariable)
                .invokeVirtual(PageBuilder.class, "isFull", boolean.class)
                .ifTrueGoto(done);

        // if (filter(cursor))
        IfStatementBuilder filterBlock = new IfStatementBuilder(context);

        Block trueBlock = new Block(context);
        filterBlock.condition(new Block(context)
                .pushThis()
                .getVariable(sessionVariable)
                .append(pushBlockVariables(context, getInputChannels(filter)))
                .getVariable(positionVariable)
                .invokeVirtual(classDefinition.getType(),
                        "filter",
                        type(boolean.class),
                        ImmutableList.<ParameterizedType>builder()
                                .add(type(ConnectorSession.class))
                                .addAll(nCopies(getInputChannels(filter).size(), type(com.facebook.presto.spi.block.Block.class)))
                                .add(type(int.class))
                                .build()))
                .ifTrue(trueBlock);

        trueBlock.getVariable(pageBuilderVariable)
                .invokeVirtual(PageBuilder.class, "declarePosition", void.class);

        for (int projectionIndex = 0; projectionIndex < projections.size(); projectionIndex++) {
            List<Integer> inputChannels = getInputChannels(projections.get(projectionIndex));

            trueBlock.pushThis()
                    .getVariable(sessionVariable)
                    .append(pushBlockVariables(context, inputChannels))
                    .getVariable(positionVariable);

            trueBlock.comment("pageBuilder.getBlockBuilder(" + projectionIndex + ")")
                    .getVariable(pageBuilderVariable)
                    .push(projectionIndex)
                    .invokeVirtual(PageBuilder.class, "getBlockBuilder", BlockBuilder.class, int.class);

            trueBlock.comment("project_" + projectionIndex + "(session, block_" + inputChannels + ", position, blockBuilder)")
                    .invokeVirtual(classDefinition.getType(),
                            "project_" + projectionIndex,
                            type(void.class),
                            ImmutableList.<ParameterizedType>builder()
                                    .add(type(ConnectorSession.class))
                                    .addAll(nCopies(inputChannels.size(), type(com.facebook.presto.spi.block.Block.class)))
                                    .add(type(int.class))
                                    .add(type(BlockBuilder.class))
                                    .build());
        }

        loopBody.append(filterBlock.build());

        method.getBody()
                .append(loop.build())
                .visitLabel(done)
                .comment("return position;")
                .getVariable(positionVariable)
                .retInt();
    }

    private void generateFilterMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, RowExpression filter)
    {
        CompilerContext context = new CompilerContext(BOOTSTRAP_METHOD);
        MethodDefinition method = classDefinition.declareMethod(context,
                a(PUBLIC),
                "filter",
                type(boolean.class),
                ImmutableList.<NamedParameterDefinition>builder()
                        .add(arg("session", ConnectorSession.class))
                        .addAll(toBlockParameters(getInputChannels(filter)))
                        .add(arg("position", int.class))
                        .build());

        method.comment("Filter: %s", filter.toString());

        Variable positionVariable = context.getVariable("position");
        Variable wasNullVariable = context.declareVariable(type(boolean.class), "wasNull");

        ByteCodeExpressionVisitor visitor = new ByteCodeExpressionVisitor(
                callSiteBinder,
                fieldReferenceCompiler(callSiteBinder, positionVariable, wasNullVariable),
                metadata.getFunctionRegistry());
        ByteCodeNode body = filter.accept(visitor, context);

        LabelNode end = new LabelNode("end");
        method
                .getBody()
                .comment("boolean wasNull = false;")
                .putVariable(wasNullVariable, false)
                .append(body)
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
                ImmutableList.<NamedParameterDefinition>builder()
                        .add(arg("session", ConnectorSession.class))
                        .addAll(toBlockParameters(getInputChannels(projection)))
                        .add(arg("position", int.class))
                        .add(arg("output", BlockBuilder.class))
                        .build());

        method.comment("Projection: %s", projection.toString());

        Variable positionVariable = context.getVariable("position");
        Variable outputVariable = context.getVariable("output");

        Variable wasNullVariable = context.declareVariable(type(boolean.class), "wasNull");

        Block body = method.getBody()
                .comment("boolean wasNull = false;")
                .putVariable(wasNullVariable, projection.getType().getJavaType() == void.class);

        ByteCodeExpressionVisitor visitor = new ByteCodeExpressionVisitor(callSiteBinder, fieldReferenceCompiler(callSiteBinder, positionVariable, wasNullVariable), metadata.getFunctionRegistry());

        body.getVariable(outputVariable)
                .comment("evaluate projection: " + projection.toString())
                .append(projection.accept(visitor, context))
                .append(generateWrite(callSiteBinder, context, wasNullVariable, projection.getType()))
                .ret();
    }

    private static List<Integer> getInputChannels(Iterable<RowExpression> expressions)
    {
        TreeSet<Integer> channels = new TreeSet<>();
        for (RowExpression expression : Expressions.subExpressions(expressions)) {
            if (expression instanceof InputReferenceExpression) {
                channels.add(((InputReferenceExpression) expression).getField());
            }
        }
        return ImmutableList.copyOf(channels);
    }

    private static List<Integer> getInputChannels(RowExpression expression)
    {
        return getInputChannels(ImmutableList.of(expression));
    }

    private static List<NamedParameterDefinition> toBlockParameters(List<Integer> inputChannels)
    {
        ImmutableList.Builder<NamedParameterDefinition> parameters = ImmutableList.builder();
        for (int channel : inputChannels) {
            parameters.add(arg("block_" + channel, com.facebook.presto.spi.block.Block.class));
        }
        return parameters.build();
    }

    private static ByteCodeNode pushBlockVariables(CompilerContext context, List<Integer> inputs)
    {
        Block block = new Block(context);
        for (int channel : inputs) {
            block.getVariable("block_" + channel);
        }
        return block;
    }

    private RowExpressionVisitor<CompilerContext, ByteCodeNode> fieldReferenceCompiler(final CallSiteBinder callSiteBinder, final Variable positionVariable, final Variable wasNullVariable)
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
                        .setDescription(format("block_%d.get%s()", field, type))
                        .getVariable("block_" + field)
                        .getVariable(positionVariable)
                        .invokeInterface(com.facebook.presto.spi.block.Block.class, "isNull", boolean.class, int.class);

                Block isNull = new Block(context)
                        .putVariable(wasNullVariable, true)
                        .pushJavaDefault(javaType);

                String methodName = "get" + Primitives.wrap(javaType).getSimpleName();

                Block isNotNull = new Block(context)
                        .append(loadConstant(context, callSiteBinder.bind(type, Type.class)))
                        .getVariable("block_" + field)
                        .getVariable(positionVariable)
                        .invokeInterface(Type.class, methodName, javaType, com.facebook.presto.spi.block.Block.class, int.class);

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
