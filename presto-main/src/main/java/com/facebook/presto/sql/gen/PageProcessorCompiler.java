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
import com.facebook.presto.byteCode.expression.ByteCodeExpression;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.PageProcessor;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.Expressions;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.Parameter.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.add;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantFalse;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantInt;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.lessThan;
import static com.facebook.presto.byteCode.instruction.JumpInstruction.jump;
import static com.facebook.presto.sql.gen.ByteCodeUtils.generateWrite;
import static com.facebook.presto.sql.gen.ByteCodeUtils.loadConstant;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

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
        ImmutableList.Builder<MethodDefinition> projectionMethods = ImmutableList.builder();
        for (int i = 0; i < projections.size(); i++) {
            projectionMethods.add(generateProjectMethod(classDefinition, callSiteBinder, "project_" + i, projections.get(i)));
        }
        generateProcessMethod(classDefinition, filter, projections, projectionMethods.build());
        generateFilterMethod(classDefinition, callSiteBinder, filter);
    }

    private static void generateProcessMethod(ClassDefinition classDefinition, RowExpression filter, List<RowExpression> projections, List<MethodDefinition> projectionMethods)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter page = arg("page", Page.class);
        Parameter start = arg("start", int.class);
        Parameter end = arg("end", int.class);
        Parameter pageBuilder = arg("pageBuilder", PageBuilder.class);
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "process", type(int.class), session, page, start, end, pageBuilder);

        Scope scope = method.getScope();
        Variable thisVariable = method.getThis();

        // extract blocks
        List<Integer> allInputChannels = getInputChannels(Iterables.concat(projections, ImmutableList.of(filter)));
        ImmutableMap.Builder<Integer, Variable> channelBlockBuilder = ImmutableMap.builder();
        for (int channel : allInputChannels) {
            Variable blockVariable = scope.declareVariable(Block.class, "block_" + channel);
            method.getBody().append(blockVariable.set(page.invoke("getBlock", Block.class, constantInt(channel))));
            channelBlockBuilder.put(channel, blockVariable);
        }
        Map<Integer, Variable> channelBlock = channelBlockBuilder.build();
        Map<RowExpression, List<Variable>> expressionInputBlocks = getExpressionInputBlocks(projections, filter, channelBlock);

        // extract block builders
        ImmutableList.Builder<Variable> builder = ImmutableList.<Variable>builder();
        for (int projectionIndex = 0; projectionIndex < projections.size(); projectionIndex++) {
            Variable blockBuilder = scope.declareVariable(BlockBuilder.class, "blockBuilder_" + projectionIndex);
            method.getBody().append(blockBuilder.set(pageBuilder.invoke("getBlockBuilder", BlockBuilder.class, constantInt(projectionIndex))));
            builder.add(blockBuilder);
        }
        List<Variable> blockBuilders = builder.build();

        // projection body
        Variable position = scope.declareVariable(int.class, "position");

        ByteCodeBlock project = new ByteCodeBlock()
                .append(pageBuilder.invoke("declarePosition", void.class));

        for (int projectionIndex = 0; projectionIndex < projections.size(); projectionIndex++) {
            RowExpression projection = projections.get(projectionIndex);
            project.append(invokeProject(thisVariable, session, expressionInputBlocks.get(projection), position, blockBuilders.get(projectionIndex), projectionMethods.get(projectionIndex)));
        }
        LabelNode done = new LabelNode("done");

        // for loop loop body
        ForLoop loop = new ForLoop()
                .initialize(position.set(start))
                .condition(lessThan(position, end))
                .update(position.set(add(position, constantInt(1))))
                .body(new ByteCodeBlock()
                        .append(new IfStatement()
                                .condition(pageBuilder.invoke("isFull", boolean.class))
                                .ifTrue(jump(done)))
                        .append(new IfStatement()
                                .condition(invokeFilter(thisVariable, session, expressionInputBlocks.get(filter), position))
                                .ifTrue(project)));

        method.getBody()
                .append(loop)
                .visitLabel(done)
                .append(position.ret());
    }

    private void generateFilterMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, RowExpression filter)
    {
        Parameter session = arg("session", ConnectorSession.class);
        List<Parameter> blocks = toBlockParameters(getInputChannels(filter));
        Parameter position = arg("position", int.class);
        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filter",
                type(boolean.class),
                ImmutableList.<Parameter>builder()
                        .add(session)
                        .addAll(blocks)
                        .add(position)
                        .build());

        method.comment("Filter: %s", filter.toString());

        Scope scope = method.getScope();
        Variable wasNullVariable = scope.declareVariable(type(boolean.class), "wasNull");

        ByteCodeExpressionVisitor visitor = new ByteCodeExpressionVisitor(
                callSiteBinder,
                fieldReferenceCompiler(callSiteBinder, position, wasNullVariable),
                metadata.getFunctionRegistry());
        ByteCodeNode body = filter.accept(visitor, scope);

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

    private MethodDefinition generateProjectMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, String methodName, RowExpression projection)
    {
        Parameter session = arg("session", ConnectorSession.class);
        List<Parameter> inputs = toBlockParameters(getInputChannels(projection));
        Parameter position = arg("position", int.class);
        Parameter output = arg("output", BlockBuilder.class);
        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                methodName,
                type(void.class),
                ImmutableList.<Parameter>builder()
                        .add(session)
                        .addAll(inputs)
                        .add(position)
                        .add(output)
                        .build());

        method.comment("Projection: %s", projection.toString());

        Scope scope = method.getScope();
        Variable wasNullVariable = scope.declareVariable(type(boolean.class), "wasNull");

        ByteCodeBlock body = method.getBody()
                .append(wasNullVariable.set(constantFalse()));

        ByteCodeExpressionVisitor visitor = new ByteCodeExpressionVisitor(callSiteBinder, fieldReferenceCompiler(callSiteBinder, position, wasNullVariable), metadata.getFunctionRegistry());

        body.getVariable(output)
                .comment("evaluate projection: " + projection.toString())
                .append(projection.accept(visitor, scope))
                .append(generateWrite(callSiteBinder, scope, wasNullVariable, projection.getType()))
                .ret();
        return method;
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

    private static List<Parameter> toBlockParameters(List<Integer> inputChannels)
    {
        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        for (int channel : inputChannels) {
            parameters.add(arg("block_" + channel, Block.class));
        }
        return parameters.build();
    }

    private static RowExpressionVisitor<Scope, ByteCodeNode> fieldReferenceCompiler(final CallSiteBinder callSiteBinder, final Variable positionVariable, final Variable wasNullVariable)
    {
        return new RowExpressionVisitor<Scope, ByteCodeNode>()
        {
            @Override
            public ByteCodeNode visitInputReference(InputReferenceExpression node, Scope scope)
            {
                int field = node.getField();
                Type type = node.getType();
                Variable block = scope.getVariable("block_" + field);

                Class<?> javaType = type.getJavaType();
                if (!javaType.isPrimitive() && javaType != Slice.class) {
                    javaType = Object.class;
                }

                IfStatement ifStatement = new IfStatement();
                ifStatement.condition()
                        .setDescription(format("block_%d.get%s()", field, type))
                        .append(block)
                        .getVariable(positionVariable)
                        .invokeInterface(Block.class, "isNull", boolean.class, int.class);

                ifStatement.ifTrue()
                        .putVariable(wasNullVariable, true)
                        .pushJavaDefault(javaType);

                String methodName = "get" + Primitives.wrap(javaType).getSimpleName();

                ifStatement.ifFalse()
                        .append(loadConstant(callSiteBinder.bind(type, Type.class)))
                        .append(block)
                        .getVariable(positionVariable)
                        .invokeInterface(Type.class, methodName, javaType, Block.class, int.class);

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

    private static Map<RowExpression, List<Variable>> getExpressionInputBlocks(List<RowExpression> projections, RowExpression filter, Map<Integer, Variable> channelBlock)
    {
        Map<RowExpression, List<Variable>> inputBlocksBuilder = new HashMap<>();

        for (RowExpression projection : projections) {
            List<Variable> inputBlocks = getInputChannels(projection).stream()
                    .map(channelBlock::get)
                    .collect(toList());

            List<Variable> existingVariables = inputBlocksBuilder.get(projection);
            // Constant expressions or expressions that are reused, should reference the same input blocks
            checkState(existingVariables == null || existingVariables.equals(inputBlocks), "malformed RowExpression");
            inputBlocksBuilder.put(projection, inputBlocks);
        }

        List<Variable> filterBlocks = getInputChannels(filter).stream()
                .map(channelBlock::get)
                .collect(toList());

        inputBlocksBuilder.put(filter, filterBlocks);

        return inputBlocksBuilder;
    }

    private static ByteCodeExpression invokeFilter(ByteCodeExpression objRef, ByteCodeExpression session, List<? extends ByteCodeExpression> blockVariables, ByteCodeExpression position)
    {
        List<ByteCodeExpression> params = ImmutableList.<ByteCodeExpression>builder()
                .add(session)
                .addAll(blockVariables)
                .add(position)
                .build();

        return objRef.invoke("filter", boolean.class, params);
    }

    private static ByteCodeNode invokeProject(Variable objRef, Variable session, List<Variable> blockVariables, ByteCodeExpression position, Variable blockBuilder, MethodDefinition projectionMethod)
    {
        List<ByteCodeExpression> params = ImmutableList.<ByteCodeExpression>builder()
                .add(session)
                .addAll(blockVariables)
                .add(position)
                .add(blockBuilder)
                .build();
        return new ByteCodeBlock().append(objRef.invoke(projectionMethod, params));
    }
}
