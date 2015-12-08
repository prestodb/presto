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
import com.facebook.presto.byteCode.FieldDefinition;
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
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
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
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.Parameter.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.add;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantFalse;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantInt;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantNull;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantTrue;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.equal;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.invokeStatic;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.lessThan;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.multiply;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.newArray;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.newInstance;
import static com.facebook.presto.byteCode.instruction.JumpInstruction.jump;
import static com.facebook.presto.sql.gen.ByteCodeUtils.generateWrite;
import static com.facebook.presto.sql.gen.ByteCodeUtils.loadConstant;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.concat;
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
        List<MethodDefinition> projectionMethodDefinitions = projectionMethods.build();

        generateConstructor(classDefinition, projections.size());
        generateProcessMethod(classDefinition, filter, projections, projectionMethodDefinitions);
        generateGetNonLazyPageMethod(classDefinition, filter, projections);
        generateProcessColumnarMethod(classDefinition, callSiteBinder, filter, projections, projectionMethodDefinitions);
        generateProcessColumnarDictionaryMethod(classDefinition, callSiteBinder, filter, projections, projectionMethodDefinitions);

        generateFilterPageMethod(classDefinition, filter);
        generateFilterMethod(classDefinition, callSiteBinder, filter);
    }

    private static void generateConstructor(ClassDefinition classDefinition, int projectionCount)
    {
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC));
        FieldDefinition inputDictionaries = classDefinition.declareField(a(PRIVATE, FINAL), "inputDictionaries", Block[].class);
        FieldDefinition outputDictionaries = classDefinition.declareField(a(PRIVATE, FINAL), "outputDictionaries", Block[].class);

        ByteCodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        body.append(thisVariable.setField(inputDictionaries, newArray(type(Block[].class), projectionCount)));
        body.append(thisVariable.setField(outputDictionaries, newArray(type(Block[].class), projectionCount)));
        body.ret();
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
        ByteCodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        // extract blocks
        List<Integer> allInputChannels = getInputChannels(concat(projections, ImmutableList.of(filter)));
        ImmutableMap.Builder<Integer, Variable> builder = ImmutableMap.builder();
        for (int channel : allInputChannels) {
            Variable blockVariable = scope.declareVariable("block_" + channel, body, page.invoke("getBlock", Block.class, constantInt(channel)));
            builder.put(channel, blockVariable);
        }
        Map<Integer, Variable> channelBlocks = builder.build();
        Map<RowExpression, List<Variable>> expressionInputBlocks = getExpressionInputBlocks(projections, filter, channelBlocks);

        // extract block builders
        ImmutableList.Builder<Variable> variableBuilder = ImmutableList.<Variable>builder();
        for (int projectionIndex = 0; projectionIndex < projections.size(); projectionIndex++) {
            Variable blockBuilder = scope.declareVariable("blockBuilder_" + projectionIndex, body, pageBuilder.invoke("getBlockBuilder", BlockBuilder.class, constantInt(projectionIndex)));
            variableBuilder.add(blockBuilder);
        }
        List<Variable> blockBuilders = variableBuilder.build();

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

        body
                .append(loop)
                .visitLabel(done)
                .append(position.ret());
    }

    private static void generateProcessColumnarMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            RowExpression filter,
            List<RowExpression> projections,
            List<MethodDefinition> projectionMethods)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter page = arg("page", Page.class);
        Parameter types = arg("types", List.class);
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "processColumnar", type(Page.class), session, page, types);

        Scope scope = method.getScope();
        ByteCodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        // extract blocks
        List<Integer> allInputChannels = getInputChannels(concat(projections, ImmutableList.of(filter)));
        ImmutableMap.Builder<Integer, Variable> builder = ImmutableMap.builder();
        for (int channel : allInputChannels) {
            Variable blockVariable = scope.declareVariable("block_" + channel, body, page.invoke("getBlock", Block.class, constantInt(channel)));
            builder.put(channel, blockVariable);
        }
        Map<Integer, Variable> channelBlocks = builder.build();
        Map<RowExpression, List<Variable>> expressionInputBlocks = getExpressionInputBlocks(projections, filter, channelBlocks);

        Variable selectedPositions = scope.declareVariable("selectedPositions", body, thisVariable.invoke("filterPage", int[].class, session, page));
        Variable cardinality = scope.declareVariable("cardinality", body, selectedPositions.length());

        body.comment("if no rows selected return null")
                .append(new IfStatement()
                        .condition(equal(cardinality, constantInt(0)))
                        .ifTrue(constantNull(Page.class).ret()));

        if (projectionMethods.isEmpty()) {
            // if no projections, return new page with selected rows
            body.append(newInstance(Page.class, cardinality, newArray(type(Block[].class), 0)).ret());
            return;
        }

        Variable pageBuilder = scope.declareVariable("pageBuilder", body, newInstance(PageBuilder.class, cardinality, types));

        // create variables for blockBuilders
        for (int projectionIndex = 0; projectionIndex < projections.size(); projectionIndex++) {
            scope.declareVariable("blockBuilder_" + projectionIndex, body, pageBuilder.invoke("getBlockBuilder", BlockBuilder.class, constantInt(projectionIndex)));
        }

        scope.declareVariable("positionCount", body, page.invoke("getPositionCount", int.class));
        scope.declareVariable(int.class, "position");

        Variable outputBlocks = scope.declareVariable("outputBlocks", body, newArray(type(Block[].class), projections.size()));

        for (int projectionIndex = 0; projectionIndex < projections.size(); projectionIndex++) {
            RowExpression projection = projections.get(projectionIndex);
            ByteCodeBlock simpleProjectionBlock = getSimpleProjectionBlock(
                    callSiteBinder,
                    scope,
                    projectionMethods.get(projectionIndex),
                    projection,
                    projectionIndex,
                    expressionInputBlocks.get(projection));
            body.append(simpleProjectionBlock);
        }

        // create new page from outputBlocks
        body.append(newInstance(Page.class, cardinality, outputBlocks).ret());
    }

    private static void generateProcessColumnarDictionaryMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            RowExpression filter,
            List<RowExpression> projections,
            List<MethodDefinition> projectionMethods)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter page = arg("page", Page.class);
        Parameter types = arg("types", List.class);
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "processColumnarDictionary", type(Page.class), session, page, types);

        Scope scope = method.getScope();
        ByteCodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        // extract blocks
        List<Integer> allInputChannels = getInputChannels(concat(projections, ImmutableList.of(filter)));
        ImmutableMap.Builder<Integer, Variable> builder = ImmutableMap.builder();
        for (int channel : allInputChannels) {
            Variable blockVariable = scope.declareVariable("block_" + channel, body, page.invoke("getBlock", Block.class, constantInt(channel)));
            builder.put(channel, blockVariable);
        }
        Map<Integer, Variable> channelBlocks = builder.build();
        Map<RowExpression, List<Variable>> expressionInputBlocks = getExpressionInputBlocks(projections, filter, channelBlocks);

        Variable selectedPositions = scope.declareVariable("selectedPositions", body, thisVariable.invoke("filterPage", int[].class, session, page));
        Variable cardinality = scope.declareVariable("cardinality", body, selectedPositions.length());

        body.comment("if no rows selected return null")
                .append(new IfStatement()
                        .condition(equal(cardinality, constantInt(0)))
                        .ifTrue(constantNull(Page.class).ret()));

        if (projectionMethods.isEmpty()) {
            // if no projections, return new page with selected rows
            body.append(newInstance(Page.class, cardinality, newArray(type(Block[].class), 0)).ret());
            return;
        }

        // create PageBuilder
        Variable pageBuilder = scope.declareVariable("pageBuilder", body, newInstance(PageBuilder.class, cardinality, types));

        for (int projectionIndex = 0; projectionIndex < projections.size(); projectionIndex++) {
            scope.declareVariable("blockBuilder_" + projectionIndex, body, pageBuilder.invoke("getBlockBuilder", BlockBuilder.class, constantInt(projectionIndex)));
        }

        body.append(page.set(thisVariable.invoke("getNonLazyPage", Page.class, page)));

        // create outputBlocks
        Variable outputBlocks = scope.declareVariable("outputBlocks", body, newArray(type(Block[].class), projections.size()));

        scope.declareVariable("positionCount", body, page.invoke("getPositionCount", int.class));
        scope.declareVariable(int.class, "position");
        scope.declareVariable(Block.class, "block");

        scope.declareVariable(Block.class, "dictionary");
        scope.declareVariable(Slice.class, "ids");
        scope.declareVariable(int.class, "dictionaryCount");

        scope.declareVariable(Block.class, "outputDictionary");
        scope.declareVariable(int[].class, "outputIds");

        for (int projectionIndex = 0; projectionIndex < projections.size(); projectionIndex++) {
            RowExpression projection = projections.get(projectionIndex);
            List<Integer> inputChannels = getInputChannels(projection);

            ByteCodeBlock simpleProjection = getSimpleProjectionBlock(
                    callSiteBinder,
                    scope,
                    projectionMethods.get(projectionIndex),
                    projection,
                    projectionIndex,
                    expressionInputBlocks.get(projection));

            if (inputChannels.size() != 1) {
                body.append(simpleProjection);
                continue;
            }

            Variable inputBlock = Iterables.getOnlyElement(expressionInputBlocks.get(projection));
            ByteCodeBlock dictionaryProjection = getDictionaryProjectionBlock(
                                scope,
                                projectionMethods.get(projectionIndex),
                                projectionIndex,
                                inputBlock);

            IfStatement ifStatement = new IfStatement()
                    .condition(inputBlock.instanceOf(DictionaryBlock.class))
                    .ifTrue(dictionaryProjection)
                    .ifFalse(simpleProjection);
            body.append(ifStatement);
        }

        body.append(newInstance(Page.class, cardinality, outputBlocks).ret());
    }

    private static ByteCodeBlock getDictionaryProjectionBlock(
            Scope scope,
            MethodDefinition projectionMethod,
            int projectionIndex,
            Variable inputBlock)
    {
        Variable session = scope.getVariable("session");

        Variable cardinality = scope.getVariable("cardinality");
        Variable selectedPositions = scope.getVariable("selectedPositions");

        Variable outputBlocks = scope.getVariable("outputBlocks");
        Variable position = scope.getVariable("position");

        Variable dictionary = scope.getVariable("dictionary");
        Variable ids = scope.getVariable("ids");
        Variable dictionaryCount = scope.getVariable("dictionaryCount");

        Variable outputDictionary = scope.getVariable("outputDictionary");
        Variable outputIds = scope.getVariable("outputIds");

        Variable thisVariable = scope.getThis();
        Variable blockBuilder = scope.getVariable("blockBuilder_" + projectionIndex);

        ByteCodeBlock dictionaryProjection = new ByteCodeBlock().comment("Dictionary projection");

        dictionaryProjection
                .comment("Extract dictionary and ids")
                .append(dictionary.set(inputBlock.cast(DictionaryBlock.class).invoke("getDictionary", Block.class)))
                .append(ids.set(inputBlock.cast(DictionaryBlock.class).invoke("getIds", Slice.class)))
                .append(dictionaryCount.set(dictionary.invoke("getPositionCount", int.class)));

        ByteCodeExpression inputDictionaries = scope.getThis().getField("inputDictionaries", Block[].class);
        ByteCodeExpression outputDictionaries = scope.getThis().getField("outputDictionaries", Block[].class);

        ByteCodeBlock projectDictionary =  new ByteCodeBlock()
                .append(new ForLoop()
                        .initialize(position.set(constantInt(0)))
                        .condition(lessThan(position, dictionaryCount))
                        .update(position.increment())
                        .body(invokeProject(thisVariable, session, ImmutableList.of(dictionary), position, blockBuilder, projectionMethod)))
                .append(outputDictionary.set(blockBuilder.invoke("build", Block.class)))
                .append(inputDictionaries.setElement(projectionIndex, dictionary))
                .append(outputDictionaries.setElement(projectionIndex, outputDictionary));

        dictionaryProjection
                .comment("Project dictionary")
                .append(
                        new IfStatement()
                                .condition(equal(inputDictionaries.getElement(projectionIndex), dictionary))
                                .ifTrue(outputDictionary.set(outputDictionaries.getElement(projectionIndex)))
                                .ifFalse(projectDictionary));

        dictionaryProjection
                .comment("Filter ids")
                .append(outputIds.set(newArray(type(int[].class), cardinality)))
                .append(new ForLoop()
                        .initialize(position.set(constantInt(0)))
                        .condition(lessThan(position, cardinality))
                        .update(position.increment())
                        .body(outputIds.setElement(position, ids.invoke("getInt", int.class, multiply(selectedPositions.getElement(position), constantInt(SizeOf.SIZE_OF_INT))))))
                .append(outputBlocks.setElement(projectionIndex,
                        newInstance(DictionaryBlock.class, cardinality, outputDictionary, invokeStatic(Slices.class, "wrappedIntArray", Slice.class, outputIds)).cast(Block.class)));

        return dictionaryProjection;
    }

    private static ByteCodeBlock getSimpleProjectionBlock(
            CallSiteBinder callSiteBinder,
            Scope scope,
            MethodDefinition projectionMethod,
            RowExpression projection,
            int projectionIndex,
            List<Variable> blocks)
    {
        Variable session = scope.getVariable("session");

        Variable cardinality = scope.getVariable("cardinality");
        Variable selectedPositions = scope.getVariable("selectedPositions");

        Variable page = scope.getVariable("page");
        Variable pageBuilder = scope.getVariable("pageBuilder");
        Variable outputBlocks = scope.getVariable("outputBlocks");

        Variable position = scope.getVariable("position");
        Variable positionCount = scope.getVariable("positionCount");

        Variable thisVariable = scope.getThis();
        Variable blockBuilder = scope.getVariable("blockBuilder_" + projectionIndex);

        ByteCodeBlock projectBlock = new ByteCodeBlock()
                .append(new ForLoop()
                        .initialize(position.set(constantInt(0)))
                        .condition(lessThan(position, cardinality))
                        .update(position.increment())
                        .body(invokeProject(thisVariable,
                                session,
                                blocks,
                                selectedPositions.getElement(position),
                                blockBuilder,
                                projectionMethod)))
                .append(outputBlocks.setElement(projectionIndex, blockBuilder.invoke("build", Block.class)));

        ByteCodeBlock simpleProjection = new ByteCodeBlock();

        if (isIdentityExpression(projection)) {
            // if nothing is filtered out, copy the entire block, else project it
            int channel = Iterables.getOnlyElement(getInputChannels(projection));
            simpleProjection.append(new IfStatement()
                    .condition(equal(cardinality, positionCount))
                    .ifTrue(outputBlocks.setElement(projectionIndex, page.invoke("getBlock", Block.class, constantInt(channel))))
                    .ifFalse(projectBlock));
        }
        else if (isConstantExpression(projection)) {
            // if projection is a constant, create RLE block of constant expression with cardinality positions
            ConstantExpression constantExpression = (ConstantExpression) projection;
            verify(getInputChannels(projection).isEmpty());

            ByteCodeExpression type = pageBuilder.invoke("getType", Type.class, constantInt(projectionIndex));
            ByteCodeExpression value = loadConstant(callSiteBinder, constantExpression.getValue(), Object.class);

            simpleProjection.append(outputBlocks.setElement(projectionIndex, invokeStatic(RunLengthEncodedBlock.class, "create", Block.class, type, value, cardinality)));
        }
        else {
            simpleProjection.append(projectBlock);
        }
        return simpleProjection;
    }

    private static void generateGetNonLazyPageMethod(ClassDefinition classDefinition, RowExpression filter, List<RowExpression> projections)
    {
        Parameter page = arg("page", Page.class);
        MethodDefinition method = classDefinition.declareMethod(a(PRIVATE), "getNonLazyPage", type(Page.class), page);

        Scope scope = method.getScope();
        ByteCodeBlock body = method.getBody();

        List<Integer> allInputChannels = getInputChannels(concat(projections, ImmutableList.of(filter)));
        if (allInputChannels.isEmpty()) {
            body.append(page.ret());
            return;
        }

        ImmutableMap.Builder<Integer, Variable> builder = ImmutableMap.builder();
        for (int channel : allInputChannels) {
            Variable blockVariable = scope.declareVariable("block_" + channel, body, page.invoke("getBlock", Block.class, constantInt(channel)));
            builder.put(channel, blockVariable);
        }
        Map<Integer, Variable> channelBlocks = builder.build();

        Variable blocks = scope.declareVariable("blocks", body, page.invoke("getBlocks", Block[].class));
        Variable positionCount = scope.declareVariable("positionCount", body, page.invoke("getPositionCount", int.class));
        Variable createNewPage = scope.declareVariable("createNewPage", body, constantFalse());

        for (Map.Entry<Integer, Variable> entry : channelBlocks.entrySet()) {
            int channel = entry.getKey();
            Variable inputBlock = entry.getValue();
            IfStatement ifStmt = new IfStatement();
            ifStmt.condition(inputBlock.instanceOf(LazyBlock.class))
                    .ifTrue()
                    .append(blocks.setElement(channel, inputBlock.cast(LazyBlock.class).invoke("getBlock", Block.class)))
                    .append(createNewPage.set(constantTrue()));
            body.append(ifStmt);
        }

        body.append(new IfStatement()
                .condition(createNewPage)
                .ifTrue(page.set(newInstance(Page.class, positionCount, blocks))));

        body.append(page.ret());
    }

    private static void generateFilterPageMethod(ClassDefinition classDefinition, RowExpression filter)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter page = arg("page", Page.class);

        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "filterPage", type(int[].class), session, page);
        method.comment("Filter: %s rows in the page", filter.toString());

        Scope scope = method.getScope();
        Variable thisVariable = method.getThis();
        ByteCodeBlock body = method.getBody();

        Variable positionCount = scope.declareVariable("positionCount", body, page.invoke("getPositionCount", int.class));
        Variable selectedPositions = scope.declareVariable("selectedPositions", body, newArray(type(int[].class), positionCount));

        List<Integer> filterChannels = getInputChannels(filter);

        // extract block variables
        ImmutableList.Builder<Variable> blockVariablesBuilder = ImmutableList.<Variable>builder();
        for (int channel : filterChannels) {
            Variable blockVariable = scope.declareVariable("block_" + channel, body, page.invoke("getBlock", Block.class, constantInt(channel)));
            blockVariablesBuilder.add(blockVariable);
        }
        List<Variable> blockVariables = blockVariablesBuilder.build();

        Variable selectedCount = scope.declareVariable("selectedCount", body, constantInt(0));
        Variable position = scope.declareVariable(int.class, "position");

        IfStatement ifStatement = new IfStatement();
        ifStatement.condition(invokeFilter(thisVariable, session, blockVariables, position))
                .ifTrue()
                .append(selectedPositions.setElement(selectedCount, position))
                .append(selectedCount.increment());

        body.append(new ForLoop()
                .initialize(position.set(constantInt(0)))
                .condition(lessThan(position, positionCount))
                .update(position.increment())
                .body(ifStatement));

        body.append(invokeStatic(Arrays.class, "copyOf", int[].class, selectedPositions, selectedCount).ret());
    }

    private void generateFilterMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, RowExpression filter)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter position = arg("position", int.class);

        List<Parameter> blocks = toBlockParameters(getInputChannels(filter));
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
        ByteCodeBlock body = method.getBody();

        Scope scope = method.getScope();
        Variable wasNullVariable = scope.declareVariable("wasNull", body, constantFalse());

        ByteCodeExpressionVisitor visitor = new ByteCodeExpressionVisitor(
                callSiteBinder,
                fieldReferenceCompiler(callSiteBinder, position, wasNullVariable),
                metadata.getFunctionRegistry());
        ByteCodeNode visitorBody = filter.accept(visitor, scope);

        Variable result = scope.declareVariable(boolean.class, "result");
        body.append(visitorBody)
                .putVariable(result)
                .append(new IfStatement()
                        .condition(wasNullVariable)
                        .ifTrue(constantFalse().ret())
                        .ifFalse(result.ret()));
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
        ByteCodeBlock body = method.getBody();

        Variable wasNullVariable = scope.declareVariable("wasNull", body, constantFalse());
        ByteCodeExpressionVisitor visitor = new ByteCodeExpressionVisitor(callSiteBinder, fieldReferenceCompiler(callSiteBinder, position, wasNullVariable), metadata.getFunctionRegistry());

        body.getVariable(output)
                .comment("evaluate projection: " + projection.toString())
                .append(projection.accept(visitor, scope))
                .append(generateWrite(callSiteBinder, scope, wasNullVariable, projection.getType()))
                .ret();
        return method;
    }

    private static boolean isIdentityExpression(RowExpression expression)
    {
        List<RowExpression> rowExpressions = Expressions.subExpressions(ImmutableList.of(expression));
        return rowExpressions.size() == 1 && Iterables.getOnlyElement(rowExpressions) instanceof InputReferenceExpression;
    }

    private static boolean isConstantExpression(RowExpression expression)
    {
        List<RowExpression> rowExpressions = Expressions.subExpressions(ImmutableList.of(expression));
        return rowExpressions.size() == 1 &&
                Iterables.getOnlyElement(rowExpressions) instanceof ConstantExpression &&
                ((ConstantExpression) Iterables.getOnlyElement(rowExpressions)).getValue() != null;
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
