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

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.ForLoop;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.bytecode.instruction.LabelNode;
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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.add;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantNull;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantTrue;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.equal;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.lessThan;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.multiply;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newArray;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newInstance;
import static com.facebook.presto.bytecode.instruction.JumpInstruction.jump;
import static com.facebook.presto.sql.gen.BytecodeUtils.generateWrite;
import static com.facebook.presto.sql.gen.BytecodeUtils.loadConstant;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.concat;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
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
        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);
        ImmutableList.Builder<MethodDefinition> projectMethods = ImmutableList.builder();
        ImmutableList.Builder<MethodDefinition> projectColumnarMethods = ImmutableList.builder();
        ImmutableList.Builder<MethodDefinition> projectDictionaryMethods = ImmutableList.builder();
        for (int i = 0; i < projections.size(); i++) {
            MethodDefinition project = generateProjectMethod(classDefinition, callSiteBinder, cachedInstanceBinder, "project_" + i, projections.get(i));
            MethodDefinition projectColumnar = generateProjectColumnarMethod(classDefinition, callSiteBinder, "projectColumnar_" + i, projections.get(i), project);
            MethodDefinition projectDictionary = generateProjectDictionaryMethod(classDefinition, "projectDictionary_" + i, projections.get(i), project, projectColumnar);

            projectMethods.add(project);
            projectColumnarMethods.add(projectColumnar);
            projectDictionaryMethods.add(projectDictionary);
        }

        List<MethodDefinition> projectMethodDefinitions = projectMethods.build();
        List<MethodDefinition> projectColumnarMethodDefinitions = projectColumnarMethods.build();
        List<MethodDefinition> projectDictionaryMethodDefinitions = projectDictionaryMethods.build();

        generateProcessMethod(classDefinition, filter, projections, projectMethodDefinitions);
        generateGetNonLazyPageMethod(classDefinition, filter, projections);
        generateProcessColumnarMethod(classDefinition, projections, projectColumnarMethodDefinitions);
        generateProcessColumnarDictionaryMethod(classDefinition, projections, projectColumnarMethodDefinitions, projectDictionaryMethodDefinitions);

        generateFilterPageMethod(classDefinition, filter);
        generateFilterMethod(classDefinition, callSiteBinder, cachedInstanceBinder, filter);
        generateConstructor(classDefinition, cachedInstanceBinder, projections.size());
    }

    private static void generateConstructor(ClassDefinition classDefinition, CachedInstanceBinder cachedInstanceBinder, int projectionCount)
    {
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC));
        FieldDefinition inputDictionaries = classDefinition.declareField(a(PRIVATE, FINAL), "inputDictionaries", Block[].class);
        FieldDefinition outputDictionaries = classDefinition.declareField(a(PRIVATE, FINAL), "outputDictionaries", Block[].class);

        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        body.append(thisVariable.setField(inputDictionaries, newArray(type(Block[].class), projectionCount)));
        body.append(thisVariable.setField(outputDictionaries, newArray(type(Block[].class), projectionCount)));
        cachedInstanceBinder.generateInitializations(thisVariable, body);
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
        BytecodeBlock body = method.getBody();
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

        // projection body
        Variable position = scope.declareVariable(int.class, "position");

        BytecodeBlock project = new BytecodeBlock()
                .append(pageBuilder.invoke("declarePosition", void.class));

        for (int projectionIndex = 0; projectionIndex < projections.size(); projectionIndex++) {
            RowExpression projection = projections.get(projectionIndex);
            project.append(invokeProject(thisVariable, session, expressionInputBlocks.get(projection), position, pageBuilder, constantInt(projectionIndex), projectionMethods.get(projectionIndex)));
        }
        LabelNode done = new LabelNode("done");

        // for loop loop body
        ForLoop loop = new ForLoop()
                .initialize(position.set(start))
                .condition(lessThan(position, end))
                .update(position.set(add(position, constantInt(1))))
                .body(new BytecodeBlock()
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
            List<RowExpression> projections,
            List<MethodDefinition> projectColumnarMethods)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter page = arg("page", Page.class);
        Parameter types = arg("types", List.class);
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "processColumnar", type(Page.class), session, page, types);

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        Variable selectedPositions = scope.declareVariable("selectedPositions", body, thisVariable.invoke("filterPage", int[].class, session, page));
        Variable cardinality = scope.declareVariable("cardinality", body, selectedPositions.length());

        body.comment("if no rows selected return null")
                .append(new IfStatement()
                        .condition(equal(cardinality, constantInt(0)))
                        .ifTrue(constantNull(Page.class).ret()));

        if (projections.isEmpty()) {
            // if no projections, return new page with selected rows
            body.append(newInstance(Page.class, cardinality, newArray(type(Block[].class), 0)).ret());
            return;
        }

        Variable pageBuilder = scope.declareVariable("pageBuilder", body, newInstance(PageBuilder.class, cardinality, types));
        Variable outputBlocks = scope.declareVariable("outputBlocks", body, newArray(type(Block[].class), projections.size()));

        for (int projectionIndex = 0; projectionIndex < projections.size(); projectionIndex++) {
            List<BytecodeExpression> params = ImmutableList.<BytecodeExpression>builder()
                    .add(session)
                    .add(page)
                    .add(selectedPositions)
                    .add(pageBuilder)
                    .add(constantInt(projectionIndex))
                    .build();
            body.append(outputBlocks.setElement(projectionIndex, thisVariable.invoke(projectColumnarMethods.get(projectionIndex), params)));
        }

        // create new page from outputBlocks
        body.append(newInstance(Page.class, cardinality, outputBlocks).ret());
    }

    private static MethodDefinition generateProjectColumnarMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            String methodName,
            RowExpression projection,
            MethodDefinition projectionMethod)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter page = arg("page", Page.class);
        Parameter selectedPositions = arg("selectedPositions", int[].class);
        Parameter pageBuilder = arg("pageBuilder", PageBuilder.class);
        Parameter projectionIndex = arg("projectionIndex", int.class);

        List<Parameter> params = ImmutableList.<Parameter>builder()
                .add(session)
                .add(page)
                .add(selectedPositions)
                .add(pageBuilder)
                .add(projectionIndex)
                .build();
        MethodDefinition method = classDefinition.declareMethod(a(PRIVATE), methodName, type(Block.class), params);

        BytecodeBlock body = method.getBody();
        Scope scope = method.getScope();
        Variable thisVariable = method.getThis();

        ImmutableList.Builder<Variable> builder = ImmutableList.<Variable>builder();
        for (int channel : getInputChannels(projection)) {
            Variable blockVariable = scope.declareVariable("block_" + channel, body, page.invoke("getBlock", Block.class, constantInt(channel)));
            builder.add(blockVariable);
        }
        List<Variable> inputs = builder.build();

        Variable positionCount = scope.declareVariable("positionCount", body, page.invoke("getPositionCount", int.class));
        Variable position = scope.declareVariable("position", body, constantInt(0));
        Variable cardinality = scope.declareVariable("cardinality", body, selectedPositions.length());
        Variable outputBlock = scope.declareVariable(Block.class, "outputBlock");
        Variable blockBuilder = scope.declareVariable("blockBuilder", body, pageBuilder.invoke("getBlockBuilder", BlockBuilder.class, projectionIndex));
        Variable type = scope.declareVariable("type", body, pageBuilder.invoke("getType", Type.class, projectionIndex));

        BytecodeBlock projectBlock = new BytecodeBlock()
                .append(new ForLoop()
                        .initialize(position.set(constantInt(0)))
                        .condition(lessThan(position, cardinality))
                        .update(position.increment())
                        .body(invokeProject(
                                thisVariable,
                                session,
                                inputs,
                                selectedPositions.getElement(position),
                                pageBuilder,
                                projectionIndex,
                                projectionMethod)))
                .append(outputBlock.set(blockBuilder.invoke("build", Block.class)));

        if (isIdentityExpression(projection)) {
            // if nothing is filtered out, copy the entire block, else project it
            body.append(new IfStatement()
                    .condition(equal(cardinality, positionCount))
                    .ifTrue(outputBlock.set(inputs.get(0)))
                    .ifFalse(projectBlock));
        }
        else if (isConstantExpression(projection)) {
            // if projection is a constant, create RLE block of constant expression with cardinality positions
            ConstantExpression constantExpression = (ConstantExpression) projection;
            verify(getInputChannels(projection).isEmpty());
            BytecodeExpression value = loadConstant(callSiteBinder, constantExpression.getValue(), Object.class);
            body.append(outputBlock.set(invokeStatic(RunLengthEncodedBlock.class, "create", Block.class, type, value, cardinality)));
        }
        else {
            body.append(projectBlock);
        }
        body.append(outputBlock.ret());
        return method;
    }

    private static MethodDefinition generateProjectDictionaryMethod(
            ClassDefinition classDefinition,
            String methodName,
            RowExpression projection,
            MethodDefinition project,
            MethodDefinition projectColumnar)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter page = arg("page", Page.class);
        Parameter selectedPositions = arg("selectedPositions", int[].class);
        Parameter pageBuilder = arg("pageBuilder", PageBuilder.class);
        Parameter projectionIndex = arg("projectionIndex", int.class);

        List<Parameter> params = ImmutableList.<Parameter>builder()
                .add(session)
                .add(page)
                .add(selectedPositions)
                .add(pageBuilder)
                .add(projectionIndex)
                .build();

        MethodDefinition method = classDefinition.declareMethod(a(PRIVATE), methodName, type(Block.class), params);
        BytecodeBlock body = method.getBody();
        Scope scope = method.getScope();
        Variable thisVariable = method.getThis();

        List<Integer> inputChannels = getInputChannels(projection);

        if (inputChannels.size() != 1) {
            body.append(thisVariable.invoke(projectColumnar, params).ret());
            return method;
        }

        Variable inputBlock = scope.declareVariable("inputBlock", body, page.invoke("getBlock", Block.class, constantInt(Iterables.getOnlyElement(inputChannels))));
        IfStatement ifStatement = new IfStatement()
                .condition(inputBlock.instanceOf(DictionaryBlock.class))
                .ifFalse(thisVariable.invoke(projectColumnar, params).ret());
        body.append(ifStatement);

        Variable blockBuilder = scope.declareVariable("blockBuilder", body, pageBuilder.invoke("getBlockBuilder", BlockBuilder.class, projectionIndex));
        Variable cardinality = scope.declareVariable("cardinality", body, selectedPositions.length());

        Variable dictionary = scope.declareVariable(Block.class, "dictionary");
        Variable ids = scope.declareVariable(Slice.class, "ids");
        Variable dictionaryCount = scope.declareVariable(int.class, "dictionaryCount");

        Variable outputDictionary = scope.declareVariable(Block.class, "outputDictionary");
        Variable outputIds = scope.declareVariable(int[].class, "outputIds");

        BytecodeExpression inputDictionaries = thisVariable.getField("inputDictionaries", Block[].class);
        BytecodeExpression outputDictionaries = thisVariable.getField("outputDictionaries", Block[].class);

        Variable position = scope.declareVariable("position", body, constantInt(0));

        body.comment("Extract dictionary and ids")
                .append(dictionary.set(inputBlock.cast(DictionaryBlock.class).invoke("getDictionary", Block.class)))
                .append(ids.set(inputBlock.cast(DictionaryBlock.class).invoke("getIds", Slice.class)))
                .append(dictionaryCount.set(dictionary.invoke("getPositionCount", int.class)));

        BytecodeBlock projectDictionary = new BytecodeBlock()
                .comment("Project dictionary")
                .append(new ForLoop()
                        .initialize(position.set(constantInt(0)))
                        .condition(lessThan(position, dictionaryCount))
                        .update(position.increment())
                        .body(invokeProject(thisVariable, session, ImmutableList.of(dictionary), position, pageBuilder, projectionIndex, project)))
                .append(outputDictionary.set(blockBuilder.invoke("build", Block.class)))
                .append(inputDictionaries.setElement(projectionIndex, dictionary))
                .append(outputDictionaries.setElement(projectionIndex, outputDictionary));

        body.comment("Use processed dictionary, if available, else project it")
                .append(
                        new IfStatement()
                                .condition(equal(inputDictionaries.getElement(projectionIndex), dictionary))
                                .ifTrue(outputDictionary.set(outputDictionaries.getElement(projectionIndex)))
                                .ifFalse(projectDictionary));

        body.comment("Filter ids")
                .append(outputIds.set(newArray(type(int[].class), cardinality)))
                .append(new ForLoop()
                        .initialize(position.set(constantInt(0)))
                        .condition(lessThan(position, cardinality))
                        .update(position.increment())
                        .body(outputIds.setElement(position, ids.invoke("getInt", int.class, multiply(selectedPositions.getElement(position), constantInt(SIZE_OF_INT))))));

        body.append(newInstance(DictionaryBlock.class, cardinality, outputDictionary, invokeStatic(Slices.class, "wrappedIntArray", Slice.class, outputIds)).cast(Block.class).ret());
        return method;
    }

    private static void generateProcessColumnarDictionaryMethod(
            ClassDefinition classDefinition,
            List<RowExpression> projections,
            List<MethodDefinition> projectColumnarMethods,
            List<MethodDefinition> projectDictionaryMethods)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter page = arg("page", Page.class);
        Parameter types = arg("types", List.class);
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "processColumnarDictionary", type(Page.class), session, page, types);

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        Variable selectedPositions = scope.declareVariable("selectedPositions", body, thisVariable.invoke("filterPage", int[].class, session, page));
        Variable cardinality = scope.declareVariable("cardinality", body, selectedPositions.length());

        body.comment("if no rows selected return null")
                .append(new IfStatement()
                        .condition(equal(cardinality, constantInt(0)))
                        .ifTrue(constantNull(Page.class).ret()));

        if (projectColumnarMethods.isEmpty()) {
            // if no projections, return new page with selected rows
            body.append(newInstance(Page.class, cardinality, newArray(type(Block[].class), 0)).ret());
            return;
        }

        // create PageBuilder
        Variable pageBuilder = scope.declareVariable("pageBuilder", body, newInstance(PageBuilder.class, cardinality, types));

        body.append(page.set(thisVariable.invoke("getNonLazyPage", Page.class, page)));

        // create outputBlocks
        Variable outputBlocks = scope.declareVariable("outputBlocks", body, newArray(type(Block[].class), projections.size()));
        for (int projectionIndex = 0; projectionIndex < projections.size(); projectionIndex++) {
            List<BytecodeExpression> params = ImmutableList.<BytecodeExpression>builder()
                    .add(session)
                    .add(page)
                    .add(selectedPositions)
                    .add(pageBuilder)
                    .add(constantInt(projectionIndex))
                    .build();

            body.append(outputBlocks.setElement(projectionIndex, thisVariable.invoke(projectDictionaryMethods.get(projectionIndex), params)));
        }

        body.append(newInstance(Page.class, cardinality, outputBlocks).ret());
    }

    private static void generateGetNonLazyPageMethod(ClassDefinition classDefinition, RowExpression filter, List<RowExpression> projections)
    {
        Parameter page = arg("page", Page.class);
        MethodDefinition method = classDefinition.declareMethod(a(PRIVATE), "getNonLazyPage", type(Page.class), page);

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        List<Integer> allInputChannels = getInputChannels(concat(projections, ImmutableList.of(filter)));
        if (allInputChannels.isEmpty()) {
            body.append(page.ret());
            return;
        }

        Variable index = scope.declareVariable(int.class, "index");
        Variable channelCount = scope.declareVariable("channelCount", body, page.invoke("getChannelCount", int.class));
        Variable blocks = scope.declareVariable("blocks", body, newArray(type(Block[].class), channelCount));
        Variable inputBlock = scope.declareVariable(Block.class, "inputBlock");

        Variable positionCount = scope.declareVariable("positionCount", body, page.invoke("getPositionCount", int.class));
        Variable createNewPage = scope.declareVariable("createNewPage", body, constantFalse());

        ForLoop forLoop = new ForLoop()
                .initialize(index.set(constantInt(0)))
                .condition(lessThan(index, channelCount))
                .update(index.increment())
                .body(new BytecodeBlock()
                        .append(inputBlock.set(page.invoke("getBlock", Block.class, index)))
                        .append(new IfStatement()
                                .condition(inputBlock.instanceOf(LazyBlock.class))
                                .ifTrue(new BytecodeBlock()
                                        .append(blocks.setElement(index, inputBlock.cast(LazyBlock.class).invoke("getBlock", Block.class)))
                                        .append(createNewPage.set(constantTrue())))
                                .ifFalse(blocks.setElement(index, inputBlock))));

        body.append(forLoop);
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
        BytecodeBlock body = method.getBody();

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

    private void generateFilterMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, CachedInstanceBinder cachedInstanceBinder, RowExpression filter)
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
        BytecodeBlock body = method.getBody();

        Scope scope = method.getScope();
        Variable wasNullVariable = scope.declareVariable("wasNull", body, constantFalse());

        BytecodeExpressionVisitor visitor = new BytecodeExpressionVisitor(
                callSiteBinder,
                cachedInstanceBinder,
                fieldReferenceCompiler(callSiteBinder, position, wasNullVariable),
                metadata.getFunctionRegistry());
        BytecodeNode visitorBody = filter.accept(visitor, scope);

        Variable result = scope.declareVariable(boolean.class, "result");
        body.append(visitorBody)
                .putVariable(result)
                .append(new IfStatement()
                        .condition(wasNullVariable)
                        .ifTrue(constantFalse().ret())
                        .ifFalse(result.ret()));
    }

    private MethodDefinition generateProjectMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, CachedInstanceBinder cachedInstanceBinder, String methodName, RowExpression projection)
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
        BytecodeBlock body = method.getBody();

        Variable wasNullVariable = scope.declareVariable("wasNull", body, constantFalse());
        BytecodeExpressionVisitor visitor = new BytecodeExpressionVisitor(callSiteBinder, cachedInstanceBinder, fieldReferenceCompiler(callSiteBinder, position, wasNullVariable), metadata.getFunctionRegistry());

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

    private static RowExpressionVisitor<Scope, BytecodeNode> fieldReferenceCompiler(final CallSiteBinder callSiteBinder, final Variable positionVariable, final Variable wasNullVariable)
    {
        return new RowExpressionVisitor<Scope, BytecodeNode>()
        {
            @Override
            public BytecodeNode visitInputReference(InputReferenceExpression node, Scope scope)
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
            public BytecodeNode visitCall(CallExpression call, Scope scope)
            {
                throw new UnsupportedOperationException("not yet implemented");
            }

            @Override
            public BytecodeNode visitConstant(ConstantExpression literal, Scope scope)
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

    private static BytecodeExpression invokeFilter(BytecodeExpression objRef, BytecodeExpression session, List<? extends BytecodeExpression> blockVariables, BytecodeExpression position)
    {
        List<BytecodeExpression> params = ImmutableList.<BytecodeExpression>builder()
                .add(session)
                .addAll(blockVariables)
                .add(position)
                .build();

        return objRef.invoke("filter", boolean.class, params);
    }

    private static BytecodeNode invokeProject(
            Variable objRef,
            Variable session,
            List<? extends Variable> blockVariables,
            BytecodeExpression position,
            Variable pageBuilder,
            BytecodeExpression projectionIndex,
            MethodDefinition projectionMethod)
    {
        BytecodeExpression blockBuilder = pageBuilder.invoke("getBlockBuilder", BlockBuilder.class, projectionIndex);
        List<BytecodeExpression> params = ImmutableList.<BytecodeExpression>builder()
                .add(session)
                .addAll(blockVariables)
                .add(position)
                .add(blockBuilder)
                .build();
        return new BytecodeBlock().append(objRef.invoke(projectionMethod, params));
    }
}
