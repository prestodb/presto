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
import com.facebook.presto.spi.block.DictionaryId;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.DeterminismEvaluator;
import com.facebook.presto.sql.relational.Expressions;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.LambdaDefinitionExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.facebook.presto.sql.relational.Signatures;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Primitives;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.IntStream;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.add;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantBoolean;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantNull;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantTrue;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.equal;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.lessThan;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newArray;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newInstance;
import static com.facebook.presto.bytecode.instruction.JumpInstruction.jump;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.gen.BytecodeUtils.generateWrite;
import static com.facebook.presto.sql.gen.BytecodeUtils.loadConstant;
import static com.facebook.presto.sql.gen.LambdaAndTryExpressionExtractor.extractLambdaAndTryExpressions;
import static com.facebook.presto.sql.gen.TryCodeGenerator.defineTryMethod;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class PageProcessorCompiler
        implements BodyCompiler<PageProcessor>
{
    private static final RowExpression TRUE_PREDICATE = constant(true, BOOLEAN);

    private final Metadata metadata;
    private final DeterminismEvaluator determinismEvaluator;

    public PageProcessorCompiler(Metadata metadata)
    {
        this.metadata = metadata;
        this.determinismEvaluator = new DeterminismEvaluator(metadata.getFunctionRegistry());
    }

    @Override
    public void generateMethods(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, RowExpression filter, List<RowExpression> projections)
    {
        boolean hasFilter = !filter.equals(TRUE_PREDICATE);

        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);
        ImmutableList.Builder<MethodDefinition> projectMethods = ImmutableList.builder();
        ImmutableList.Builder<MethodDefinition> projectColumnarMethods = ImmutableList.builder();
        ImmutableList.Builder<MethodDefinition> projectDictionaryMethods = ImmutableList.builder();
        for (int i = 0; i < projections.size(); i++) {
            MethodDefinition project = generateProjectMethod(classDefinition, callSiteBinder, cachedInstanceBinder, "project_" + i, projections.get(i));
            MethodDefinition projectColumnar = generateProjectColumnarMethod(classDefinition, callSiteBinder, "projectColumnar_" + i, projections.get(i), project, hasFilter);
            // TODO: optimize projectRLE and projectDictionary when there is no filter condition
            MethodDefinition projectColumnarWithSelectedPositions = generateProjectColumnarMethod(classDefinition, callSiteBinder, "projectColumnarWithSelectedPositions_" + i, projections.get(i), project, true);
            MethodDefinition projectRLE = generateProjectRLEMethod(classDefinition, "projectRLE_" + i, projections.get(i), project, projectColumnarWithSelectedPositions);
            MethodDefinition projectDictionary = generateProjectDictionaryMethod(classDefinition, "projectDictionary_" + i, projections.get(i), project, projectColumnarWithSelectedPositions, projectRLE);

            projectMethods.add(project);
            projectColumnarMethods.add(projectColumnar);
            projectDictionaryMethods.add(projectDictionary);
        }

        List<MethodDefinition> projectMethodDefinitions = projectMethods.build();
        List<MethodDefinition> projectColumnarMethodDefinitions = projectColumnarMethods.build();
        List<MethodDefinition> projectDictionaryMethodDefinitions = projectDictionaryMethods.build();

        generateProcessMethod(classDefinition, filter, projections, projectMethodDefinitions);
        generateGetNonLazyPageMethod(classDefinition, filter, projections);
        generateProcessColumnarMethod(classDefinition, projections, projectColumnarMethodDefinitions, hasFilter);
        generateProcessColumnarDictionaryMethod(classDefinition, projections, projectDictionaryMethodDefinitions);

        generateFilterPageMethod(classDefinition, filter);
        generateFilterMethod(classDefinition, callSiteBinder, cachedInstanceBinder, filter);
        generateIsFilteringMethod(classDefinition, hasFilter);
        generateConstructor(classDefinition, cachedInstanceBinder, projections.size());
    }

    private static void generateConstructor(ClassDefinition classDefinition, CachedInstanceBinder cachedInstanceBinder, int projectionCount)
    {
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC));
        FieldDefinition inputDictionaries = classDefinition.declareField(a(PRIVATE, FINAL), "inputDictionaries", Block[].class);
        FieldDefinition outputDictionaries = classDefinition.declareField(a(PRIVATE, FINAL), "outputDictionaries", Block[].class);

        FieldDefinition inputFilterDictionary = classDefinition.declareField(a(PRIVATE), "inputFilterDictionary", Block.class);
        FieldDefinition filterResult = classDefinition.declareField(a(PRIVATE), "filterResult", boolean[].class);

        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        body.append(thisVariable.setField(inputDictionaries, newArray(type(Block[].class), projectionCount)));
        body.append(thisVariable.setField(outputDictionaries, newArray(type(Block[].class), projectionCount)));

        body.append(thisVariable.setField(inputFilterDictionary, constantNull(Block.class)));
        body.append(thisVariable.setField(filterResult, constantNull(boolean[].class)));

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
            List<MethodDefinition> projectColumnarMethods,
            boolean hasFilter)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter page = arg("page", Page.class);
        Parameter types = arg("types", List.class);
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "processColumnar", type(Page.class), session, page, types);

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        Optional<Variable> selectedPositions;
        Variable cardinality;
        if (hasFilter) {
            selectedPositions = Optional.of(scope.declareVariable("selectedPositions", body, thisVariable.invoke("filterPage", int[].class, session, page)));
            cardinality = scope.declareVariable("cardinality", body, selectedPositions.get().length());
        }
        else {
            selectedPositions = Optional.empty();
            cardinality = scope.declareVariable("cardinality", body, page.invoke("getPositionCount", int.class));
        }

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
            ImmutableList.Builder<BytecodeExpression> paramsBuilder = ImmutableList.<BytecodeExpression>builder()
                    .add(session)
                    .add(page)
                    .add(pageBuilder)
                    .add(constantInt(projectionIndex));
            if (hasFilter) {
                paramsBuilder.add(selectedPositions.get());
            }

            body.append(outputBlocks.setElement(projectionIndex, thisVariable.invoke(projectColumnarMethods.get(projectionIndex), paramsBuilder.build())));
        }

        // create new page from outputBlocks
        body.append(newInstance(Page.class, cardinality, outputBlocks).ret());
    }

    private static MethodDefinition generateProjectColumnarMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            String methodName,
            RowExpression projection,
            MethodDefinition projectionMethod,
            boolean hasFilter)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter page = arg("page", Page.class);
        Parameter pageBuilder = arg("pageBuilder", PageBuilder.class);
        Parameter projectionIndex = arg("projectionIndex", int.class);
        Parameter selectedPositions = arg("selectedPositions", int[].class);

        ImmutableList.Builder<Parameter> paramsBuilder = ImmutableList.<Parameter>builder()
                .add(session)
                .add(page)
                .add(pageBuilder)
                .add(projectionIndex);

        if (hasFilter) {
            paramsBuilder.add(selectedPositions);
        }

        List<Parameter> params = paramsBuilder.build();
        MethodDefinition method = classDefinition.declareMethod(a(PRIVATE), methodName, type(Block.class), params);

        BytecodeBlock body = method.getBody();
        Scope scope = method.getScope();
        Variable thisVariable = method.getThis();

        ImmutableList.Builder<Variable> builder = ImmutableList.builder();
        for (int channel : getInputChannels(projection)) {
            Variable blockVariable = scope.declareVariable("block_" + channel, body, page.invoke("getBlock", Block.class, constantInt(channel)));
            builder.add(blockVariable);
        }
        List<Variable> inputs = builder.build();

        Variable positionCount = scope.declareVariable("positionCount", body, page.invoke("getPositionCount", int.class));
        Variable loopCounter = scope.declareVariable("loopCounter", body, constantInt(0));
        Variable outputBlock = scope.declareVariable(Block.class, "outputBlock");
        Variable blockBuilder = scope.declareVariable("blockBuilder", body, pageBuilder.invoke("getBlockBuilder", BlockBuilder.class, projectionIndex));
        Variable type = scope.declareVariable("type", body, pageBuilder.invoke("getType", Type.class, projectionIndex));

        Variable cardinality;
        BytecodeExpression position;
        if (hasFilter) {
            cardinality = scope.declareVariable("cardinality", body, selectedPositions.length());
            position = selectedPositions.getElement(loopCounter);
        }
        else {
            cardinality = positionCount;
            position = loopCounter;
        }

        BytecodeBlock projectBlock = new BytecodeBlock()
                .append(new ForLoop()
                        .initialize(loopCounter.set(constantInt(0)))
                        .condition(lessThan(loopCounter, cardinality))
                        .update(loopCounter.increment())
                        .body(invokeProject(
                                thisVariable,
                                session,
                                inputs,
                                position,
                                pageBuilder,
                                projectionIndex,
                                projectionMethod)))
                .append(outputBlock.set(blockBuilder.invoke("build", Block.class)));

        if (isIdentityExpression(projection)) {
            // if nothing is filtered out, copy the entire block, else project it
            body.append(new IfStatement()
                    .condition(equal(cardinality, positionCount))
                    .ifTrue(new BytecodeBlock()
                            .append(inputs.get(0).invoke("assureLoaded", void.class))
                            .append(outputBlock.set(inputs.get(0))))
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

    private MethodDefinition generateProjectRLEMethod(
            ClassDefinition classDefinition,
            String methodName,
            RowExpression projection,
            MethodDefinition projectionMethod,
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
                .add(pageBuilder)
                .add(projectionIndex)
                .add(selectedPositions)
                .build();
        MethodDefinition method = classDefinition.declareMethod(a(PRIVATE), methodName, type(Block.class), params);

        BytecodeBlock body = method.getBody();
        Scope scope = method.getScope();
        Variable thisVariable = method.getThis();
        List<Integer> inputChannels = getInputChannels(projection);

        if (inputChannels.size() != 1 || !determinismEvaluator.isDeterministic(projection)) {
            body.append(thisVariable.invoke(projectColumnar, params)
                    .ret());
            return method;
        }

        Variable inputBlock = scope.declareVariable("inputBlock", body, page.invoke("getBlock", Block.class, constantInt(getOnlyElement(inputChannels))));
        body.append(new IfStatement()
                .condition(inputBlock.instanceOf(RunLengthEncodedBlock.class))
                .ifFalse(thisVariable.invoke(projectColumnar, params)
                        .ret()));

        Variable valueBlock = scope.declareVariable("valueBlock", body, inputBlock.cast(RunLengthEncodedBlock.class).invoke("getValue", Block.class));
        Variable blockBuilder = scope.declareVariable("blockBuilder", body, pageBuilder.invoke("getBlockBuilder", BlockBuilder.class, projectionIndex));

        body.append(invokeProject(
                thisVariable,
                session,
                singletonList(valueBlock),
                constantInt(0),
                pageBuilder,
                projectionIndex,
                projectionMethod));
        Variable outputValueBlock = scope.declareVariable("outputValueBlock", body, blockBuilder.invoke("build", Block.class));
        body.append(newInstance(RunLengthEncodedBlock.class, outputValueBlock, selectedPositions.length())
                .ret());

        return method;
    }

    private MethodDefinition generateProjectDictionaryMethod(
            ClassDefinition classDefinition,
            String methodName,
            RowExpression projection,
            MethodDefinition project,
            MethodDefinition projectColumnar,
            MethodDefinition projectRLE)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter page = arg("page", Page.class);
        Parameter selectedPositions = arg("selectedPositions", int[].class);
        Parameter pageBuilder = arg("pageBuilder", PageBuilder.class);
        Parameter projectionIndex = arg("projectionIndex", int.class);
        Parameter dictionarySourceIds = arg("dictionarySourceIds", Map.class);

        List<Parameter> params = ImmutableList.<Parameter>builder()
                .add(session)
                .add(page)
                .add(selectedPositions)
                .add(pageBuilder)
                .add(projectionIndex)
                .add(dictionarySourceIds)
                .build();

        List<Parameter> columnarParams = ImmutableList.<Parameter>builder()
                .add(session)
                .add(page)
                .add(pageBuilder)
                .add(projectionIndex)
                .add(selectedPositions)
                .build();

        MethodDefinition method = classDefinition.declareMethod(a(PRIVATE), methodName, type(Block.class), params);
        BytecodeBlock body = method.getBody();
        Scope scope = method.getScope();
        Variable thisVariable = method.getThis();

        List<Integer> inputChannels = getInputChannels(projection);

        if (inputChannels.size() != 1 || !determinismEvaluator.isDeterministic(projection)) {
            body.append(thisVariable.invoke(projectColumnar, columnarParams)
                    .ret());
            return method;
        }

        Variable inputBlock = scope.declareVariable("inputBlock", body, page.invoke("getBlock", Block.class, constantInt(getOnlyElement(inputChannels))));

        body.append(new IfStatement()
                .condition(inputBlock.instanceOf(RunLengthEncodedBlock.class))
                .ifTrue(thisVariable.invoke(projectRLE, columnarParams)
                        .ret()));

        body.append(new IfStatement()
                .condition(inputBlock.instanceOf(DictionaryBlock.class))
                .ifFalse(thisVariable.invoke(projectColumnar, columnarParams)
                        .ret()));

        Variable blockBuilder = scope.declareVariable("blockBuilder", body, pageBuilder.invoke("getBlockBuilder", BlockBuilder.class, projectionIndex));
        Variable cardinality = scope.declareVariable("cardinality", body, selectedPositions.length());

        Variable dictionary = scope.declareVariable(Block.class, "dictionary");
        Variable dictionaryCount = scope.declareVariable(int.class, "dictionaryCount");
        Variable inputSourceId = scope.declareVariable(DictionaryId.class, "inputSourceId");
        Variable outputSourceId = scope.declareVariable(DictionaryId.class, "outputSourceId");

        Variable outputDictionary = scope.declareVariable(Block.class, "outputDictionary");
        Variable outputIds = scope.declareVariable(int[].class, "outputIds");

        BytecodeExpression inputDictionaries = thisVariable.getField("inputDictionaries", Block[].class);
        BytecodeExpression outputDictionaries = thisVariable.getField("outputDictionaries", Block[].class);

        Variable position = scope.declareVariable("position", body, constantInt(0));

        BytecodeExpression castDictionaryBlock = inputBlock.cast(DictionaryBlock.class);
        body.comment("Extract dictionary, ids, positionCount and dictionarySourceId")
                .append(dictionary.set(castDictionaryBlock.invoke("getDictionary", Block.class)))
                .append(dictionaryCount.set(dictionary.invoke("getPositionCount", int.class)))
                .append(inputSourceId.set(castDictionaryBlock.invoke("getDictionarySourceId", DictionaryId.class)));

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
                        .body(outputIds.setElement(position, castDictionaryBlock.invoke("getId", int.class, selectedPositions.getElement(position)))));

        body.append(outputSourceId.set(dictionarySourceIds.invoke("get", Object.class, inputSourceId.cast(Object.class)).cast(DictionaryId.class)));
        body.append(new IfStatement()
                .condition(equal(outputSourceId, constantNull(DictionaryId.class)))
                .ifTrue(new BytecodeBlock()
                        .append(outputSourceId.set(invokeStatic(DictionaryId.class, "randomDictionaryId", DictionaryId.class)))
                        .append(dictionarySourceIds.invoke("put", Object.class, inputSourceId.cast(Object.class), outputSourceId.cast(Object.class)))
                        .pop()));

        body.append(newInstance(DictionaryBlock.class, cardinality, outputDictionary, outputIds, constantFalse(), outputSourceId)
                .cast(Block.class)
                .ret());
        return method;
    }

    private static void generateProcessColumnarDictionaryMethod(
            ClassDefinition classDefinition,
            List<RowExpression> projections,
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
        Variable dictionarySourceIds = scope.declareVariable(type(Map.class, DictionaryId.class, DictionaryId.class), "dictionarySourceIds");
        body.append(dictionarySourceIds.set(newInstance(type(HashMap.class, DictionaryId.class, DictionaryId.class))));

        body.comment("if no rows selected return null")
                .append(new IfStatement()
                        .condition(equal(cardinality, constantInt(0)))
                        .ifTrue(constantNull(Page.class).ret()));

        if (projections.isEmpty()) {
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
                    .add(dictionarySourceIds)
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

    private void generateFilterPageMethod(ClassDefinition classDefinition, RowExpression filter)
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
        Variable selectedCount = scope.declareVariable("selectedCount", body, constantInt(0));
        Variable position = scope.declareVariable(int.class, "position");

        List<Integer> filterChannels = getInputChannels(filter);

        // extract block variables
        ImmutableList.Builder<Variable> blockVariablesBuilder = ImmutableList.builder();
        for (int channel : filterChannels) {
            Variable blockVariable = scope.declareVariable("block_" + channel, body, page.invoke("getBlock", Block.class, constantInt(channel)));
            blockVariablesBuilder.add(blockVariable);
        }
        List<Variable> blockVariables = blockVariablesBuilder.build();
        if (filterChannels.size() == 1 && determinismEvaluator.isDeterministic(filter)) {
            BytecodeBlock ifFilterOnDictionaryBlock = getBytecodeFilterOnDictionary(session, scope, blockVariables.get(0));
            BytecodeBlock ifFilterOnRLEBlock = getBytecodeFilterOnRLE(session, scope, blockVariables.get(0));

            body.append(new IfStatement()
                    .condition(blockVariables.get(0).instanceOf(DictionaryBlock.class))
                    .ifTrue(ifFilterOnDictionaryBlock));

            body.append(new IfStatement()
                    .condition(blockVariables.get(0).instanceOf(RunLengthEncodedBlock.class))
                    .ifTrue(ifFilterOnRLEBlock));
        }

        body.append(new ForLoop()
                .initialize(position.set(constantInt(0)))
                .condition(lessThan(position, positionCount))
                .update(position.increment())
                .body(new IfStatement()
                        .condition(invokeFilter(thisVariable, session, blockVariables, position))
                        .ifTrue(new BytecodeBlock()
                                .append(selectedPositions.setElement(selectedCount, position))
                                .append(selectedCount.increment()))));

        body.append(invokeStatic(Arrays.class, "copyOf", int[].class, selectedPositions, selectedCount)
                .ret());
    }

    private static BytecodeBlock getBytecodeFilterOnRLE(Parameter session, Scope scope, Variable blockVariable)
    {
        Variable positionCount = scope.getVariable("positionCount");
        Variable thisVariable = scope.getThis();

        BytecodeBlock ifFilterOnRLEBlock = new BytecodeBlock();
        Variable rleBlock = scope.declareVariable("rleBlock", ifFilterOnRLEBlock, blockVariable.cast(RunLengthEncodedBlock.class));
        Variable rleValue = scope.declareVariable("rleValue", ifFilterOnRLEBlock, rleBlock.invoke("getValue", Block.class));

        ifFilterOnRLEBlock.append(new IfStatement()
                .condition(invokeFilter(thisVariable, session, singletonList(rleValue), constantInt(0)))
                .ifTrue(invokeStatic(IntStream.class, "range", IntStream.class, constantInt(0), positionCount)
                        .invoke("toArray", int[].class)
                        .ret())
                .ifFalse(newArray(type(int[].class), constantInt(0))
                        .ret()));

        return ifFilterOnRLEBlock;
    }

    private static BytecodeBlock getBytecodeFilterOnDictionary(
            Parameter session,
            Scope scope,
            Variable blockVariable)
    {
        Variable position = scope.getVariable("position");
        Variable positionCount = scope.getVariable("positionCount");
        Variable selectedCount = scope.getVariable("selectedCount");
        Variable selectedPositions = scope.getVariable("selectedPositions");
        Variable thisVariable = scope.getThis();

        BytecodeExpression inputFilterDictionary = thisVariable.getField("inputFilterDictionary", Block.class);
        BytecodeExpression filterResult = thisVariable.getField("filterResult", boolean[].class);

        BytecodeBlock ifFilterOnDictionaryBlock = new BytecodeBlock();

        Variable dictionaryBlock = scope.declareVariable("dictionaryBlock", ifFilterOnDictionaryBlock, blockVariable.cast(DictionaryBlock.class));
        Variable dictionary = scope.declareVariable("dictionary", ifFilterOnDictionaryBlock, dictionaryBlock.invoke("getDictionary", Block.class));
        Variable dictionaryPositionCount = scope.declareVariable("dictionaryPositionCount", ifFilterOnDictionaryBlock, dictionary.invoke("getPositionCount", int.class));
        Variable selectedDictionaryPositions = scope.declareVariable("selectedDictionaryPositions", ifFilterOnDictionaryBlock, newArray(type(boolean[].class), dictionaryPositionCount));

        // if cached value is available use it else filter dictionary and cache it
        ifFilterOnDictionaryBlock.append(new IfStatement()
                .condition(equal(dictionary, inputFilterDictionary))
                .ifTrue(selectedDictionaryPositions.set(filterResult))
                .ifFalse(new BytecodeBlock()
                        .append(new ForLoop()
                                .initialize(position.set(constantInt(0)))
                                .condition(lessThan(position, dictionaryPositionCount))
                                .update(position.increment())
                                .body(selectedDictionaryPositions.setElement(position, invokeFilter(thisVariable, session, singletonList(dictionary), position))))
                        .append(thisVariable.setField("inputFilterDictionary", dictionary))
                        .append(thisVariable.setField("filterResult", selectedDictionaryPositions))));

        // create selected positions
        ifFilterOnDictionaryBlock.append(new ForLoop()
                .initialize(position.set(constantInt(0)))
                .condition(lessThan(position, positionCount))
                .update(position.increment())
                .body(new IfStatement()
                        .condition(selectedDictionaryPositions.getElement(dictionaryBlock.invoke("getId", int.class, position)))
                        .ifTrue(new BytecodeBlock()
                                .append(selectedPositions.setElement(selectedCount, position))
                                .append(selectedCount.increment()))));

        // return selectedPositions
        ifFilterOnDictionaryBlock.append(invokeStatic(Arrays.class, "copyOf", int[].class, selectedPositions, selectedCount)
                .ret());
        return ifFilterOnDictionaryBlock;
    }

    private PreGeneratedExpressions generateMethodsForLambdaAndTry(
            ClassDefinition containerClassDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            RowExpression projection,
            String methodPrefix)
    {
        Set<RowExpression> lambdaAndTryExpressions = ImmutableSet.copyOf(extractLambdaAndTryExpressions(projection));
        ImmutableMap.Builder<CallExpression, MethodDefinition> tryMethodMap = ImmutableMap.builder();
        ImmutableMap.Builder<LambdaDefinitionExpression, FieldDefinition> lambdaFieldMap = ImmutableMap.builder();

        int counter = 0;
        for (RowExpression expression : lambdaAndTryExpressions) {
            if (expression instanceof CallExpression) {
                CallExpression tryExpression = (CallExpression) expression;
                verify(!Signatures.TRY.equals(tryExpression.getSignature().getName()));

                Parameter session = arg("session", ConnectorSession.class);
                List<Parameter> blocks = toBlockParameters(getInputChannels(tryExpression.getArguments()));
                Parameter position = arg("position", int.class);

                BytecodeExpressionVisitor innerExpressionVisitor = new BytecodeExpressionVisitor(
                        callSiteBinder,
                        cachedInstanceBinder,
                        fieldReferenceCompiler(callSiteBinder),
                        metadata.getFunctionRegistry(),
                        new PreGeneratedExpressions(tryMethodMap.build(), lambdaFieldMap.build()));

                List<Parameter> inputParameters = ImmutableList.<Parameter>builder()
                        .add(session)
                        .addAll(blocks)
                        .add(position)
                        .build();

                MethodDefinition tryMethod = defineTryMethod(
                        innerExpressionVisitor,
                        containerClassDefinition,
                        methodPrefix + "_try_" + counter,
                        inputParameters,
                        Primitives.wrap(tryExpression.getType().getJavaType()),
                        tryExpression,
                        callSiteBinder);

                tryMethodMap.put(tryExpression, tryMethod);
            }
            else if (expression instanceof LambdaDefinitionExpression) {
                LambdaDefinitionExpression lambdaExpression = (LambdaDefinitionExpression) expression;
                PreGeneratedExpressions preGeneratedExpressions = new PreGeneratedExpressions(tryMethodMap.build(), lambdaFieldMap.build());
                FieldDefinition methodHandleField = LambdaBytecodeGenerator.preGenerateLambdaExpression(
                        lambdaExpression,
                        methodPrefix + "_lambda_" + counter,
                        containerClassDefinition,
                        preGeneratedExpressions,
                        callSiteBinder,
                        cachedInstanceBinder,
                        metadata.getFunctionRegistry());
                lambdaFieldMap.put(lambdaExpression, methodHandleField);
            }
            else {
                throw new VerifyException(format("unexpected expression: %s", expression.toString()));
            }
            counter++;
        }

        return new PreGeneratedExpressions(tryMethodMap.build(), lambdaFieldMap.build());
    }

    private void generateFilterMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, CachedInstanceBinder cachedInstanceBinder, RowExpression filter)
    {
        PreGeneratedExpressions preGeneratedExpressions = generateMethodsForLambdaAndTry(classDefinition, callSiteBinder, cachedInstanceBinder, filter, "filter");

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
        BytecodeBlock body = method.getBody();

        Scope scope = method.getScope();
        Variable wasNullVariable = scope.declareVariable("wasNull", body, constantFalse());

        BytecodeExpressionVisitor visitor = new BytecodeExpressionVisitor(
                callSiteBinder,
                cachedInstanceBinder,
                fieldReferenceCompiler(callSiteBinder),
                metadata.getFunctionRegistry(),
                preGeneratedExpressions);

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
        PreGeneratedExpressions preGeneratedExpressions = generateMethodsForLambdaAndTry(classDefinition, callSiteBinder, cachedInstanceBinder, projection, methodName);

        Parameter session = arg("session", ConnectorSession.class);
        List<Parameter> blocks = toBlockParameters(getInputChannels(projection));
        Parameter position = arg("position", int.class);
        Parameter output = arg("output", BlockBuilder.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                methodName,
                type(void.class),
                ImmutableList.<Parameter>builder()
                        .add(session)
                        .addAll(blocks)
                        .add(position)
                        .add(output)
                        .build());

        method.comment("Projection: %s", projection.toString());

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        Variable wasNullVariable = scope.declareVariable("wasNull", body, constantFalse());
        BytecodeExpressionVisitor visitor = new BytecodeExpressionVisitor(
                callSiteBinder,
                cachedInstanceBinder,
                fieldReferenceCompiler(callSiteBinder),
                metadata.getFunctionRegistry(),
                preGeneratedExpressions);

        body.getVariable(output)
                .comment("evaluate projection: " + projection.toString())
                .append(projection.accept(visitor, scope))
                .append(generateWrite(callSiteBinder, scope, wasNullVariable, projection.getType()))
                .ret();
        return method;
    }

    private MethodDefinition generateIsFilteringMethod(ClassDefinition classDefinition, boolean hasFilter)
    {
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "isFiltering", type(boolean.class));
        method.getBody().append(constantBoolean(hasFilter).ret());
        return method;
    }

    private static boolean isIdentityExpression(RowExpression expression)
    {
        List<RowExpression> rowExpressions = Expressions.subExpressions(ImmutableList.of(expression));
        return rowExpressions.size() == 1 && getOnlyElement(rowExpressions) instanceof InputReferenceExpression;
    }

    private static boolean isConstantExpression(RowExpression expression)
    {
        List<RowExpression> rowExpressions = Expressions.subExpressions(ImmutableList.of(expression));
        return rowExpressions.size() == 1 &&
                getOnlyElement(rowExpressions) instanceof ConstantExpression &&
                ((ConstantExpression) getOnlyElement(rowExpressions)).getValue() != null;
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

    private static RowExpressionVisitor<Scope, BytecodeNode> fieldReferenceCompiler(CallSiteBinder callSiteBinder)
    {
        return new InputReferenceCompiler(
                (scope, field) -> scope.getVariable("block_" + field),
                (scope, field) -> scope.getVariable("position"),
                callSiteBinder);
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
