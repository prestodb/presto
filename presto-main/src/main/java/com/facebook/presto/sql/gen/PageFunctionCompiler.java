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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.project.ConstantPageProjection;
import com.facebook.presto.operator.project.InputChannels;
import com.facebook.presto.operator.project.InputPageProjection;
import com.facebook.presto.operator.project.PageFieldsToInputParametersRewriter;
import com.facebook.presto.operator.project.PageFilter;
import com.facebook.presto.operator.project.PageProjection;
import com.facebook.presto.operator.project.SelectedPositions;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.LambdaBytecodeGenerator.CompiledLambda;
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

import javax.inject.Inject;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.CompilerUtils.defineClass;
import static com.facebook.presto.bytecode.CompilerUtils.makeClassName;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.add;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.and;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantBoolean;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.lessThan;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newArray;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newInstance;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.not;
import static com.facebook.presto.operator.project.PageFieldsToInputParametersRewriter.rewritePageFieldsToInputParameters;
import static com.facebook.presto.spi.StandardErrorCode.COMPILER_ERROR;
import static com.facebook.presto.sql.gen.BytecodeUtils.generateWrite;
import static com.facebook.presto.sql.gen.BytecodeUtils.invoke;
import static com.facebook.presto.sql.gen.LambdaAndTryExpressionExtractor.extractLambdaAndTryExpressions;
import static com.facebook.presto.sql.gen.TryCodeGenerator.defineTryMethod;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PageFunctionCompiler
{
    private final Metadata metadata;
    private final DeterminismEvaluator determinismEvaluator;

    @Inject
    public PageFunctionCompiler(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.determinismEvaluator = new DeterminismEvaluator(metadata.getFunctionRegistry());
    }

    public Supplier<PageProjection> compileProjection(RowExpression projection)
    {
        requireNonNull(projection, "projection is null");

        if (projection instanceof InputReferenceExpression) {
            InputReferenceExpression input = (InputReferenceExpression) projection;
            InputPageProjection projectionFunction = new InputPageProjection(input.getField(), input.getType());
            return () -> projectionFunction;
        }

        if (projection instanceof ConstantExpression) {
            ConstantExpression constant = (ConstantExpression) projection;
            ConstantPageProjection projectionFunction = new ConstantPageProjection(constant.getValue(), constant.getType());
            return () -> projectionFunction;
        }

        PageFieldsToInputParametersRewriter.Result result = rewritePageFieldsToInputParameters(projection);

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        ClassDefinition classDefinition = defineProjectionClass(result.getRewrittenExpression(), result.getInputChannels(), callSiteBinder);

        Class<? extends PageProjection> projectionClass;
        try {
            projectionClass = defineClass(classDefinition, PageProjection.class, callSiteBinder.getBindings(), getClass().getClassLoader());
        }
        catch (Exception e) {
            throw new PrestoException(COMPILER_ERROR, e);
        }

        return () -> {
            try {
                return projectionClass.newInstance();
            }
            catch (ReflectiveOperationException e) {
                throw new PrestoException(COMPILER_ERROR, e);
            }
        };
    }

    private ClassDefinition defineProjectionClass(RowExpression projection, InputChannels inputChannels, CallSiteBinder callSiteBinder)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(PageProjection.class.getSimpleName()),
                type(Object.class),
                type(PageProjection.class));

        FieldDefinition blockBuilderField = classDefinition.declareField(a(PRIVATE), "blockBuilder", BlockBuilder.class);

        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        generatePageProjectMethod(classDefinition, blockBuilderField);

        PreGeneratedExpressions preGeneratedExpressions = generateMethodsForLambdaAndTry(classDefinition, callSiteBinder, cachedInstanceBinder, projection);
        generateProjectMethod(classDefinition, callSiteBinder, cachedInstanceBinder, preGeneratedExpressions, projection, blockBuilderField);

        // getType
        BytecodeExpression type = invoke(callSiteBinder.bind(projection.getType(), Type.class), "type");
        classDefinition.declareMethod(a(PUBLIC), "getType", type(Type.class))
                .getBody()
                .append(type.ret());

        // isDeterministic
        classDefinition.declareMethod(a(PUBLIC), "isDeterministic", type(boolean.class))
                .getBody()
                .append(constantBoolean(determinismEvaluator.isDeterministic(projection)).ret());

        // getInputChannels
        classDefinition.declareMethod(a(PUBLIC), "getInputChannels", type(InputChannels.class))
                .getBody()
                .append(invoke(callSiteBinder.bind(inputChannels, InputChannels.class), "getInputChannels").ret());

        // toString
        String toStringResult = toStringHelper(classDefinition.getType()
                .getJavaClassName())
                .add("projection", projection)
                .toString();
        classDefinition.declareMethod(a(PUBLIC), "toString", type(String.class))
                .getBody()
                // bind constant via invokedynamic to avoid constant pool issues due to large strings
                .append(invoke(callSiteBinder.bind(toStringResult, String.class), "toString").ret());

        // constructor
        generateConstructor(classDefinition, cachedInstanceBinder, preGeneratedExpressions, method -> {
            Variable thisVariable = method.getThis();
            BytecodeBlock body = method.getBody();
            body.append(thisVariable.setField(
                    blockBuilderField,
                    type.invoke("createBlockBuilder", BlockBuilder.class, newInstance(BlockBuilderStatus.class), constantInt(1))));
        });

        return classDefinition;
    }

    private static MethodDefinition generatePageProjectMethod(ClassDefinition classDefinition, FieldDefinition blockBuilder)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter page = arg("page", Page.class);
        Parameter selectedPositions = arg("selectedPositions", SelectedPositions.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "project",
                type(Block.class),
                ImmutableList.<Parameter>builder()
                        .add(session)
                        .add(page)
                        .add(selectedPositions)
                        .build());

        Scope scope = method.getScope();
        Variable thisVariable = method.getThis();
        BytecodeBlock body = method.getBody();

        Variable from = scope.declareVariable("from", body, selectedPositions.invoke("getOffset", int.class));
        Variable to = scope.declareVariable("to", body, add(from, selectedPositions.invoke("size", int.class)));
        Variable positions = scope.declareVariable(int[].class, "positions");
        Variable index = scope.declareVariable(int.class, "index");

        // reset block builder before using since a previous run may have thrown leaving data in the block builder
        body.append(thisVariable.setField(
                blockBuilder,
                thisVariable.getField(blockBuilder).invoke("newBlockBuilderLike", BlockBuilder.class, newInstance(BlockBuilderStatus.class))));

        IfStatement ifStatement = new IfStatement()
                .condition(selectedPositions.invoke("isList", boolean.class));
        body.append(ifStatement);

        ifStatement.ifTrue(new BytecodeBlock()
                .append(positions.set(selectedPositions.invoke("getPositions", int[].class)))
                .append(new ForLoop("positions loop")
                        .initialize(index.set(from))
                        .condition(lessThan(index, to))
                        .update(index.increment())
                        .body(thisVariable.invoke("project", void.class, session, page, positions.getElement(index)))));

        ifStatement.ifFalse(new ForLoop("range based loop")
                .initialize(index.set(from))
                .condition(lessThan(index, to))
                .update(index.increment())
                .body(thisVariable.invoke("project", void.class, session, page, index)));

        Variable block = scope.declareVariable(Block.class, "block");
        body.append(block.set(thisVariable.getField(blockBuilder).invoke("build", Block.class)))
                .append(block.ret());

        return method;
    }

    private MethodDefinition generateProjectMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            PreGeneratedExpressions preGeneratedExpressions,
            RowExpression projection,
            FieldDefinition blockBuilder)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter page = arg("page", Page.class);
        Parameter position = arg("position", int.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "project",
                type(void.class),
                ImmutableList.<Parameter>builder()
                        .add(session)
                        .add(page)
                        .add(position)
                        .build());

        method.comment("Projection: %s", projection.toString());

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        declareBlockVariables(projection, page, scope, body);

        Variable wasNullVariable = scope.declareVariable("wasNull", body, constantFalse());
        RowExpressionCompiler compiler = new RowExpressionCompiler(
                callSiteBinder,
                cachedInstanceBinder,
                fieldReferenceCompiler(callSiteBinder),
                metadata.getFunctionRegistry(),
                preGeneratedExpressions);

        body.append(thisVariable.getField(blockBuilder))
                .append(compiler.compile(projection, scope))
                .append(generateWrite(callSiteBinder, scope, wasNullVariable, projection.getType()))
                .ret();
        return method;
    }

    public Supplier<PageFilter> compileFilter(RowExpression filter)
    {
        requireNonNull(filter, "filter is null");

        PageFieldsToInputParametersRewriter.Result result = rewritePageFieldsToInputParameters(filter);

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        ClassDefinition classDefinition = defineFilterClass(result.getRewrittenExpression(), result.getInputChannels(), callSiteBinder);

        Class<? extends PageFilter> functionClass;
        try {
            functionClass = defineClass(classDefinition, PageFilter.class, callSiteBinder.getBindings(), getClass().getClassLoader());
        }
        catch (Exception e) {
            throw new PrestoException(COMPILER_ERROR, filter.toString(), e.getCause());
        }

        return () -> {
            try {
                return functionClass.newInstance();
            }
            catch (ReflectiveOperationException e) {
                throw new PrestoException(COMPILER_ERROR, e);
            }
        };
    }

    private ClassDefinition defineFilterClass(RowExpression filter, InputChannels inputChannels, CallSiteBinder callSiteBinder)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(PageFilter.class.getSimpleName()),
                type(Object.class),
                type(PageFilter.class));

        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        PreGeneratedExpressions preGeneratedExpressions = generateMethodsForLambdaAndTry(classDefinition, callSiteBinder, cachedInstanceBinder, filter);
        generateFilterMethod(classDefinition, callSiteBinder, cachedInstanceBinder, preGeneratedExpressions, filter);

        FieldDefinition selectedPositions = classDefinition.declareField(a(PRIVATE), "selectedPositions", boolean[].class);
        generatePageFilterMethod(classDefinition, selectedPositions);

        // isDeterministic
        classDefinition.declareMethod(a(PUBLIC), "isDeterministic", type(boolean.class))
                .getBody()
                .append(constantBoolean(determinismEvaluator.isDeterministic(filter)))
                .retBoolean();

        // getInputChannels
        classDefinition.declareMethod(a(PUBLIC), "getInputChannels", type(InputChannels.class))
                .getBody()
                .append(invoke(callSiteBinder.bind(inputChannels, InputChannels.class), "getInputChannels"))
                .retObject();

        // toString
        String toStringResult = toStringHelper(classDefinition.getType()
                .getJavaClassName())
                .add("filter", filter)
                .toString();
        classDefinition.declareMethod(a(PUBLIC), "toString", type(String.class))
                .getBody()
                // bind constant via invokedynamic to avoid constant pool issues due to large strings
                .append(invoke(callSiteBinder.bind(toStringResult, String.class), "toString"))
                .retObject();

        // constructor
        generateConstructor(classDefinition, cachedInstanceBinder, preGeneratedExpressions, method -> {
            Variable thisVariable = method.getScope().getThis();
            method.getBody().append(thisVariable.setField(selectedPositions, newArray(type(boolean[].class), 0)));
        });

        return classDefinition;
    }

    private static MethodDefinition generatePageFilterMethod(ClassDefinition classDefinition, FieldDefinition selectedPositionsField)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter page = arg("page", Page.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filter",
                type(SelectedPositions.class),
                ImmutableList.<Parameter>builder()
                        .add(session)
                        .add(page)
                        .build());

        Scope scope = method.getScope();
        Variable thisVariable = method.getThis();
        BytecodeBlock body = method.getBody();

        Variable positionCount = scope.declareVariable("positionCount", body, page.invoke("getPositionCount", int.class));

        body.append(new IfStatement("grow selectedPositions if necessary")
                .condition(lessThan(thisVariable.getField(selectedPositionsField).length(), positionCount))
                .ifTrue(thisVariable.setField(selectedPositionsField, newArray(type(boolean[].class), positionCount))));

        Variable selectedPositions = scope.declareVariable("selectedPositions", body, thisVariable.getField(selectedPositionsField));
        Variable position = scope.declareVariable(int.class, "position");
        body.append(new ForLoop()
                .initialize(position.set(constantInt(0)))
                .condition(lessThan(position, positionCount))
                .update(position.increment())
                .body(selectedPositions.setElement(position, thisVariable.invoke("filter", boolean.class, session, page, position))));

        body.append(invokeStatic(
                PageFilter.class,
                "positionsArrayToSelectedPositions",
                SelectedPositions.class,
                selectedPositions,
                positionCount)
                .ret());

        return method;
    }

    private MethodDefinition generateFilterMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            PreGeneratedExpressions preGeneratedExpressions,
            RowExpression filter)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter page = arg("page", Page.class);
        Parameter position = arg("position", int.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filter",
                type(boolean.class),
                ImmutableList.<Parameter>builder()
                        .add(session)
                        .add(page)
                        .add(position)
                        .build());

        method.comment("Filter: %s", filter.toString());

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        declareBlockVariables(filter, page, scope, body);

        Variable wasNullVariable = scope.declareVariable("wasNull", body, constantFalse());
        RowExpressionCompiler compiler = new RowExpressionCompiler(
                callSiteBinder,
                cachedInstanceBinder,
                fieldReferenceCompiler(callSiteBinder),
                metadata.getFunctionRegistry(),
                preGeneratedExpressions);

        Variable result = scope.declareVariable(boolean.class, "result");
        body.append(compiler.compile(filter, scope))
                // store result so we can check for null
                .putVariable(result)
                .append(and(not(wasNullVariable), result).ret());
        return method;
    }

    private PreGeneratedExpressions generateMethodsForLambdaAndTry(
            ClassDefinition containerClassDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            RowExpression expression)
    {
        Set<RowExpression> lambdaAndTryExpressions = ImmutableSet.copyOf(extractLambdaAndTryExpressions(expression));
        ImmutableMap.Builder<CallExpression, MethodDefinition> tryMethodMap = ImmutableMap.builder();
        ImmutableMap.Builder<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap = ImmutableMap.builder();

        int counter = 0;
        for (RowExpression lambdaOrTryExpression : lambdaAndTryExpressions) {
            if (lambdaOrTryExpression instanceof CallExpression) {
                CallExpression tryExpression = (CallExpression) lambdaOrTryExpression;
                verify(!Signatures.TRY.equals(tryExpression.getSignature().getName()));

                Parameter session = arg("session", ConnectorSession.class);
                List<Parameter> blocks = toBlockParameters(getInputChannels(tryExpression.getArguments()));
                Parameter position = arg("position", int.class);

                RowExpressionCompiler innerExpressionCompiler = new RowExpressionCompiler(
                        callSiteBinder,
                        cachedInstanceBinder,
                        fieldReferenceCompiler(callSiteBinder),
                        metadata.getFunctionRegistry(),
                        new PreGeneratedExpressions(tryMethodMap.build(), compiledLambdaMap.build()));

                List<Parameter> inputParameters = ImmutableList.<Parameter>builder()
                        .add(session)
                        .addAll(blocks)
                        .add(position)
                        .build();

                MethodDefinition tryMethod = defineTryMethod(
                        innerExpressionCompiler,
                        containerClassDefinition,
                        "try_" + counter,
                        inputParameters,
                        Primitives.wrap(tryExpression.getType().getJavaType()),
                        tryExpression,
                        callSiteBinder);

                tryMethodMap.put(tryExpression, tryMethod);
            }
            else if (lambdaOrTryExpression instanceof LambdaDefinitionExpression) {
                LambdaDefinitionExpression lambdaExpression = (LambdaDefinitionExpression) lambdaOrTryExpression;
                PreGeneratedExpressions preGeneratedExpressions = new PreGeneratedExpressions(tryMethodMap.build(), compiledLambdaMap.build());
                CompiledLambda compiledLambda = LambdaBytecodeGenerator.preGenerateLambdaExpression(
                        lambdaExpression,
                        "lambda_" + counter,
                        containerClassDefinition,
                        preGeneratedExpressions,
                        callSiteBinder,
                        cachedInstanceBinder,
                        metadata.getFunctionRegistry());
                compiledLambdaMap.put(lambdaExpression, compiledLambda);
            }
            else {
                throw new VerifyException(format("unexpected expression: %s", lambdaOrTryExpression.toString()));
            }
            counter++;
        }

        return new PreGeneratedExpressions(tryMethodMap.build(), compiledLambdaMap.build());
    }

    private static void generateConstructor(
            ClassDefinition classDefinition,
            CachedInstanceBinder cachedInstanceBinder,
            PreGeneratedExpressions preGeneratedExpressions,
            Consumer<MethodDefinition> additionalStatements)
    {
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC));

        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        additionalStatements.accept(constructorDefinition);

        cachedInstanceBinder.generateInitializations(thisVariable, body);
        for (CompiledLambda compiledLambda : preGeneratedExpressions.getCompiledLambdaMap().values()) {
            compiledLambda.generateInitialization(thisVariable, body);
        }
        body.ret();
    }

    private static void declareBlockVariables(RowExpression expression, Parameter page, Scope scope, BytecodeBlock body)
    {
        for (int channel : getInputChannels(expression)) {
            scope.declareVariable("block_" + channel, body, page.invoke("getBlock", Block.class, constantInt(channel)));
        }
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

    private static RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompiler(CallSiteBinder callSiteBinder)
    {
        return new InputReferenceCompiler(
                (scope, field) -> scope.getVariable("block_" + field),
                (scope, field) -> scope.getVariable("position"),
                callSiteBinder);
    }
}
