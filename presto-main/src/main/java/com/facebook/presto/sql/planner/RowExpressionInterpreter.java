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
package com.facebook.presto.sql.planner;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.RowBlockBuilder;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.FunctionType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.SqlInvokedScalarFunctionImplementation;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.InterpretedFunctionInvoker;
import com.facebook.presto.sql.planner.Interpreters.LambdaVariableResolver;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.util.Failures;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import io.airlift.joni.Regex;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.StandardTypes.ARRAY;
import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.StandardTypes.ROW;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.expressions.DynamicFilters.isDynamicFilter;
import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.metadata.CastType.JSON_TO_ARRAY_CAST;
import static com.facebook.presto.metadata.CastType.JSON_TO_MAP_CAST;
import static com.facebook.presto.metadata.CastType.JSON_TO_ROW_CAST;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.EVALUATED;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.SERIALIZABLE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.BIND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.COALESCE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.DEREFERENCE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.NULL_IF;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.ROW_CONSTRUCTOR;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.SWITCH;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.WHEN;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.gen.VarArgsToMapAdapterGenerator.generateVarArgsToMapAdapter;
import static com.facebook.presto.sql.planner.Interpreters.interpretDereference;
import static com.facebook.presto.sql.planner.Interpreters.interpretLikePredicate;
import static com.facebook.presto.sql.planner.LiteralEncoder.estimatedSizeInBytes;
import static com.facebook.presto.sql.planner.LiteralEncoder.isSupportedLiteralType;
import static com.facebook.presto.sql.planner.RowExpressionInterpreter.SpecialCallResult.changed;
import static com.facebook.presto.sql.planner.RowExpressionInterpreter.SpecialCallResult.notChanged;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.SqlFunctionUtils.getSqlFunctionRowExpression;
import static com.facebook.presto.type.LikeFunctions.isLikePattern;
import static com.facebook.presto.type.LikeFunctions.unescapeLiteralLikePattern;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.insertArguments;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class RowExpressionInterpreter
{
    private static final long MAX_SERIALIZABLE_OBJECT_SIZE = 1000;
    private final RowExpression expression;
    private final Metadata metadata;
    private final ConnectorSession session;
    private final Level optimizationLevel;
    private final InterpretedFunctionInvoker functionInvoker;
    private final RowExpressionDeterminismEvaluator determinismEvaluator;
    private final FunctionAndTypeManager functionAndTypeManager;
    private final FunctionResolution resolution;

    private final Visitor visitor;

    public static Object evaluateConstantRowExpression(RowExpression expression, Metadata metadata, ConnectorSession session)
    {
        // evaluate the expression
        Object result = new RowExpressionInterpreter(expression, metadata, session, EVALUATED).evaluate();
        verify(!(result instanceof RowExpression), "RowExpression interpreter returned an unresolved expression");
        return result;
    }

    public static RowExpressionInterpreter rowExpressionInterpreter(RowExpression expression, Metadata metadata, ConnectorSession session)
    {
        return new RowExpressionInterpreter(expression, metadata, session, EVALUATED);
    }

    public RowExpressionInterpreter(RowExpression expression, Metadata metadata, ConnectorSession session, Level optimizationLevel)
    {
        this.expression = requireNonNull(expression, "expression is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.session = requireNonNull(session, "session is null");
        this.optimizationLevel = optimizationLevel;
        this.functionInvoker = new InterpretedFunctionInvoker(metadata.getFunctionAndTypeManager());
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(metadata.getFunctionAndTypeManager());
        this.resolution = new FunctionResolution(metadata.getFunctionAndTypeManager());
        this.functionAndTypeManager = metadata.getFunctionAndTypeManager();

        this.visitor = new Visitor();
    }

    public Type getType()
    {
        return expression.getType();
    }

    public Object evaluate()
    {
        checkState(optimizationLevel.ordinal() >= EVALUATED.ordinal(), "evaluate() not allowed for optimizer");
        return expression.accept(visitor, null);
    }

    public Object optimize()
    {
        checkState(optimizationLevel.ordinal() < EVALUATED.ordinal(), "optimize() not allowed for interpreter");
        return optimize(null);
    }

    /**
     * Replace symbol with constants
     */
    public Object optimize(VariableResolver inputs)
    {
        checkState(optimizationLevel.ordinal() <= EVALUATED.ordinal(), "optimize(SymbolResolver) not allowed for interpreter");
        return expression.accept(visitor, inputs);
    }

    private class Visitor
            implements RowExpressionVisitor<Object, Object>
    {
        @Override
        public Object visitInputReference(InputReferenceExpression node, Object context)
        {
            return node;
        }

        @Override
        public Object visitConstant(ConstantExpression node, Object context)
        {
            return node.getValue();
        }

        @Override
        public Object visitVariableReference(VariableReferenceExpression node, Object context)
        {
            if (context instanceof VariableResolver) {
                return ((VariableResolver) context).getValue(node);
            }
            return node;
        }

        @Override
        public Object visitCall(CallExpression node, Object context)
        {
            List<Type> argumentTypes = new ArrayList<>();
            List<Object> argumentValues = new ArrayList<>();
            for (RowExpression expression : node.getArguments()) {
                Object value = expression.accept(this, context);
                argumentValues.add(value);
                argumentTypes.add(expression.getType());
            }

            FunctionHandle functionHandle = node.getFunctionHandle();
            FunctionMetadata functionMetadata = metadata.getFunctionAndTypeManager().getFunctionMetadata(node.getFunctionHandle());
            if (!functionMetadata.isCalledOnNullInput()) {
                for (Object value : argumentValues) {
                    if (value == null) {
                        return null;
                    }
                }
            }

            // Special casing for large constant array construction
            if (resolution.isArrayConstructor(functionHandle)) {
                SpecialCallResult result = tryHandleArrayConstructor(node, argumentValues);
                if (result.isChanged()) {
                    return result.getValue();
                }
            }

            // Special casing for cast
            if (resolution.isCastFunction(functionHandle)) {
                SpecialCallResult result = tryHandleCast(node, argumentValues);
                if (result.isChanged()) {
                    return result.getValue();
                }
            }

            // Special casing for like
            if (resolution.isLikeFunction(functionHandle)) {
                SpecialCallResult result = tryHandleLike(node, argumentValues, argumentTypes, context);
                if (result.isChanged()) {
                    return result.getValue();
                }
            }

            if (functionMetadata.getFunctionKind() != SCALAR) {
                return call(node.getDisplayName(), functionHandle, node.getType(), toRowExpressions(argumentValues, node.getArguments()));
            }

            // do not optimize non-deterministic functions
            if (optimizationLevel.ordinal() < EVALUATED.ordinal() &&
                    (!functionMetadata.isDeterministic() ||
                            hasUnresolvedValue(argumentValues) ||
                            isDynamicFilter(node) ||
                            resolution.isFailFunction(functionHandle))) {
                return call(node.getDisplayName(), functionHandle, node.getType(), toRowExpressions(argumentValues, node.getArguments()));
            }

            Object value;
            switch (functionMetadata.getImplementationType()) {
                case BUILTIN:
                    value = functionInvoker.invoke(functionHandle, session.getSqlFunctionProperties(), argumentValues);
                    break;
                case SQL:
                    SqlInvokedScalarFunctionImplementation functionImplementation = (SqlInvokedScalarFunctionImplementation) functionAndTypeManager.getScalarFunctionImplementation(functionHandle);
                    RowExpression function = getSqlFunctionRowExpression(
                            functionMetadata,
                            functionImplementation,
                            metadata,
                            session.getSqlFunctionProperties(),
                            session.getSessionFunctions(),
                            node.getArguments());
                    RowExpressionInterpreter rowExpressionInterpreter = new RowExpressionInterpreter(function, metadata, session, optimizationLevel);
                    if (optimizationLevel.ordinal() >= EVALUATED.ordinal()) {
                        value = rowExpressionInterpreter.evaluate();
                    }
                    else {
                        value = rowExpressionInterpreter.optimize();
                    }
                    break;
                case THRIFT:
                    // do not interpret remote functions on coordinator
                    return call(node.getDisplayName(), functionHandle, node.getType(), toRowExpressions(argumentValues, node.getArguments()));
                default:
                    throw new IllegalArgumentException(format("Unsupported function implementation type: %s", functionMetadata.getImplementationType()));
            }

            if (optimizationLevel.ordinal() <= SERIALIZABLE.ordinal() && !isSerializable(value, node.getType())) {
                return call(node.getDisplayName(), functionHandle, node.getType(), toRowExpressions(argumentValues, node.getArguments()));
            }
            return value;
        }

        @Override
        public Object visitLambda(LambdaDefinitionExpression node, Object context)
        {
            if (optimizationLevel.ordinal() < EVALUATED.ordinal()) {
                // TODO: enable optimization related to lambda expression
                // Currently, we are not able to determine if lambda is deterministic.
                // context is passed down as null here since lambda argument can only be resolved under the evaluation context.
                RowExpression rewrittenBody = toRowExpression(processWithExceptionHandling(node.getBody(), null), node.getBody());
                if (!rewrittenBody.equals(node.getBody())) {
                    return new LambdaDefinitionExpression(node.getArgumentTypes(), node.getArguments(), rewrittenBody);
                }
                return node;
            }
            RowExpression body = node.getBody();
            FunctionType functionType = (FunctionType) node.getType();
            checkArgument(node.getArguments().size() == functionType.getArgumentTypes().size());

            return generateVarArgsToMapAdapter(
                    Primitives.wrap(functionType.getReturnType().getJavaType()),
                    functionType.getArgumentTypes().stream()
                            .map(Type::getJavaType)
                            .map(Primitives::wrap)
                            .collect(toImmutableList()),
                    node.getArguments(),
                    map -> body.accept(this, new LambdaVariableResolver(map)));
        }

        @Override
        public Object visitSpecialForm(SpecialFormExpression node, Object context)
        {
            switch (node.getForm()) {
                case IF: {
                    checkArgument(node.getArguments().size() == 3);
                    Object condition = processWithExceptionHandling(node.getArguments().get(0), context);

                    if (condition instanceof RowExpression) {
                        return new SpecialFormExpression(
                                IF,
                                node.getType(),
                                toRowExpression(condition, node.getArguments().get(0)),
                                toRowExpression(processWithExceptionHandling(node.getArguments().get(1), context), node.getArguments().get(1)),
                                toRowExpression(processWithExceptionHandling(node.getArguments().get(2), context), node.getArguments().get(2)));
                    }
                    else if (Boolean.TRUE.equals(condition)) {
                        return processWithExceptionHandling(node.getArguments().get(1), context);
                    }

                    return processWithExceptionHandling(node.getArguments().get(2), context);
                }
                case NULL_IF: {
                    checkArgument(node.getArguments().size() == 2);
                    Object left = processWithExceptionHandling(node.getArguments().get(0), context);
                    if (left == null) {
                        return null;
                    }

                    Object right = processWithExceptionHandling(node.getArguments().get(1), context);
                    if (right == null) {
                        return left;
                    }

                    if (hasUnresolvedValue(left, right)) {
                        return new SpecialFormExpression(
                                NULL_IF,
                                node.getType(),
                                toRowExpression(left, node.getArguments().get(0)),
                                toRowExpression(right, node.getArguments().get(1)));
                    }

                    Type leftType = node.getArguments().get(0).getType();
                    Type rightType = node.getArguments().get(1).getType();
                    Type commonType = metadata.getFunctionAndTypeManager().getCommonSuperType(leftType, rightType).get();
                    FunctionHandle firstCast = metadata.getFunctionAndTypeManager().lookupCast(CAST, leftType.getTypeSignature(), commonType.getTypeSignature());
                    FunctionHandle secondCast = metadata.getFunctionAndTypeManager().lookupCast(CAST, rightType.getTypeSignature(), commonType.getTypeSignature());

                    // cast(first as <common type>) == cast(second as <common type>)
                    boolean equal = Boolean.TRUE.equals(invokeOperator(
                            EQUAL,
                            ImmutableList.of(commonType, commonType),
                            ImmutableList.of(
                                    functionInvoker.invoke(firstCast, session.getSqlFunctionProperties(), left),
                                    functionInvoker.invoke(secondCast, session.getSqlFunctionProperties(), right))));

                    if (equal) {
                        return null;
                    }
                    return left;
                }
                case IS_NULL: {
                    checkArgument(node.getArguments().size() == 1);
                    Object value = processWithExceptionHandling(node.getArguments().get(0), context);
                    if (value instanceof RowExpression) {
                        return new SpecialFormExpression(
                                IS_NULL,
                                node.getType(),
                                toRowExpression(value, node.getArguments().get(0)));
                    }
                    return value == null;
                }
                case AND: {
                    Object left = node.getArguments().get(0).accept(this, context);
                    Object right;

                    if (Boolean.FALSE.equals(left)) {
                        return false;
                    }

                    right = node.getArguments().get(1).accept(this, context);

                    if (Boolean.TRUE.equals(right)) {
                        return left;
                    }

                    if (Boolean.FALSE.equals(right) || Boolean.TRUE.equals(left)) {
                        return right;
                    }

                    if (left == null && right == null) {
                        return null;
                    }
                    return new SpecialFormExpression(
                            AND,
                            node.getType(),
                            toRowExpressions(
                                    asList(left, right),
                                    node.getArguments().subList(0, 2)));
                }
                case OR: {
                    Object left = node.getArguments().get(0).accept(this, context);
                    Object right;

                    if (Boolean.TRUE.equals(left)) {
                        return true;
                    }

                    right = node.getArguments().get(1).accept(this, context);

                    if (Boolean.FALSE.equals(right)) {
                        return left;
                    }

                    if (Boolean.TRUE.equals(right) || Boolean.FALSE.equals(left)) {
                        return right;
                    }

                    if (left == null && right == null) {
                        return null;
                    }
                    return new SpecialFormExpression(
                            OR,
                            node.getType(),
                            toRowExpressions(
                                    asList(left, right),
                                    node.getArguments().subList(0, 2)));
                }
                case ROW_CONSTRUCTOR: {
                    RowType rowType = (RowType) node.getType();
                    List<Type> parameterTypes = rowType.getTypeParameters();
                    List<RowExpression> arguments = node.getArguments();
                    checkArgument(parameterTypes.size() == arguments.size(), "RowConstructor does not contain all fields");
                    for (int i = 0; i < parameterTypes.size(); i++) {
                        checkArgument(parameterTypes.get(i).equals(arguments.get(i).getType()), "RowConstructor has field with incorrect type");
                    }

                    int cardinality = arguments.size();
                    List<Object> values = new ArrayList<>(cardinality);
                    arguments.forEach(argument -> values.add(argument.accept(this, context)));
                    if (hasUnresolvedValue(values)) {
                        return new SpecialFormExpression(ROW_CONSTRUCTOR, node.getType(), toRowExpressions(values, node.getArguments()));
                    }
                    else {
                        BlockBuilder blockBuilder = new RowBlockBuilder(parameterTypes, null, 1);
                        BlockBuilder singleRowBlockWriter = blockBuilder.beginBlockEntry();
                        for (int i = 0; i < cardinality; ++i) {
                            writeNativeValue(parameterTypes.get(i), singleRowBlockWriter, values.get(i));
                        }
                        blockBuilder.closeEntry();
                        return rowType.getObject(blockBuilder, 0);
                    }
                }
                case COALESCE: {
                    Type type = node.getType();
                    List<Object> values = node.getArguments().stream()
                            .map(value -> processWithExceptionHandling(value, context))
                            .filter(Objects::nonNull)
                            .flatMap(expression -> {
                                if (expression instanceof SpecialFormExpression && ((SpecialFormExpression) expression).getForm() == COALESCE) {
                                    return ((SpecialFormExpression) expression).getArguments().stream();
                                }
                                return Stream.of(expression);
                            })
                            .collect(toList());

                    if ((!values.isEmpty() && !(values.get(0) instanceof RowExpression)) || values.size() == 1) {
                        return values.get(0);
                    }
                    ImmutableList.Builder<RowExpression> operandsBuilder = ImmutableList.builder();
                    Set<RowExpression> visitedExpression = new HashSet<>();
                    for (Object value : values) {
                        RowExpression expression = LiteralEncoder.toRowExpression(value, type);
                        if (!determinismEvaluator.isDeterministic(expression) || visitedExpression.add(expression)) {
                            operandsBuilder.add(expression);
                        }
                        if (expression instanceof ConstantExpression && !(((ConstantExpression) expression).getValue() == null)) {
                            break;
                        }
                    }
                    List<RowExpression> expressions = operandsBuilder.build();

                    if (expressions.isEmpty()) {
                        return null;
                    }

                    if (expressions.size() == 1) {
                        return getOnlyElement(expressions);
                    }
                    return new SpecialFormExpression(COALESCE, node.getType(), expressions);
                }
                case IN: {
                    checkArgument(node.getArguments().size() >= 2, "values must not be empty");

                    // use toList to handle null values
                    List<RowExpression> valueExpressions = node.getArguments().subList(1, node.getArguments().size());
                    List<Object> values = valueExpressions.stream().map(value -> value.accept(this, context)).collect(toList());
                    List<Type> valuesTypes = valueExpressions.stream().map(RowExpression::getType).collect(toImmutableList());
                    Object target = node.getArguments().get(0).accept(this, context);
                    Type targetType = node.getArguments().get(0).getType();

                    if (target == null) {
                        return null;
                    }

                    boolean hasUnresolvedValue = false;
                    if (target instanceof RowExpression) {
                        hasUnresolvedValue = true;
                    }

                    boolean hasNullValue = false;
                    boolean found = false;
                    List<RowExpression> unresolvedValues = new ArrayList<>(values.size());
                    for (int i = 0; i < values.size(); i++) {
                        Object value = values.get(i);
                        Type valueType = valuesTypes.get(i);
                        if (value instanceof RowExpression || target instanceof RowExpression) {
                            hasUnresolvedValue = true;
                            unresolvedValues.add(toRowExpression(value, valueExpressions.get(i)));
                            continue;
                        }

                        if (value == null) {
                            hasNullValue = true;
                        }
                        else {
                            Boolean result = (Boolean) invokeOperator(EQUAL, ImmutableList.of(targetType, valueType), ImmutableList.of(target, value));
                            if (result == null) {
                                hasNullValue = true;
                            }
                            else if (!found && result) {
                                // in does not short-circuit so we must evaluate all value in the list
                                found = true;
                            }
                        }
                    }
                    if (found) {
                        return true;
                    }

                    if (hasUnresolvedValue) {
                        List<RowExpression> simplifiedExpressionValues = Stream.concat(
                                Stream.concat(
                                        Stream.of(toRowExpression(target, node.getArguments().get(0))),
                                        unresolvedValues.stream().filter(determinismEvaluator::isDeterministic).distinct()),
                                unresolvedValues.stream().filter((expression -> !determinismEvaluator.isDeterministic(expression))))
                                .collect(toImmutableList());
                        return new SpecialFormExpression(IN, node.getType(), simplifiedExpressionValues);
                    }
                    if (hasNullValue) {
                        return null;
                    }
                    return false;
                }
                case DEREFERENCE: {
                    checkArgument(node.getArguments().size() == 2);

                    Object base = node.getArguments().get(0).accept(this, context);
                    int index = ((Number) node.getArguments().get(1).accept(this, context)).intValue();

                    // if the base part is evaluated to be null, the dereference expression should also be null
                    if (base == null) {
                        return null;
                    }

                    if (hasUnresolvedValue(base)) {
                        return new SpecialFormExpression(
                                DEREFERENCE,
                                node.getType(),
                                toRowExpression(base, node.getArguments().get(0)),
                                toRowExpression((long) index, node.getArguments().get(1)));
                    }
                    return interpretDereference(base, node.getType(), index);
                }
                case BIND: {
                    List<Object> values = node.getArguments()
                            .stream()
                            .map(value -> value.accept(this, context))
                            .collect(toImmutableList());
                    if (hasUnresolvedValue(values)) {
                        return new SpecialFormExpression(
                                BIND,
                                node.getType(),
                                toRowExpressions(values, node.getArguments()));
                    }
                    return insertArguments((MethodHandle) values.get(values.size() - 1), 0, values.subList(0, values.size() - 1).toArray());
                }
                case SWITCH: {
                    List<RowExpression> whenClauses;
                    Object elseValue = null;
                    RowExpression last = node.getArguments().get(node.getArguments().size() - 1);
                    if (last instanceof SpecialFormExpression && ((SpecialFormExpression) last).getForm().equals(WHEN)) {
                        whenClauses = node.getArguments().subList(1, node.getArguments().size());
                    }
                    else {
                        whenClauses = node.getArguments().subList(1, node.getArguments().size() - 1);
                    }

                    List<RowExpression> simplifiedWhenClauses = new ArrayList<>();
                    Object value = processWithExceptionHandling(node.getArguments().get(0), context);
                    if (value != null) {
                        for (RowExpression whenClause : whenClauses) {
                            checkArgument(whenClause instanceof SpecialFormExpression && ((SpecialFormExpression) whenClause).getForm().equals(WHEN));

                            RowExpression operand = ((SpecialFormExpression) whenClause).getArguments().get(0);
                            RowExpression result = ((SpecialFormExpression) whenClause).getArguments().get(1);

                            Object operandValue = processWithExceptionHandling(operand, context);

                            // call equals(value, operand)
                            if (operandValue instanceof RowExpression || value instanceof RowExpression) {
                                // cannot fully evaluate, add updated whenClause
                                simplifiedWhenClauses.add(new SpecialFormExpression(WHEN, whenClause.getType(), toRowExpression(operandValue, operand), toRowExpression(processWithExceptionHandling(result, context), result)));
                            }
                            else if (operandValue != null) {
                                Boolean isEqual = (Boolean) invokeOperator(
                                        EQUAL,
                                        ImmutableList.of(node.getArguments().get(0).getType(), operand.getType()),
                                        ImmutableList.of(value, operandValue));
                                if (isEqual != null && isEqual) {
                                    if (simplifiedWhenClauses.isEmpty()) {
                                        // this is the left-most true predicate. So return it.
                                        return processWithExceptionHandling(result, context);
                                    }

                                    elseValue = processWithExceptionHandling(result, context);
                                    break; // Done we found the last match. Don't need to go any further.
                                }
                            }
                        }
                    }

                    if (elseValue == null) {
                        elseValue = processWithExceptionHandling(last, context);
                    }

                    if (simplifiedWhenClauses.isEmpty()) {
                        return elseValue;
                    }

                    ImmutableList.Builder<RowExpression> argumentsBuilder = ImmutableList.builder();
                    argumentsBuilder.add(toRowExpression(value, node.getArguments().get(0)))
                            .addAll(simplifiedWhenClauses)
                            .add(toRowExpression(elseValue, last));
                    return new SpecialFormExpression(SWITCH, node.getType(), argumentsBuilder.build());
                }
                default:
                    throw new IllegalStateException("Can not compile special form: " + node.getForm());
            }
        }

        private Object processWithExceptionHandling(RowExpression expression, Object context)
        {
            if (expression == null) {
                return null;
            }
            try {
                return expression.accept(this, context);
            }
            catch (RuntimeException e) {
                // HACK
                // Certain operations like 0 / 0 or likeExpression may throw exceptions.
                // Wrap them in a call that will throw the exception if the expression is actually executed
                return createFailureFunction(e, expression.getType());
            }
        }

        private RowExpression createFailureFunction(RuntimeException exception, Type type)
        {
            requireNonNull(exception, "Exception is null");

            String failureInfo = JsonCodec.jsonCodec(FailureInfo.class).toJson(Failures.toFailure(exception).toFailureInfo());
            FunctionHandle jsonParse = metadata.getFunctionAndTypeManager().lookupFunction("json_parse", fromTypes(VARCHAR));
            Object json = functionInvoker.invoke(jsonParse, session.getSqlFunctionProperties(), utf8Slice(failureInfo));
            FunctionHandle cast = metadata.getFunctionAndTypeManager().lookupCast(CAST, UNKNOWN.getTypeSignature(), type.getTypeSignature());
            if (exception instanceof PrestoException) {
                long errorCode = ((PrestoException) exception).getErrorCode().getCode();
                FunctionHandle failureFunction = metadata.getFunctionAndTypeManager().lookupFunction("fail", fromTypes(INTEGER, JSON));
                return call(CAST.name(), cast, type, call("fail", failureFunction, UNKNOWN, constant(errorCode, INTEGER), LiteralEncoder.toRowExpression(json, JSON)));
            }

            FunctionHandle failureFunction = metadata.getFunctionAndTypeManager().lookupFunction("fail", fromTypes(JSON));
            return call(CAST.name(), cast, type, call("fail", failureFunction, UNKNOWN, LiteralEncoder.toRowExpression(json, JSON)));
        }

        private boolean hasUnresolvedValue(Object... values)
        {
            return hasUnresolvedValue(ImmutableList.copyOf(values));
        }

        private boolean hasUnresolvedValue(List<Object> values)
        {
            return values.stream().anyMatch(instanceOf(RowExpression.class)::apply);
        }

        private Object invokeOperator(OperatorType operatorType, List<? extends Type> argumentTypes, List<Object> argumentValues)
        {
            FunctionHandle operatorHandle = metadata.getFunctionAndTypeManager().resolveOperator(operatorType, fromTypes(argumentTypes));
            return functionInvoker.invoke(operatorHandle, session.getSqlFunctionProperties(), argumentValues);
        }

        private List<RowExpression> toRowExpressions(List<Object> values, List<RowExpression> unchangedValues)
        {
            checkArgument(values != null, "value is null");
            checkArgument(unchangedValues != null, "value is null");
            checkArgument(values.size() == unchangedValues.size());
            ImmutableList.Builder<RowExpression> rowExpressions = ImmutableList.builder();
            for (int i = 0; i < values.size(); i++) {
                rowExpressions.add(toRowExpression(values.get(i), unchangedValues.get(i)));
            }
            return rowExpressions.build();
        }

        private RowExpression toRowExpression(Object value, RowExpression originalRowExpression)
        {
            if (optimizationLevel.ordinal() <= SERIALIZABLE.ordinal() && !isSerializable(value, originalRowExpression.getType())) {
                return originalRowExpression;
            }
            // handle lambda
            if (optimizationLevel.ordinal() < EVALUATED.ordinal() && value instanceof MethodHandle) {
                return originalRowExpression;
            }
            return LiteralEncoder.toRowExpression(value, originalRowExpression.getType());
        }

        private boolean isSerializable(Object value, Type type)
        {
            // If value is already RowExpression, constant values contained inside should already have been made serializable. Otherwise, we make sure the object is small and serializable.
            return value instanceof RowExpression || (isSupportedLiteralType(type) && estimatedSizeInBytes(value) <= MAX_SERIALIZABLE_OBJECT_SIZE);
        }

        private SpecialCallResult tryHandleArrayConstructor(CallExpression callExpression, List<Object> argumentValues)
        {
            checkArgument(resolution.isArrayConstructor(callExpression.getFunctionHandle()));
            boolean allConstants = true;
            for (Object values : argumentValues) {
                if (values instanceof RowExpression) {
                    allConstants = false;
                    break;
                }
            }
            if (allConstants) {
                Type elementType = ((ArrayType) callExpression.getType()).getElementType();
                BlockBuilder arrayBlockBuilder = elementType.createBlockBuilder(null, argumentValues.size());
                for (Object value : argumentValues) {
                    writeNativeValue(elementType, arrayBlockBuilder, value);
                }
                return changed(arrayBlockBuilder.build());
            }
            return notChanged();
        }

        private SpecialCallResult tryHandleCast(CallExpression callExpression, List<Object> argumentValues)
        {
            checkArgument(resolution.isCastFunction(callExpression.getFunctionHandle()));
            checkArgument(callExpression.getArguments().size() == 1);
            RowExpression source = callExpression.getArguments().get(0);
            Type sourceType = source.getType();
            Type targetType = callExpression.getType();

            Object value = argumentValues.get(0);

            if (value == null) {
                return changed(null);
            }

            if (value instanceof RowExpression) {
                if (sourceType.equals(targetType)) {
                    return changed(value);
                }
                if (callExpression.getArguments().get(0) instanceof CallExpression) {
                    // Optimization for CAST(JSON_PARSE(...) AS ARRAY/MAP/ROW), solves https://github.com/prestodb/presto/issues/12829
                    CallExpression innerCall = (CallExpression) callExpression.getArguments().get(0);
                    if (functionAndTypeManager.getFunctionMetadata(innerCall.getFunctionHandle()).getName().getObjectName().equals("json_parse")) {
                        checkArgument(innerCall.getType().equals(JSON));
                        checkArgument(innerCall.getArguments().size() == 1);
                        TypeSignature returnType = functionAndTypeManager.getFunctionMetadata(callExpression.getFunctionHandle()).getReturnType();
                        if (returnType.getBase().equals(ARRAY)) {
                            return changed(call(
                                    JSON_TO_ARRAY_CAST.name(),
                                    functionAndTypeManager.lookupCast(
                                            JSON_TO_ARRAY_CAST,
                                            parseTypeSignature(StandardTypes.VARCHAR),
                                            returnType),
                                    callExpression.getType(),
                                    innerCall.getArguments()));
                        }
                        if (returnType.getBase().equals(MAP)) {
                            return changed(call(
                                    JSON_TO_MAP_CAST.name(),
                                    functionAndTypeManager.lookupCast(
                                            JSON_TO_MAP_CAST,
                                            parseTypeSignature(StandardTypes.VARCHAR),
                                            returnType),
                                    callExpression.getType(),
                                    innerCall.getArguments()));
                        }
                        if (returnType.getBase().equals(ROW)) {
                            return changed(call(
                                    JSON_TO_ROW_CAST.name(),
                                    functionAndTypeManager.lookupCast(
                                            JSON_TO_ROW_CAST,
                                            parseTypeSignature(StandardTypes.VARCHAR),
                                            returnType),
                                    callExpression.getType(),
                                    innerCall.getArguments()));
                        }
                    }
                }
                return changed(call(callExpression.getDisplayName(), callExpression.getFunctionHandle(), callExpression.getType(), toRowExpression(value, source)));
            }

            // TODO: still there is limitation for RowExpression. Example types could be Regex
            if (optimizationLevel.ordinal() <= SERIALIZABLE.ordinal() && !isSupportedLiteralType(targetType)) {
                // Otherwise, cast will be evaluated through invoke later and generates unserializable constant expression.
                return changed(call(callExpression.getDisplayName(), callExpression.getFunctionHandle(), callExpression.getType(), toRowExpression(value, source)));
            }

            if (metadata.getFunctionAndTypeManager().isTypeOnlyCoercion(sourceType, targetType)) {
                return changed(value);
            }
            return notChanged();
        }

        private SpecialCallResult tryHandleLike(CallExpression callExpression, List<Object> argumentValues, List<Type> argumentTypes, Object context)
        {
            FunctionResolution resolution = new FunctionResolution(metadata.getFunctionAndTypeManager());
            checkArgument(resolution.isLikeFunction(callExpression.getFunctionHandle()));
            checkArgument(callExpression.getArguments().size() == 2);
            RowExpression likePatternExpression = callExpression.getArguments().get(1);
            if (!(likePatternExpression instanceof CallExpression &&
                    (((CallExpression) likePatternExpression).getFunctionHandle().equals(resolution.likePatternFunction()) ||
                            (resolution.isCastFunction(((CallExpression) likePatternExpression).getFunctionHandle()))))) {
                // expression was already optimized
                return notChanged();
            }
            Object value = argumentValues.get(0);
            Object possibleCompiledPattern = argumentValues.get(1);

            if (value == null) {
                return changed(null);
            }

            CallExpression likePatternCall = (CallExpression) likePatternExpression;

            Object nonCompiledPattern = likePatternCall.getArguments().get(0).accept(this, context);
            if (nonCompiledPattern == null) {
                return changed(null);
            }

            boolean hasEscape = false;  // We cannot use Optional given escape could exist and its value is null
            Object escape = null;
            if (likePatternCall.getArguments().size() == 2) {
                hasEscape = true;
                escape = likePatternCall.getArguments().get(1).accept(this, context);
            }

            if (hasEscape && escape == null) {
                return changed(null);
            }

            if (!hasUnresolvedValue(value) && !hasUnresolvedValue(nonCompiledPattern) && (!hasEscape || !hasUnresolvedValue(escape))) {
                // fast path when we know the pattern and escape are constants
                if (possibleCompiledPattern instanceof Regex) {
                    return changed(interpretLikePredicate(argumentTypes.get(0), (Slice) value, (Regex) possibleCompiledPattern));
                }
                if (possibleCompiledPattern == null) {
                    return changed(null);
                }

                checkState(possibleCompiledPattern instanceof CallExpression);
                // this corresponds to ExpressionInterpreter::getConstantPattern
                if (hasEscape) {
                    // like_pattern(pattern, escape)
                    possibleCompiledPattern = functionInvoker.invoke(((CallExpression) possibleCompiledPattern).getFunctionHandle(), session.getSqlFunctionProperties(), nonCompiledPattern, escape);
                }
                else {
                    // like_pattern(pattern)
                    possibleCompiledPattern = functionInvoker.invoke(((CallExpression) possibleCompiledPattern).getFunctionHandle(), session.getSqlFunctionProperties(), nonCompiledPattern);
                }

                checkState(possibleCompiledPattern instanceof Regex, "unexpected like pattern type " + possibleCompiledPattern.getClass());
                return changed(interpretLikePredicate(argumentTypes.get(0), (Slice) value, (Regex) possibleCompiledPattern));
            }

            // if pattern is a constant without % or _ replace with a comparison
            if (nonCompiledPattern instanceof Slice && (escape == null || escape instanceof Slice) && !isLikePattern((Slice) nonCompiledPattern, (Slice) escape)) {
                Slice unescapedPattern = unescapeLiteralLikePattern((Slice) nonCompiledPattern, (Slice) escape);
                Type valueType = argumentTypes.get(0);
                Type patternType = createVarcharType(unescapedPattern.length());
                Optional<Type> commonSuperType = metadata.getFunctionAndTypeManager().getCommonSuperType(valueType, patternType);
                checkArgument(commonSuperType.isPresent(), "Missing super type when optimizing %s", callExpression);
                RowExpression valueExpression = LiteralEncoder.toRowExpression(value, valueType);
                RowExpression patternExpression = LiteralEncoder.toRowExpression(unescapedPattern, patternType);
                Type superType = commonSuperType.get();
                if (!valueType.equals(superType)) {
                    FunctionHandle cast = metadata.getFunctionAndTypeManager().lookupCast(CAST, valueType.getTypeSignature(), superType.getTypeSignature());
                    valueExpression = call(CAST.name(), cast, superType, valueExpression);
                }
                if (!patternType.equals(superType)) {
                    FunctionHandle cast = metadata.getFunctionAndTypeManager().lookupCast(CAST, patternType.getTypeSignature(), superType.getTypeSignature());
                    patternExpression = call(CAST.name(), cast, superType, patternExpression);
                }
                FunctionHandle equal = metadata.getFunctionAndTypeManager().resolveOperator(EQUAL, fromTypes(superType, superType));
                return changed(call(EQUAL.name(), equal, BOOLEAN, valueExpression, patternExpression).accept(this, context));
            }
            return notChanged();
        }
    }

    static final class SpecialCallResult
    {
        private final Object value;
        private final boolean changed;

        private SpecialCallResult(Object value, boolean changed)
        {
            this.value = value;
            this.changed = changed;
        }

        public static SpecialCallResult notChanged()
        {
            return new SpecialCallResult(null, false);
        }

        public static SpecialCallResult changed(Object value)
        {
            return new SpecialCallResult(value, true);
        }

        public Object getValue()
        {
            return value;
        }

        public boolean isChanged()
        {
            return changed;
        }
    }
}
