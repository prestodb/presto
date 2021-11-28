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
import com.facebook.presto.Session;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.RowBlockBuilder;
import com.facebook.presto.common.block.SingleRowBlock;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.FunctionType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.RowType.Field;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeUtils;
import com.facebook.presto.expressions.DynamicFilters;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.scalar.ArraySubscriptOperator;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.SqlInvokedScalarFunctionImplementation;
import com.facebook.presto.sql.InterpretedFunctionInvoker;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Interpreters.LambdaVariableResolver;
import com.facebook.presto.sql.planner.iterative.rule.DesugarCurrentUser;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BindExpression;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CurrentUser;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FieldReference;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QuantifiedComparisonExpression;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.type.LikeFunctions;
import com.facebook.presto.util.Failures;
import com.facebook.presto.util.FastutilSetHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Primitives;
import io.airlift.joni.Regex;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.isLegacyRowFieldOrdinalAccessEnabled;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.TypeUtils.isEnumType;
import static com.facebook.presto.common.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.metadata.FunctionAndTypeManager.qualifyObjectName;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.function.FunctionImplementationType.JAVA;
import static com.facebook.presto.spi.function.FunctionImplementationType.SQL;
import static com.facebook.presto.sql.analyzer.ConstantExpressionVerifier.verifyExpressionIsConstant;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.createConstantAnalyzer;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.resolveEnumLiteral;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.gen.VarArgsToMapAdapterGenerator.generateVarArgsToMapAdapter;
import static com.facebook.presto.sql.planner.ExpressionDeterminismEvaluator.isDeterministic;
import static com.facebook.presto.sql.planner.Interpreters.interpretDereference;
import static com.facebook.presto.sql.planner.Interpreters.interpretLikePredicate;
import static com.facebook.presto.sql.planner.LiteralEncoder.estimatedSizeInBytes;
import static com.facebook.presto.sql.planner.LiteralEncoder.isSupportedLiteralType;
import static com.facebook.presto.sql.planner.iterative.rule.CanonicalizeExpressionRewriter.canonicalizeExpression;
import static com.facebook.presto.sql.relational.Expressions.variable;
import static com.facebook.presto.sql.relational.SqlFunctionUtils.getSqlFunctionExpression;
import static com.facebook.presto.type.LikeFunctions.isLikePattern;
import static com.facebook.presto.type.LikeFunctions.unescapeLiteralLikePattern;
import static com.facebook.presto.util.LegacyRowFieldOrdinalAccessUtil.parseAnonymousRowFieldOrdinalAccess;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@Deprecated
public class ExpressionInterpreter
{
    private static final long MAX_SERIALIZABLE_OBJECT_SIZE = 1000;
    private final Expression expression;
    private final Metadata metadata;
    private final LiteralEncoder literalEncoder;
    private final Session session;
    private final ConnectorSession connectorSession;
    // if optimize flag is on, we will ways return evaluated result or throw.
    private final boolean optimize;
    private final Map<NodeRef<Expression>, Type> expressionTypes;
    private final InterpretedFunctionInvoker functionInvoker;
    private final boolean legacyRowFieldOrdinalAccess;

    private final Visitor visitor;

    // identity-based cache for LIKE expressions with constant pattern and escape char
    private final IdentityHashMap<LikePredicate, Regex> likePatternCache = new IdentityHashMap<>();
    private final IdentityHashMap<InListExpression, Set<?>> inListCache = new IdentityHashMap<>();

    public static ExpressionInterpreter expressionInterpreter(Expression expression, Metadata metadata, Session session, Map<NodeRef<Expression>, Type> expressionTypes)
    {
        return new ExpressionInterpreter(expression, metadata, session, expressionTypes, false);
    }

    public static ExpressionInterpreter expressionOptimizer(Expression expression, Metadata metadata, Session session, Map<NodeRef<Expression>, Type> expressionTypes)
    {
        requireNonNull(expression, "expression is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(session, "session is null");

        return new ExpressionInterpreter(expression, metadata, session, expressionTypes, true);
    }

    public static Object evaluateConstantExpression(Expression expression, Type expectedType, Metadata metadata, Session session, List<Expression> parameters)
    {
        ExpressionAnalyzer analyzer = createConstantAnalyzer(metadata, session, parameters, WarningCollector.NOOP);
        analyzer.analyze(expression, Scope.create());

        Type actualType = analyzer.getExpressionTypes().get(NodeRef.of(expression));
        if (!metadata.getFunctionAndTypeManager().canCoerce(actualType, expectedType)) {
            throw new SemanticException(SemanticErrorCode.TYPE_MISMATCH, expression, format("Cannot cast type %s to %s",
                    actualType.getTypeSignature(),
                    expectedType.getTypeSignature()));
        }

        Map<NodeRef<Expression>, Type> coercions = ImmutableMap.<NodeRef<Expression>, Type>builder()
                .putAll(analyzer.getExpressionCoercions())
                .put(NodeRef.of(expression), expectedType)
                .build();
        return evaluateConstantExpression(expression, coercions, analyzer.getTypeOnlyCoercions(), metadata, session, ImmutableSet.of(), parameters);
    }

    private static Object evaluateConstantExpression(
            Expression expression,
            Map<NodeRef<Expression>, Type> coercions,
            Set<NodeRef<Expression>> typeOnlyCoercions,
            Metadata metadata,
            Session session,
            Set<NodeRef<Expression>> columnReferences,
            List<Expression> parameters)
    {
        requireNonNull(columnReferences, "columnReferences is null");

        verifyExpressionIsConstant(columnReferences, expression);

        // add coercions
        Expression rewrite = Coercer.addCoercions(expression, coercions, typeOnlyCoercions);

        // redo the analysis since above expression rewriter might create new expressions which do not have entries in the type map
        ExpressionAnalyzer analyzer = createConstantAnalyzer(metadata, session, parameters, WarningCollector.NOOP);
        analyzer.analyze(rewrite, Scope.create());

        // remove syntax sugar
        rewrite = DesugarAtTimeZoneRewriter.rewrite(rewrite, analyzer.getExpressionTypes());

        // expressionInterpreter/optimizer only understands a subset of expression types
        // TODO: remove this when the new expression tree is implemented
        Expression canonicalized = canonicalizeExpression(rewrite);

        // The optimization above may have rewritten the expression tree which breaks all the identity maps, so redo the analysis
        // to re-analyze coercions that might be necessary
        analyzer = createConstantAnalyzer(metadata, session, parameters, WarningCollector.NOOP);
        analyzer.analyze(canonicalized, Scope.create());

        // evaluate the expression
        Object result = expressionInterpreter(canonicalized, metadata, session, analyzer.getExpressionTypes()).evaluate();
        verify(!(result instanceof Expression), "Expression interpreter returned an unresolved expression");
        return result;
    }

    private ExpressionInterpreter(Expression expression, Metadata metadata, Session session, Map<NodeRef<Expression>, Type> expressionTypes, boolean optimize)
    {
        this.expression = requireNonNull(expression, "expression is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.literalEncoder = new LiteralEncoder(metadata.getBlockEncodingSerde());
        this.session = requireNonNull(session, "session is null");
        this.connectorSession = session.toConnectorSession();
        this.expressionTypes = ImmutableMap.copyOf(requireNonNull(expressionTypes, "expressionTypes is null"));
        verify((expressionTypes.containsKey(NodeRef.of(expression))));
        this.optimize = optimize;
        this.functionInvoker = new InterpretedFunctionInvoker(metadata.getFunctionAndTypeManager());
        this.legacyRowFieldOrdinalAccess = isLegacyRowFieldOrdinalAccessEnabled(session);

        this.visitor = new Visitor();
    }

    public Type getType()
    {
        return expressionTypes.get(NodeRef.of(expression));
    }

    public Object evaluate()
    {
        checkState(!optimize, "evaluate() not allowed for optimizer");
        return visitor.process(expression, null);
    }

    public Object evaluate(VariableResolver inputs)
    {
        checkState(!optimize, "evaluate(SymbolResolver) not allowed for optimizer");
        return visitor.process(expression, inputs);
    }

    public Object optimize(VariableResolver inputs)
    {
        checkState(optimize, "evaluate(SymbolResolver) not allowed for interpreter");
        return visitor.process(expression, inputs);
    }

    @SuppressWarnings("FloatingPointEquality")
    private class Visitor
            extends AstVisitor<Object, Object>
    {
        private final Map<NodeRef<Expression>, Type> generatedExpressionTypes = new HashMap<>();

        @Override
        public Object visitFieldReference(FieldReference node, Object context)
        {
            throw new UnsupportedOperationException("Field references not supported in interpreter");
        }

        @Override
        protected Object visitDereferenceExpression(DereferenceExpression node, Object context)
        {
            Type type = type(node.getBase());
            // if there is no type for the base of Dereference, it must be QualifiedName
            if (type == null) {
                return node;
            }

            Type returnType = type(node);
            if (isEnumType(type) && isEnumType(returnType)) {
                return resolveEnumLiteral(node, returnType);
            }

            Object base = process(node.getBase(), context);
            // if the base part is evaluated to be null, the dereference expression should also be null
            if (base == null) {
                return null;
            }

            if (hasUnresolvedValue(base)) {
                return new DereferenceExpression(toExpression(base, type), node.getField());
            }

            RowType rowType = (RowType) type;
            String fieldName = node.getField().getValue();
            List<Field> fields = rowType.getFields();
            int index = -1;
            for (int i = 0; i < fields.size(); i++) {
                Field field = fields.get(i);
                if (field.getName().isPresent() && field.getName().get().equalsIgnoreCase(fieldName)) {
                    checkArgument(index < 0, "Ambiguous field %s in type %s", field, rowType.getDisplayName());
                    index = i;
                }
            }

            if (legacyRowFieldOrdinalAccess && index < 0) {
                OptionalInt rowIndex = parseAnonymousRowFieldOrdinalAccess(fieldName, fields);
                if (rowIndex.isPresent()) {
                    index = rowIndex.getAsInt();
                }
            }

            return interpretDereference(base, returnType, index);
        }

        @Override
        protected Object visitIdentifier(Identifier node, Object context)
        {
            // Identifier only exists before planning.
            // ExpressionInterpreter should only be invoked after planning.
            // As a result, this method should be unreachable.
            // However, RelationPlanner.visitUnnest and visitValues invokes evaluateConstantExpression.
            return ((VariableResolver) context).getValue(variable(node.getValue(), type(node)));
        }

        @Override
        protected Object visitParameter(Parameter node, Object context)
        {
            return node;
        }

        @Override
        protected Object visitSymbolReference(SymbolReference node, Object context)
        {
            return ((VariableResolver) context).getValue(variable(node.getName(), type(node)));
        }

        @Override
        protected Object visitLiteral(Literal node, Object context)
        {
            return LiteralInterpreter.evaluate(metadata, connectorSession, node);
        }

        @Override
        protected Object visitIsNullPredicate(IsNullPredicate node, Object context)
        {
            Object value = process(node.getValue(), context);

            if (value instanceof Expression) {
                return new IsNullPredicate(toExpression(value, type(node.getValue())));
            }

            return value == null;
        }

        @Override
        protected Object visitIsNotNullPredicate(IsNotNullPredicate node, Object context)
        {
            Object value = process(node.getValue(), context);

            if (value instanceof Expression) {
                return new IsNotNullPredicate(toExpression(value, type(node.getValue())));
            }

            return value != null;
        }

        @Override
        protected Object visitSearchedCaseExpression(SearchedCaseExpression node, Object context)
        {
            Object defaultResult = processWithExceptionHandling(node.getDefaultValue().orElse(null), context);

            List<WhenClause> whenClauses = new ArrayList<>();
            for (WhenClause whenClause : node.getWhenClauses()) {
                Object whenOperand = processWithExceptionHandling(whenClause.getOperand(), context);
                Object result = processWithExceptionHandling(whenClause.getResult(), context);

                if (whenOperand instanceof Expression) {
                    // cannot fully evaluate, add updated whenClause
                    whenClauses.add(new WhenClause(
                            toExpression(whenOperand, type(whenClause.getOperand())),
                            toExpression(result, type(whenClause.getResult()))));
                }
                else if (Boolean.TRUE.equals(whenOperand)) {
                    // condition is true, use this as defaultResult
                    defaultResult = result;
                    break;
                }
            }

            if (whenClauses.isEmpty()) {
                return defaultResult;
            }

            Expression resultExpression = (defaultResult == null) ? null : toExpression(defaultResult, type(node));
            return new SearchedCaseExpression(whenClauses, Optional.ofNullable(resultExpression));
        }

        @Override
        protected Object visitIfExpression(IfExpression node, Object context)
        {
            Object trueValue = processWithExceptionHandling(node.getTrueValue(), context);
            Object falseValue = processWithExceptionHandling(node.getFalseValue().orElse(null), context);
            Object condition = processWithExceptionHandling(node.getCondition(), context);

            if (condition instanceof Expression) {
                Expression falseValueExpression = (falseValue == null) ? null : toExpression(falseValue, type(node.getFalseValue().get()));
                return new IfExpression(
                        toExpression(condition, type(node.getCondition())),
                        toExpression(trueValue, type(node.getTrueValue())),
                        falseValueExpression);
            }
            else if (Boolean.TRUE.equals(condition)) {
                return trueValue;
            }
            else {
                return falseValue;
            }
        }

        private Object processWithExceptionHandling(Expression expression, Object context)
        {
            if (expression == null) {
                return null;
            }

            try {
                return process(expression, context);
            }
            catch (RuntimeException e) {
                // HACK
                // Certain operations like 0 / 0 or likeExpression may throw exceptions.
                // Wrap them in a FunctionCall that will throw the exception if the expression is actually executed
                return createFailureFunction(e, type(expression));
            }
        }

        @Override
        protected Object visitSimpleCaseExpression(SimpleCaseExpression node, Object context)
        {
            Object operand = processWithExceptionHandling(node.getOperand(), context);
            Type operandType = type(node.getOperand());

            // evaluate defaultClause
            Expression defaultClause = node.getDefaultValue().orElse(null);
            Object defaultResult = processWithExceptionHandling(defaultClause, context);

            // if operand is null, return defaultValue
            if (operand == null) {
                return defaultResult;
            }

            List<WhenClause> whenClauses = new ArrayList<>();
            for (WhenClause whenClause : node.getWhenClauses()) {
                Object whenOperand = processWithExceptionHandling(whenClause.getOperand(), context);
                Object result = processWithExceptionHandling(whenClause.getResult(), context);

                if (whenOperand instanceof Expression || operand instanceof Expression) {
                    // cannot fully evaluate, add updated whenClause
                    whenClauses.add(new WhenClause(
                            toExpression(whenOperand, type(whenClause.getOperand())),
                            toExpression(result, type(whenClause.getResult()))));
                }
                else if (whenOperand != null && isEqual(operand, operandType, whenOperand, type(whenClause.getOperand()))) {
                    // condition is true, use this as defaultResult
                    defaultResult = result;
                    break;
                }
            }

            if (whenClauses.isEmpty()) {
                return defaultResult;
            }

            Expression defaultExpression = (defaultResult == null) ? null : toExpression(defaultResult, type(node));
            return new SimpleCaseExpression(toExpression(operand, type(node.getOperand())), whenClauses, Optional.ofNullable(defaultExpression));
        }

        private boolean isEqual(Object operand1, Type type1, Object operand2, Type type2)
        {
            return Boolean.TRUE.equals(invokeOperator(OperatorType.EQUAL, ImmutableList.of(type1, type2), ImmutableList.of(operand1, operand2)));
        }

        private void addGeneratedExpressionType(Expression expression, Type type)
        {
            generatedExpressionTypes.put(NodeRef.of(expression), type);
        }

        private Type type(Expression expression)
        {
            Type type = generatedExpressionTypes.get(NodeRef.of(expression));
            if (type != null) {
                return type;
            }
            return expressionTypes.get(NodeRef.of(expression));
        }

        @Override
        protected Object visitCoalesceExpression(CoalesceExpression node, Object context)
        {
            Type type = type(node);
            List<Object> values = node.getOperands().stream()
                    .map(value -> processWithExceptionHandling(value, context))
                    .filter(Objects::nonNull)
                    .flatMap(expression -> {
                        if (expression instanceof CoalesceExpression) {
                            return ((CoalesceExpression) expression).getOperands().stream();
                        }
                        return Stream.of(expression);
                    })
                    .collect(toList());

            if ((!values.isEmpty() && !(values.get(0) instanceof Expression)) || values.size() == 1) {
                return values.get(0);
            }
            ImmutableList.Builder<Expression> operandsBuilder = ImmutableList.builder();
            Set<Expression> visitedExpression = new HashSet<>();
            for (Object value : values) {
                Expression expression = toExpression(value, type);
                if (!isDeterministic(expression) || visitedExpression.add(expression)) {
                    operandsBuilder.add(expression);
                }
                // TODO: Replace this logic with an anlyzer which specifies whether it evaluates to null
                if (expression instanceof Literal && !(expression instanceof NullLiteral)) {
                    break;
                }
            }
            List<Expression> expressions = operandsBuilder.build();

            if (expressions.isEmpty()) {
                return null;
            }

            if (expressions.size() == 1) {
                return getOnlyElement(expressions);
            }
            return new CoalesceExpression(expressions);
        }

        @Override
        protected Object visitInPredicate(InPredicate node, Object context)
        {
            Object value = process(node.getValue(), context);

            Expression valueListExpression = node.getValueList();
            if (!(valueListExpression instanceof InListExpression)) {
                if (!optimize) {
                    throw new UnsupportedOperationException("IN predicate value list type not yet implemented: " + valueListExpression.getClass().getName());
                }
                return node;
            }
            InListExpression valueList = (InListExpression) valueListExpression;
            verify(!valueList.getValues().isEmpty()); // `NULL IN ()` would be false, but is not possible
            if (value == null) {
                return null;
            }

            Set<?> set = inListCache.get(valueList);

            // We use the presence of the node in the map to indicate that we've already done
            // the analysis below. If the value is null, it means that we can't apply the HashSet
            // optimization
            if (!inListCache.containsKey(valueList)) {
                if (valueList.getValues().stream().allMatch(Literal.class::isInstance) &&
                        valueList.getValues().stream().noneMatch(NullLiteral.class::isInstance)) {
                    Set objectSet = valueList.getValues().stream().map(expression -> process(expression, context)).collect(Collectors.toSet());
                    set = FastutilSetHelper.toFastutilHashSet(objectSet, type(node.getValue()), metadata.getFunctionAndTypeManager());
                }
                inListCache.put(valueList, set);
            }

            if (set != null && !(value instanceof Expression)) {
                return set.contains(value);
            }

            boolean hasUnresolvedValue = false;
            if (value instanceof Expression) {
                hasUnresolvedValue = true;
            }

            boolean hasNullValue = false;
            boolean found = false;
            List<Object> values = new ArrayList<>(valueList.getValues().size());
            List<Type> types = new ArrayList<>(valueList.getValues().size());
            for (Expression expression : valueList.getValues()) {
                Object inValue = process(expression, context);
                if (value instanceof Expression || inValue instanceof Expression) {
                    hasUnresolvedValue = true;
                    values.add(inValue);
                    types.add(type(expression));
                    continue;
                }

                if (inValue == null) {
                    hasNullValue = true;
                }
                else {
                    Boolean result = (Boolean) invokeOperator(OperatorType.EQUAL, types(node.getValue(), expression), ImmutableList.of(value, inValue));
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
                Type type = type(node.getValue());
                List<Expression> expressionValues = toExpressions(values, types);
                List<Expression> simplifiedExpressionValues = Stream.concat(
                        expressionValues.stream()
                                .filter(ExpressionDeterminismEvaluator::isDeterministic)
                                .distinct(),
                        expressionValues.stream()
                                .filter((expression -> !isDeterministic(expression))))
                        .collect(toImmutableList());
                return new InPredicate(toExpression(value, type), new InListExpression(simplifiedExpressionValues));
            }
            if (hasNullValue) {
                return null;
            }
            return false;
        }

        @Override
        protected Object visitExists(ExistsPredicate node, Object context)
        {
            if (!optimize) {
                throw new UnsupportedOperationException("Exists subquery not yet implemented");
            }
            return node;
        }

        @Override
        protected Object visitSubqueryExpression(SubqueryExpression node, Object context)
        {
            if (!optimize) {
                throw new UnsupportedOperationException("Subquery not yet implemented");
            }
            return node;
        }

        @Override
        protected Object visitArithmeticUnary(ArithmeticUnaryExpression node, Object context)
        {
            Object value = process(node.getValue(), context);
            if (value == null) {
                return null;
            }
            if (value instanceof Expression) {
                return new ArithmeticUnaryExpression(node.getSign(), toExpression(value, type(node.getValue())));
            }

            switch (node.getSign()) {
                case PLUS:
                    return value;
                case MINUS:
                    FunctionHandle operatorHandle = metadata.getFunctionAndTypeManager().resolveOperator(OperatorType.NEGATION, fromTypes(types(node.getValue())));
                    MethodHandle handle = metadata.getFunctionAndTypeManager().getJavaScalarFunctionImplementation(operatorHandle).getMethodHandle();

                    if (handle.type().parameterCount() > 0 && handle.type().parameterType(0) == SqlFunctionProperties.class) {
                        handle = handle.bindTo(connectorSession.getSqlFunctionProperties());
                    }
                    try {
                        return handle.invokeWithArguments(value);
                    }
                    catch (Throwable throwable) {
                        throwIfInstanceOf(throwable, RuntimeException.class);
                        throwIfInstanceOf(throwable, Error.class);
                        throw new RuntimeException(throwable.getMessage(), throwable);
                    }
            }

            throw new UnsupportedOperationException("Unsupported unary operator: " + node.getSign());
        }

        @Override
        protected Object visitArithmeticBinary(ArithmeticBinaryExpression node, Object context)
        {
            Object left = process(node.getLeft(), context);
            if (left == null) {
                return null;
            }
            Object right = process(node.getRight(), context);
            if (right == null) {
                return null;
            }

            if (hasUnresolvedValue(left, right)) {
                return new ArithmeticBinaryExpression(node.getOperator(), toExpression(left, type(node.getLeft())), toExpression(right, type(node.getRight())));
            }

            return invokeOperator(OperatorType.valueOf(node.getOperator().name()), types(node.getLeft(), node.getRight()), ImmutableList.of(left, right));
        }

        @Override
        protected Object visitComparisonExpression(ComparisonExpression node, Object context)
        {
            ComparisonExpression.Operator operator = node.getOperator();

            Object left = process(node.getLeft(), context);
            if (left == null && operator != ComparisonExpression.Operator.IS_DISTINCT_FROM) {
                return null;
            }

            Object right = process(node.getRight(), context);
            if (operator == ComparisonExpression.Operator.IS_DISTINCT_FROM) {
                if (left == null && right == null) {
                    return false;
                }
                else if (left == null && !hasUnresolvedValue(right)) {
                    return true;
                }
                else if (right == null && !hasUnresolvedValue(left)) {
                    return true;
                }
            }
            else if (right == null) {
                return null;
            }

            if (hasUnresolvedValue(left, right)) {
                return new ComparisonExpression(operator, toExpression(left, type(node.getLeft())), toExpression(right, type(node.getRight())));
            }

            return invokeOperator(OperatorType.valueOf(operator.name()), types(node.getLeft(), node.getRight()), ImmutableList.of(left, right));
        }

        @Override
        protected Object visitBetweenPredicate(BetweenPredicate node, Object context)
        {
            Object value = process(node.getValue(), context);
            if (value == null) {
                return null;
            }
            Object min = process(node.getMin(), context);
            if (min == null) {
                return null;
            }
            Object max = process(node.getMax(), context);
            if (max == null) {
                return null;
            }

            if (hasUnresolvedValue(value, min, max)) {
                return new BetweenPredicate(
                        toExpression(value, type(node.getValue())),
                        toExpression(min, type(node.getMin())),
                        toExpression(max, type(node.getMax())));
            }

            return invokeOperator(OperatorType.BETWEEN, types(node.getValue(), node.getMin(), node.getMax()), ImmutableList.of(value, min, max));
        }

        @Override
        protected Object visitNullIfExpression(NullIfExpression node, Object context)
        {
            Object first = process(node.getFirst(), context);
            if (first == null) {
                return null;
            }
            Object second = process(node.getSecond(), context);
            if (second == null) {
                return first;
            }

            Type firstType = type(node.getFirst());
            Type secondType = type(node.getSecond());

            if (hasUnresolvedValue(first, second)) {
                return new NullIfExpression(toExpression(first, firstType), toExpression(second, secondType));
            }

            FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
            Type commonType = functionAndTypeManager.getCommonSuperType(firstType, secondType).get();

            FunctionHandle firstCast = functionAndTypeManager.lookupCast(CAST, firstType.getTypeSignature(), commonType.getTypeSignature());
            FunctionHandle secondCast = functionAndTypeManager.lookupCast(CAST, secondType.getTypeSignature(), commonType.getTypeSignature());

            // cast(first as <common type>) == cast(second as <common type>)
            boolean equal = Boolean.TRUE.equals(invokeOperator(
                    OperatorType.EQUAL,
                    ImmutableList.of(commonType, commonType),
                    ImmutableList.of(
                            functionInvoker.invoke(firstCast, session.getSqlFunctionProperties(), ImmutableList.of(first)),
                            functionInvoker.invoke(secondCast, session.getSqlFunctionProperties(), ImmutableList.of(second)))));

            if (equal) {
                return null;
            }
            else {
                return first;
            }
        }

        @Override
        protected Object visitNotExpression(NotExpression node, Object context)
        {
            Object value = process(node.getValue(), context);
            if (value == null) {
                return null;
            }

            if (value instanceof Expression) {
                return new NotExpression(toExpression(value, type(node.getValue())));
            }

            return !(Boolean) value;
        }

        @Override
        protected Object visitLogicalBinaryExpression(LogicalBinaryExpression node, Object context)
        {
            Object left = process(node.getLeft(), context);
            Object right;

            switch (node.getOperator()) {
                case AND: {
                    if (Boolean.FALSE.equals(left)) {
                        return false;
                    }

                    right = process(node.getRight(), context);

                    if (Boolean.FALSE.equals(left) || Boolean.TRUE.equals(right)) {
                        return left;
                    }

                    if (Boolean.FALSE.equals(right) || Boolean.TRUE.equals(left)) {
                        return right;
                    }
                    break;
                }
                case OR: {
                    if (Boolean.TRUE.equals(left)) {
                        return true;
                    }

                    right = process(node.getRight(), context);

                    if (Boolean.TRUE.equals(left) || Boolean.FALSE.equals(right)) {
                        return left;
                    }

                    if (Boolean.TRUE.equals(right) || Boolean.FALSE.equals(left)) {
                        return right;
                    }
                    break;
                }
                default:
                    throw new IllegalStateException("Unknown LogicalBinaryExpression#Type");
            }

            if (left == null && right == null) {
                return null;
            }

            return new LogicalBinaryExpression(node.getOperator(),
                    toExpression(left, type(node.getLeft())),
                    toExpression(right, type(node.getRight())));
        }

        @Override
        protected Object visitBooleanLiteral(BooleanLiteral node, Object context)
        {
            return node.equals(BooleanLiteral.TRUE_LITERAL);
        }

        @Override
        protected Object visitFunctionCall(FunctionCall node, Object context)
        {
            List<Type> argumentTypes = new ArrayList<>();
            List<Object> argumentValues = new ArrayList<>();
            for (Expression expression : node.getArguments()) {
                Object value = process(expression, context);
                Type type = type(expression);
                argumentValues.add(value);
                argumentTypes.add(type);
            }
            FunctionHandle functionHandle = metadata.getFunctionAndTypeManager().resolveFunction(
                    Optional.of(session.getSessionFunctions()),
                    session.getTransactionId(),
                    qualifyObjectName(node.getName()),
                    fromTypes(argumentTypes));
            FunctionMetadata functionMetadata = metadata.getFunctionAndTypeManager().getFunctionMetadata(functionHandle);
            if (!functionMetadata.isCalledOnNullInput()) {
                for (int i = 0; i < argumentValues.size(); i++) {
                    Object value = argumentValues.get(i);
                    if (value == null) {
                        return null;
                    }
                }
            }

            // do not optimize non-deterministic functions
            if (optimize &&
                    (!functionMetadata.isDeterministic() ||
                            hasUnresolvedValue(argumentValues) ||
                            node.getName().equals(QualifiedName.of(DynamicFilters.DynamicFilterPlaceholderFunction.NAME)) ||
                            node.getName().equals(QualifiedName.of("fail")))) {
                return new FunctionCall(node.getName(), node.getWindow(), node.isDistinct(), node.isIgnoreNulls(), toExpressions(argumentValues, argumentTypes));
            }

            Object result;

            FunctionImplementationType implementationType = functionMetadata.getImplementationType();
            if (implementationType.isExternal()) {
                // do not interpret remote functions on coordinator
                return new FunctionCall(node.getName(), node.getWindow(), node.isDistinct(), node.isIgnoreNulls(), toExpressions(argumentValues, argumentTypes));
            }
            else if (implementationType.equals(JAVA)) {
                result = functionInvoker.invoke(functionHandle, session.getSqlFunctionProperties(), argumentValues);
            }
            else {
                checkState(implementationType.equals(SQL));
                Expression function = getSqlFunctionExpression(functionMetadata, (SqlInvokedScalarFunctionImplementation) metadata.getFunctionAndTypeManager().getScalarFunctionImplementation(functionHandle), metadata, new PlanVariableAllocator(), session.getSqlFunctionProperties(), node.getArguments());
                ExpressionInterpreter functionInterpreter = new ExpressionInterpreter(
                        function,
                        metadata,
                        session,
                        getExpressionTypes(session, metadata, new SqlParser(), TypeProvider.empty(), function, emptyList(), WarningCollector.NOOP),
                        optimize);
                result = functionInterpreter.visitor.process(function, context);
                if (result instanceof FunctionCall) {
                    // Cannot interpret function to constant
                    return new FunctionCall(node.getName(), node.getWindow(), node.isDistinct(), node.isIgnoreNulls(), toExpressions(argumentValues, argumentTypes));
                }
            }

            if (optimize && !isSerializable(result, type(node))) {
                return new FunctionCall(node.getName(), node.getWindow(), node.isDistinct(), node.isIgnoreNulls(), toExpressions(argumentValues, argumentTypes));
            }
            return result;
        }

        @Override
        protected Object visitLambdaExpression(LambdaExpression node, Object context)
        {
            if (optimize) {
                // TODO: enable optimization related to lambda expression
                // A mechanism to convert function type back into lambda expression need to exist to enable optimization
                return node;
            }

            Expression body = node.getBody();
            List<String> argumentNames = node.getArguments().stream()
                    .map(LambdaArgumentDeclaration::getName)
                    .map(Identifier::getValue)
                    .collect(toImmutableList());
            FunctionType functionType = (FunctionType) expressionTypes.get(NodeRef.<Expression>of(node));
            checkArgument(argumentNames.size() == functionType.getArgumentTypes().size());

            return generateVarArgsToMapAdapter(
                    Primitives.wrap(functionType.getReturnType().getJavaType()),
                    functionType.getArgumentTypes().stream()
                            .map(Type::getJavaType)
                            .map(Primitives::wrap)
                            .collect(toImmutableList()),
                    argumentNames,
                    map -> process(body, new LambdaVariableResolver(map)));
        }

        @Override
        protected Object visitBindExpression(BindExpression node, Object context)
        {
            List<Object> values = node.getValues().stream()
                    .map(value -> process(value, context))
                    .collect(toList());
            Object function = process(node.getFunction(), context);

            if (hasUnresolvedValue(values) || hasUnresolvedValue(function)) {
                ImmutableList.Builder<Expression> builder = ImmutableList.builder();
                for (int i = 0; i < values.size(); i++) {
                    builder.add(toExpression(values.get(i), type(node.getValues().get(i))));
                }

                return new BindExpression(
                        builder.build(),
                        toExpression(function, type(node.getFunction())));
            }

            return MethodHandles.insertArguments((MethodHandle) function, 0, values.toArray());
        }

        @Override
        protected Object visitLikePredicate(LikePredicate node, Object context)
        {
            Object value = process(node.getValue(), context);

            if (value == null) {
                return null;
            }

            if (value instanceof Slice &&
                    node.getPattern() instanceof StringLiteral &&
                    (!node.getEscape().isPresent() || node.getEscape().get() instanceof StringLiteral)) {
                // fast path when we know the pattern and escape are constant
                return interpretLikePredicate(type(node.getValue()), (Slice) value, getConstantPattern(node));
            }

            Object pattern = process(node.getPattern(), context);

            if (pattern == null) {
                return null;
            }

            Object escape = null;
            if (node.getEscape().isPresent()) {
                escape = process(node.getEscape().get(), context);

                if (escape == null) {
                    return null;
                }
            }

            if (value instanceof Slice &&
                    pattern instanceof Slice &&
                    (escape == null || escape instanceof Slice)) {
                Regex regex;
                if (escape == null) {
                    regex = LikeFunctions.likePattern((Slice) pattern);
                }
                else {
                    regex = LikeFunctions.likePattern((Slice) pattern, (Slice) escape);
                }

                return interpretLikePredicate(type(node.getValue()), (Slice) value, regex);
            }

            // if pattern is a constant without % or _ replace with a comparison
            if (pattern instanceof Slice && (escape == null || escape instanceof Slice) && !isLikePattern((Slice) pattern, (Slice) escape)) {
                Slice unescapedPattern = unescapeLiteralLikePattern((Slice) pattern, (Slice) escape);
                Type valueType = type(node.getValue());
                Type patternType = createVarcharType(unescapedPattern.length());
                FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
                Optional<Type> commonSuperType = functionAndTypeManager.getCommonSuperType(valueType, patternType);
                checkArgument(commonSuperType.isPresent(), "Missing super type when optimizing %s", node);
                Expression valueExpression = toExpression(value, valueType);
                Expression patternExpression = toExpression(unescapedPattern, patternType);
                Type superType = commonSuperType.get();
                if (!valueType.equals(superType)) {
                    valueExpression = new Cast(valueExpression, superType.getTypeSignature().toString(), false, functionAndTypeManager.isTypeOnlyCoercion(valueType, superType));
                }
                if (!patternType.equals(superType)) {
                    patternExpression = new Cast(patternExpression, superType.getTypeSignature().toString(), false, functionAndTypeManager.isTypeOnlyCoercion(patternType, superType));
                }
                return new ComparisonExpression(ComparisonExpression.Operator.EQUAL, valueExpression, patternExpression);
            }

            Optional<Expression> optimizedEscape = Optional.empty();
            if (node.getEscape().isPresent()) {
                optimizedEscape = Optional.of(toExpression(escape, type(node.getEscape().get())));
            }

            return new LikePredicate(
                    toExpression(value, type(node.getValue())),
                    toExpression(pattern, type(node.getPattern())),
                    optimizedEscape);
        }

        private Regex getConstantPattern(LikePredicate node)
        {
            Regex result = likePatternCache.get(node);

            if (result == null) {
                StringLiteral pattern = (StringLiteral) node.getPattern();

                if (node.getEscape().isPresent()) {
                    Slice escape = ((StringLiteral) node.getEscape().get()).getSlice();
                    result = LikeFunctions.likePattern(pattern.getSlice(), escape);
                }
                else {
                    result = LikeFunctions.likePattern(pattern.getSlice());
                }

                likePatternCache.put(node, result);
            }

            return result;
        }

        @Override
        public Object visitCast(Cast node, Object context)
        {
            Object value = process(node.getExpression(), context);
            Type targetType = metadata.getType(parseTypeSignature(node.getType()));
            if (targetType == null) {
                throw new IllegalArgumentException("Unsupported type: " + node.getType());
            }

            if (value == null) {
                return null;
            }

            Type sourceType = type(node.getExpression());
            if (value instanceof Expression) {
                if (targetType.equals(sourceType)) {
                    return value;
                }

                return new Cast((Expression) value, node.getType(), node.isSafe(), node.isTypeOnly());
            }

            if (node.isTypeOnly()) {
                return value;
            }

            if (optimize && !isSupportedLiteralType(type(node))) {
                // do not invoke cast function if it can produce unserializable type
                return new Cast(toExpression(value, sourceType), node.getType(), node.isSafe(), node.isTypeOnly());
            }

            FunctionHandle operator = metadata.getFunctionAndTypeManager().lookupCast(CAST, sourceType.getTypeSignature(), targetType.getTypeSignature());

            try {
                Object castedValue = functionInvoker.invoke(operator, session.getSqlFunctionProperties(), ImmutableList.of(value));
                if (optimize && !isSerializable(castedValue, type(node))) {
                    return new Cast(toExpression(value, sourceType), node.getType(), node.isSafe(), node.isTypeOnly());
                }
                return castedValue;
            }
            catch (RuntimeException e) {
                if (node.isSafe()) {
                    return null;
                }
                throw e;
            }
        }

        @Override
        protected Object visitArrayConstructor(ArrayConstructor node, Object context)
        {
            Type elementType = ((ArrayType) type(node)).getElementType();
            BlockBuilder arrayBlockBuilder = elementType.createBlockBuilder(null, node.getValues().size());

            for (Expression expression : node.getValues()) {
                Object value = process(expression, context);
                if (value instanceof Expression) {
                    FunctionCall functionCall = new FunctionCall(QualifiedName.of(ArrayConstructor.ARRAY_CONSTRUCTOR), node.getValues());
                    addGeneratedExpressionType(functionCall, type(node));
                    return visitFunctionCall(functionCall, context);
                }
                writeNativeValue(elementType, arrayBlockBuilder, value);
            }

            return arrayBlockBuilder.build();
        }

        @Override
        protected Object visitCurrentUser(CurrentUser node, Object context)
        {
            FunctionCall functionCall = DesugarCurrentUser.getCall(node);
            addGeneratedExpressionType(functionCall, type(node));
            return visitFunctionCall(functionCall, context);
        }

        @Override
        protected Object visitRow(Row node, Object context)
        {
            RowType rowType = (RowType) type(node);
            List<Type> parameterTypes = rowType.getTypeParameters();
            List<Expression> arguments = node.getItems();

            int cardinality = arguments.size();
            List<Object> values = new ArrayList<>(cardinality);
            for (Expression argument : arguments) {
                values.add(process(argument, context));
            }
            if (hasUnresolvedValue(values)) {
                return new Row(toExpressions(values, parameterTypes));
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

        @Override
        protected Object visitSubscriptExpression(SubscriptExpression node, Object context)
        {
            Object base = process(node.getBase(), context);
            if (base == null) {
                return null;
            }
            Object index = process(node.getIndex(), context);
            if (index == null) {
                return null;
            }
            if ((index instanceof Long) && isArray(type(node.getBase()))) {
                ArraySubscriptOperator.checkArrayIndex((Long) index);
            }

            if (hasUnresolvedValue(base, index)) {
                return new SubscriptExpression(toExpression(base, type(node.getBase())), toExpression(index, type(node.getIndex())));
            }

            // Subscript on Row hasn't got a dedicated operator. It is interpreted by hand.
            if (base instanceof SingleRowBlock) {
                SingleRowBlock row = (SingleRowBlock) base;
                int position = toIntExact((long) index - 1);
                if (position < 0 || position >= row.getPositionCount()) {
                    throw new PrestoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "ROW index out of bounds: " + (position + 1));
                }
                Type returnType = type(node.getBase()).getTypeParameters().get(position);
                return TypeUtils.readNativeValue(returnType, row, position);
            }

            // Subscript on Array or Map is interpreted using operator.
            return invokeOperator(OperatorType.SUBSCRIPT, types(node.getBase(), node.getIndex()), ImmutableList.of(base, index));
        }

        @Override
        protected Object visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, Object context)
        {
            if (!optimize) {
                throw new UnsupportedOperationException("QuantifiedComparison not yet implemented");
            }
            return node;
        }

        @Override
        protected Object visitExpression(Expression node, Object context)
        {
            throw new PrestoException(NOT_SUPPORTED, "not yet implemented: " + node.getClass().getName());
        }

        @Override
        protected Object visitNode(Node node, Object context)
        {
            throw new UnsupportedOperationException("Evaluator visitor can only handle Expression nodes");
        }

        private List<Type> types(Expression... types)
        {
            return Stream.of(types)
                    .map(NodeRef::of)
                    .map(expressionTypes::get)
                    .collect(toImmutableList());
        }

        private boolean hasUnresolvedValue(Object... values)
        {
            ArrayList<Object> valuesList = new ArrayList<>(values.length);
            for (Object value : values) {
                if (value != null) {
                    valuesList.add(value);
                }
            }
            return !valuesList.isEmpty() && hasUnresolvedValue(valuesList);
        }

        private boolean hasUnresolvedValue(List<Object> values)
        {
            return values.stream().anyMatch(instanceOf(Expression.class)::apply);
        }

        private Object invokeOperator(OperatorType operatorType, List<? extends Type> argumentTypes, List<Object> argumentValues)
        {
            FunctionHandle operatorHandle = metadata.getFunctionAndTypeManager().resolveOperator(operatorType, fromTypes(argumentTypes));
            return functionInvoker.invoke(operatorHandle, session.getSqlFunctionProperties(), argumentValues);
        }

        private Expression toExpression(Object base, Type type)
        {
            return literalEncoder.toExpression(base, type);
        }

        private boolean isSerializable(Object value, Type type)
        {
            requireNonNull(type, "type is null");
            // If value is already Expression, literal values contained inside should already have been made serializable. Otherwise, we make sure the object is small and serializable.
            return value instanceof Expression || (isSupportedLiteralType(type) && estimatedSizeInBytes(value) <= MAX_SERIALIZABLE_OBJECT_SIZE);
        }

        private List<Expression> toExpressions(List<Object> values, List<Type> types)
        {
            return literalEncoder.toExpressions(values, types);
        }
    }

    private Expression createFailureFunction(RuntimeException exception, Type type)
    {
        requireNonNull(exception, "Exception is null");

        String failureInfo = JsonCodec.jsonCodec(FailureInfo.class).toJson(Failures.toFailure(exception).toFailureInfo());
        if (exception instanceof PrestoException) {
            long errorCode = ((PrestoException) exception).getErrorCode().getCode();
            FunctionCall jsonParse = new FunctionCall(QualifiedName.of("json_parse"), ImmutableList.of(new StringLiteral(failureInfo)));
            FunctionCall failureFunction = new FunctionCall(QualifiedName.of("fail"), ImmutableList.of(literalEncoder.toExpression(errorCode, INTEGER), jsonParse));

            return new Cast(failureFunction, type.getTypeSignature().toString());
        }
        FunctionCall jsonParse = new FunctionCall(QualifiedName.of("json_parse"), ImmutableList.of(new StringLiteral(failureInfo)));
        FunctionCall failureFunction = new FunctionCall(QualifiedName.of("fail"), ImmutableList.of(jsonParse));

        return new Cast(failureFunction, type.getTypeSignature().toString());
    }

    private static boolean isArray(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.ARRAY);
    }
}
