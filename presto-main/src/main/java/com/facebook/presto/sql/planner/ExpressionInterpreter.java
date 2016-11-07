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

import com.facebook.presto.Session;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.scalar.ArraySubscriptOperator;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.planner.optimizations.CanonicalizeExpressions;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FieldReference;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.QuantifiedComparisonExpression;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.TryExpression;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.LikeFunctions;
import com.facebook.presto.type.RowType;
import com.facebook.presto.type.RowType.RowField;
import com.facebook.presto.util.Failures;
import com.facebook.presto.util.FastutilSetHelper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Defaults;
import com.google.common.base.Functions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.airlift.joni.Regex;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.createConstantAnalyzer;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.EXPRESSION_NOT_CONSTANT;
import static com.facebook.presto.sql.gen.TryCodeGenerator.tryExpressionExceptionHandler;
import static com.facebook.presto.sql.planner.LiteralInterpreter.toExpression;
import static com.facebook.presto.sql.planner.LiteralInterpreter.toExpressions;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.any;
import static java.util.Objects.requireNonNull;

public class ExpressionInterpreter
{
    private final Expression expression;
    private final Metadata metadata;
    private final ConnectorSession session;
    private final boolean optimize;
    private final IdentityHashMap<Expression, Type> expressionTypes;

    private final Visitor visitor;

    // identity-based cache for LIKE expressions with constant pattern and escape char
    private final IdentityHashMap<LikePredicate, Regex> likePatternCache = new IdentityHashMap<>();
    private final IdentityHashMap<InListExpression, Set<?>> inListCache = new IdentityHashMap<>();

    public static ExpressionInterpreter expressionInterpreter(Expression expression, Metadata metadata, Session session, IdentityHashMap<Expression, Type> expressionTypes)
    {
        requireNonNull(expression, "expression is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(session, "session is null");

        return new ExpressionInterpreter(expression, metadata, session, expressionTypes, false);
    }

    public static ExpressionInterpreter expressionOptimizer(Expression expression, Metadata metadata, Session session, IdentityHashMap<Expression, Type> expressionTypes)
    {
        requireNonNull(expression, "expression is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(session, "session is null");

        return new ExpressionInterpreter(expression, metadata, session, expressionTypes, true);
    }

    public static Object evaluateConstantExpression(Expression expression, Type expectedType, Metadata metadata, Session session, List<Expression> parameters)
    {
        ExpressionAnalyzer analyzer = createConstantAnalyzer(metadata, session, parameters);
        analyzer.analyze(expression, Scope.builder().build());

        Type actualType = analyzer.getExpressionTypes().get(expression);
        if (!metadata.getTypeManager().canCoerce(actualType, expectedType)) {
            throw new SemanticException(SemanticErrorCode.TYPE_MISMATCH, expression, String.format("Cannot cast type %s to %s",
                    expectedType.getTypeSignature(),
                    actualType.getTypeSignature()));
        }

        IdentityHashMap<Expression, Type> coercions = new IdentityHashMap<>();
        coercions.putAll(analyzer.getExpressionCoercions());
        coercions.put(expression, expectedType);
        return evaluateConstantExpression(expression, coercions, metadata, session, ImmutableSet.of(), parameters);
    }

    public static Object evaluateConstantExpression(
            Expression expression,
            IdentityHashMap<Expression, Type> coercions,
            Metadata metadata, Session session,
            Set<Expression> columnReferences,
            List<Expression> parameters)
    {
        requireNonNull(columnReferences, "columnReferences is null");

        verifyExpressionIsConstant(columnReferences, expression);

        // add coercions
        Expression rewrite = ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Expression rewrittenExpression = treeRewriter.defaultRewrite(node, context);

                // cast expression if coercion is registered
                Type coerceToType = coercions.get(node);

                if (coerceToType != null) {
                    rewrittenExpression = new Cast(rewrittenExpression, coerceToType.getTypeSignature().toString());
                }

                return rewrittenExpression;
            }
        }, expression);

        // redo the analysis since above expression rewriter might create new expressions which do not have entries in the type map
        ExpressionAnalyzer analyzer = createConstantAnalyzer(metadata, session, parameters);
        analyzer.analyze(rewrite, Scope.builder().build());

        // remove syntax sugar
        rewrite = ExpressionTreeRewriter.rewriteWith(new DesugaringRewriter(analyzer.getExpressionTypes()), rewrite);

        // expressionInterpreter/optimizer only understands a subset of expression types
        // TODO: remove this when the new expression tree is implemented
        Expression canonicalized = CanonicalizeExpressions.canonicalizeExpression(rewrite);

        // The optimization above may have rewritten the expression tree which breaks all the identity maps, so redo the analysis
        // to re-analyze coercions that might be necessary
        analyzer = createConstantAnalyzer(metadata, session, parameters);
        analyzer.analyze(canonicalized, Scope.builder().build());

        // evaluate the expression
        Object result = expressionInterpreter(canonicalized, metadata, session, analyzer.getExpressionTypes()).evaluate(0);
        verify(!(result instanceof Expression), "Expression interpreter returned an unresolved expression");
        return result;
    }

    public static void verifyExpressionIsConstant(Set<Expression> columnReferences, Expression expression)
    {
        new ConstantExpressionVerifierVisitor(columnReferences, expression).process(expression, null);
    }

    private ExpressionInterpreter(Expression expression, Metadata metadata, Session session, IdentityHashMap<Expression, Type> expressionTypes, boolean optimize)
    {
        this.expression = expression;
        this.metadata = metadata;
        this.session = session.toConnectorSession();
        this.expressionTypes = expressionTypes;
        this.optimize = optimize;

        this.visitor = new Visitor();
    }

    public Object evaluate(RecordCursor inputs)
    {
        checkState(!optimize, "evaluate(RecordCursor) not allowed for optimizer");
        return visitor.process(expression, inputs);
    }

    public Object evaluate(int position, Block... inputs)
    {
        checkState(!optimize, "evaluate(int, Block...) not allowed for optimizer");
        return visitor.process(expression, new SinglePagePositionContext(position, inputs));
    }

    public Object evaluate(int leftPosition, Block[] leftBlocks, int rightPosition, Block[] rightBlocks)
    {
        checkState(!optimize, "evaluate(int, Block[], int, Block[]) not allowed for optimizer");
        return visitor.process(expression, new TwoPagesPositionContext(leftPosition, leftBlocks, rightPosition, rightBlocks));
    }

    public Object optimize(SymbolResolver inputs)
    {
        checkState(optimize, "evaluate(SymbolResolver) not allowed for interpreter");
        return visitor.process(expression, inputs);
    }

    private static class ConstantExpressionVerifierVisitor
            extends DefaultTraversalVisitor<Void, Void>
    {
        private final Set<Expression> columnReferences;
        private final Expression expression;

        public ConstantExpressionVerifierVisitor(Set<Expression> columnReferences, Expression expression)
        {
            this.columnReferences = columnReferences;
            this.expression = expression;
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            if (columnReferences.contains(node)) {
                throw new SemanticException(EXPRESSION_NOT_CONSTANT, expression, "Constant expression cannot contain column references");
            }

            process(node.getBase(), context);
            return null;
        }

        @Override
        protected Void visitQualifiedNameReference(QualifiedNameReference node, Void context)
        {
            throw new SemanticException(EXPRESSION_NOT_CONSTANT, expression, "Constant expression cannot contain column references");
        }

        @Override
        protected Void visitFieldReference(FieldReference node, Void context)
        {
            throw new SemanticException(EXPRESSION_NOT_CONSTANT, expression, "Constant expression cannot contain column references");
        }
    }

    @SuppressWarnings("FloatingPointEquality")
    private class Visitor
            extends AstVisitor<Object, Object>
    {
        @Override
        public Object visitFieldReference(FieldReference node, Object context)
        {
            Type type = expressionTypes.get(node);

            int channel = node.getFieldIndex();
            if (context instanceof PagePositionContext) {
                PagePositionContext pagePositionContext = (PagePositionContext) context;
                int position = pagePositionContext.getPosition(channel);
                Block block = pagePositionContext.getBlock(channel);

                if (block.isNull(position)) {
                    return null;
                }

                Class<?> javaType = type.getJavaType();
                if (javaType == boolean.class) {
                    return type.getBoolean(block, position);
                }
                else if (javaType == long.class) {
                    return type.getLong(block, position);
                }
                else if (javaType == double.class) {
                    return type.getDouble(block, position);
                }
                else if (javaType == Slice.class) {
                    return type.getSlice(block, position);
                }
                else if (javaType == Block.class) {
                    return type.getObject(block, position);
                }
                else {
                    throw new UnsupportedOperationException("not yet implemented");
                }
            }
            else if (context instanceof RecordCursor) {
                RecordCursor cursor = (RecordCursor) context;
                if (cursor.isNull(channel)) {
                    return null;
                }

                Class<?> javaType = type.getJavaType();
                if (javaType == boolean.class) {
                    return cursor.getBoolean(channel);
                }
                else if (javaType == long.class) {
                    return cursor.getLong(channel);
                }
                else if (javaType == double.class) {
                    return cursor.getDouble(channel);
                }
                else if (javaType == Slice.class) {
                    return cursor.getSlice(channel);
                }
                else if (javaType == Block.class) {
                    return cursor.getObject(channel);
                }
                else {
                    throw new UnsupportedOperationException("not yet implemented");
                }
            }
            throw new UnsupportedOperationException("Inputs or cursor myst be set");
        }

        @Override
        protected Object visitDereferenceExpression(DereferenceExpression node, Object context)
        {
            Type type = expressionTypes.get(node.getBase());
            // if there is no type for the base of Dereference, it must be QualifiedName
            if (type == null) {
                return node;
            }

            Object base = process(node.getBase(), context);
            // if the base part is evaluated to be null, the dereference expression should also be null
            if (base == null) {
                return null;
            }

            if (hasUnresolvedValue(base)) {
                return new DereferenceExpression(toExpression(base, type), node.getFieldName());
            }

            RowType rowType = checkType(type, RowType.class, "type");
            Block row = (Block) base;
            Type returnType = expressionTypes.get(node);
            List<RowField> fields = rowType.getFields();
            int index = -1;
            for (int i = 0; i < fields.size(); i++) {
                RowField field = fields.get(i);
                if (field.getName().isPresent() && field.getName().get().equalsIgnoreCase(node.getFieldName())) {
                    checkArgument(index < 0, "Ambiguous field %s in type %s", field, rowType.getDisplayName());
                    index = i;
                }
            }
            checkState(index >= 0, "could not find field name: %s", node.getFieldName());
            if (row.isNull(index)) {
                return null;
            }
            Class<?> javaType = returnType.getJavaType();
            if (javaType == long.class) {
                return returnType.getLong(row, index);
            }
            else if (javaType == double.class) {
                return returnType.getDouble(row, index);
            }
            else if (javaType == boolean.class) {
                return returnType.getBoolean(row, index);
            }
            else if (javaType == Slice.class) {
                return returnType.getSlice(row, index);
            }
            else if (!javaType.isPrimitive()) {
                return returnType.getObject(row, index);
            }
            throw new UnsupportedOperationException("Dereference a unsupported primitive type: " + javaType.getName());
        }

        @Override
        protected Object visitQualifiedNameReference(QualifiedNameReference node, Object context)
        {
            return node;
        }

        @Override
        protected Object visitParameter(Parameter node, Object context)
        {
            return node;
        }

        @Override
        protected Object visitSymbolReference(SymbolReference node, Object context)
        {
            return ((SymbolResolver) context).getValue(Symbol.from(node));
        }

        @Override
        protected Object visitLiteral(Literal node, Object context)
        {
            return LiteralInterpreter.evaluate(metadata, session, node);
        }

        @Override
        protected Object visitIsNullPredicate(IsNullPredicate node, Object context)
        {
            Object value = process(node.getValue(), context);

            if (value instanceof Expression) {
                return new IsNullPredicate(toExpression(value, expressionTypes.get(node.getValue())));
            }

            return value == null;
        }

        @Override
        protected Object visitIsNotNullPredicate(IsNotNullPredicate node, Object context)
        {
            Object value = process(node.getValue(), context);

            if (value instanceof Expression) {
                return new IsNotNullPredicate(toExpression(value, expressionTypes.get(node.getValue())));
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
                        falseValueExpression
                );
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
                // Wrap them a FunctionCall that will throw the exception if the expression is actually executed
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
            return (Boolean) invokeOperator(OperatorType.EQUAL, ImmutableList.of(type1, type2), ImmutableList.of(operand1, operand2));
        }

        private Type type(Expression expression)
        {
            return expressionTypes.get(expression);
        }

        @Override
        protected Object visitCoalesceExpression(CoalesceExpression node, Object context)
        {
            Type type = type(node);
            List<Object> values = node.getOperands().stream()
                    .map(value -> processWithExceptionHandling(value, context))
                    .filter(value -> value != null)
                    .collect(Collectors.toList());

            if ((!values.isEmpty() && !(values.get(0) instanceof Expression)) || values.size() == 1) {
                return values.get(0);
            }

            List<Expression> expressions = values.stream()
                    .map(value -> toExpression(value, type))
                    .collect(Collectors.toList());

            if (expressions.isEmpty()) {
                return null;
            }
            return new CoalesceExpression(expressions);
        }

        @Override
        protected Object visitInPredicate(InPredicate node, Object context)
        {
            Object value = process(node.getValue(), context);
            if (value == null) {
                return null;
            }

            Expression valueListExpression = node.getValueList();
            if (!(valueListExpression instanceof InListExpression)) {
                if (!optimize) {
                    throw new UnsupportedOperationException("IN predicate value list type not yet implemented: " + valueListExpression.getClass().getName());
                }
                return node;
            }
            InListExpression valueList = (InListExpression) valueListExpression;

            Set<?> set = inListCache.get(valueList);

            // We use the presence of the node in the map to indicate that we've already done
            // the analysis below. If the value is null, it means that we can't apply the HashSet
            // optimization
            if (!inListCache.containsKey(valueList)) {
                if (valueList.getValues().stream().allMatch(Literal.class::isInstance) &&
                        valueList.getValues().stream().noneMatch(NullLiteral.class::isInstance)) {
                    Set objectSet = valueList.getValues().stream().map(expression -> process(expression, context)).collect(Collectors.toSet());
                    set = FastutilSetHelper.toFastutilHashSet(objectSet, expressionTypes.get(node.getValue()), metadata.getFunctionRegistry());
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
                    types.add(expressionTypes.get(expression));
                    continue;
                }

                if (inValue == null) {
                    hasNullValue = true;
                }
                else if (!found && (Boolean) invokeOperator(OperatorType.EQUAL, types(node.getValue(), expression), ImmutableList.of(value, inValue))) {
                    // in does not short-circuit so we must evaluate all value in the list
                    found = true;
                }
            }
            if (found) {
                return true;
            }

            if (hasUnresolvedValue) {
                Type type = expressionTypes.get(node.getValue());
                List<Expression> expressionValues = toExpressions(values, types);
                List<Expression> simplifiedExpressionValues = Stream.concat(
                        expressionValues.stream()
                                .filter(DeterminismEvaluator::isDeterministic)
                                .distinct(),
                        expressionValues.stream()
                                .filter((expression -> !DeterminismEvaluator.isDeterministic(expression))))
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
                return new ArithmeticUnaryExpression(node.getSign(), toExpression(value, expressionTypes.get(node.getValue())));
            }

            switch (node.getSign()) {
                case PLUS:
                    return value;
                case MINUS:
                    Signature operatorSignature = metadata.getFunctionRegistry().resolveOperator(OperatorType.NEGATION, types(node.getValue()));
                    MethodHandle handle = metadata.getFunctionRegistry().getScalarFunctionImplementation(operatorSignature).getMethodHandle();

                    if (handle.type().parameterCount() > 0 && handle.type().parameterType(0) == ConnectorSession.class) {
                        handle = handle.bindTo(session);
                    }
                    try {
                        return handle.invokeWithArguments(value);
                    }
                    catch (Throwable throwable) {
                        Throwables.propagateIfInstanceOf(throwable, RuntimeException.class);
                        Throwables.propagateIfInstanceOf(throwable, Error.class);
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
                return new ArithmeticBinaryExpression(node.getType(), toExpression(left, expressionTypes.get(node.getLeft())), toExpression(right, expressionTypes.get(node.getRight())));
            }

            return invokeOperator(OperatorType.valueOf(node.getType().name()), types(node.getLeft(), node.getRight()), ImmutableList.of(left, right));
        }

        @Override
        protected Object visitComparisonExpression(ComparisonExpression node, Object context)
        {
            ComparisonExpressionType type = node.getType();

            Object left = process(node.getLeft(), context);
            if (left == null && type != ComparisonExpressionType.IS_DISTINCT_FROM) {
                return null;
            }

            Object right = process(node.getRight(), context);
            if (type == ComparisonExpressionType.IS_DISTINCT_FROM) {
                if (left == null && right == null) {
                    return false;
                }
                else if (left == null || right == null) {
                    return true;
                }
            }
            else if (right == null) {
                return null;
            }

            if (hasUnresolvedValue(left, right)) {
                return new ComparisonExpression(type, toExpression(left, expressionTypes.get(node.getLeft())), toExpression(right, expressionTypes.get(node.getRight())));
            }

            return invokeOperator(OperatorType.valueOf(type.name()), types(node.getLeft(), node.getRight()), ImmutableList.of(left, right));
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
                        toExpression(value, expressionTypes.get(node.getValue())),
                        toExpression(min, expressionTypes.get(node.getMin())),
                        toExpression(max, expressionTypes.get(node.getMax())));
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

            Type firstType = expressionTypes.get(node.getFirst());
            Type secondType = expressionTypes.get(node.getSecond());

            if (hasUnresolvedValue(first, second)) {
                return new NullIfExpression(toExpression(first, firstType), toExpression(second, secondType));
            }

            Type commonType = metadata.getTypeManager().getCommonSuperType(firstType, secondType).get();

            Signature firstCast = metadata.getFunctionRegistry().getCoercion(firstType, commonType);
            Signature secondCast = metadata.getFunctionRegistry().getCoercion(secondType, commonType);
            ScalarFunctionImplementation firstCastFunction = metadata.getFunctionRegistry().getScalarFunctionImplementation(firstCast);
            ScalarFunctionImplementation secondCastFunction = metadata.getFunctionRegistry().getScalarFunctionImplementation(secondCast);

            // cast(first as <common type>) == cast(second as <common type>)
            boolean equal = (Boolean) invokeOperator(
                    OperatorType.EQUAL,
                    ImmutableList.of(commonType, commonType),
                    ImmutableList.of(
                            invoke(session, firstCastFunction, ImmutableList.of(first)),
                            invoke(session, secondCastFunction, ImmutableList.of(second))));

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
                return new NotExpression(toExpression(value, expressionTypes.get(node.getValue())));
            }

            return !(Boolean) value;
        }

        @Override
        protected Object visitLogicalBinaryExpression(LogicalBinaryExpression node, Object context)
        {
            Object left = process(node.getLeft(), context);
            Object right = process(node.getRight(), context);

            switch (node.getType()) {
                case AND: {
                    // if either left or right is false, result is always false regardless of nulls
                    if (Boolean.FALSE.equals(left) || Boolean.TRUE.equals(right)) {
                        return left;
                    }

                    if (Boolean.FALSE.equals(right) || Boolean.TRUE.equals(left)) {
                        return right;
                    }
                    break;
                }
                case OR: {
                    // if either left or right is true, result is always true regardless of nulls
                    if (Boolean.TRUE.equals(left) || Boolean.FALSE.equals(right)) {
                        return left;
                    }

                    if (Boolean.TRUE.equals(right) || Boolean.FALSE.equals(left)) {
                        return right;
                    }
                    break;
                }
            }

            if (left == null && right == null) {
                return null;
            }

            return new LogicalBinaryExpression(node.getType(),
                    toExpression(left, expressionTypes.get(node.getLeft())),
                    toExpression(right, expressionTypes.get(node.getRight())));
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
                Type type = expressionTypes.get(expression);
                argumentValues.add(value);
                argumentTypes.add(type);
            }
            Signature functionSignature = metadata.getFunctionRegistry().resolveFunction(node.getName(), Lists.transform(argumentTypes, Type::getTypeSignature));
            ScalarFunctionImplementation function = metadata.getFunctionRegistry().getScalarFunctionImplementation(functionSignature);
            for (int i = 0; i < argumentValues.size(); i++) {
                Object value = argumentValues.get(i);
                if (value == null && !function.getNullableArguments().get(i)) {
                    return null;
                }
            }

            // do not optimize non-deterministic functions
            if (optimize && (!function.isDeterministic() || hasUnresolvedValue(argumentValues))) {
                return new FunctionCall(node.getName(), node.getWindow(), node.isDistinct(), toExpressions(argumentValues, argumentTypes));
            }
            return invoke(session, function, argumentValues);
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
                    (node.getEscape() instanceof StringLiteral || node.getEscape() == null)) {
                // fast path when we know the pattern and escape are constant
                return LikeFunctions.like((Slice) value, getConstantPattern(node));
            }

            Object pattern = process(node.getPattern(), context);

            if (pattern == null) {
                return null;
            }

            Object escape = null;
            if (node.getEscape() != null) {
                escape = process(node.getEscape(), context);

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

                return LikeFunctions.like((Slice) value, regex);
            }

            // if pattern is a constant without % or _ replace with a comparison
            if (pattern instanceof Slice && escape == null) {
                String stringPattern = ((Slice) pattern).toStringUtf8();
                if (!stringPattern.contains("%") && !stringPattern.contains("_")) {
                    return new ComparisonExpression(ComparisonExpressionType.EQUAL,
                            toExpression(value, expressionTypes.get(node.getValue())),
                            toExpression(pattern, expressionTypes.get(node.getPattern())));
                }
            }

            Expression optimizedEscape = null;
            if (node.getEscape() != null) {
                optimizedEscape = toExpression(escape, expressionTypes.get(node.getEscape()));
            }

            return new LikePredicate(
                    toExpression(value, expressionTypes.get(node.getValue())),
                    toExpression(pattern, expressionTypes.get(node.getPattern())),
                    optimizedEscape);
        }

        private Regex getConstantPattern(LikePredicate node)
        {
            Regex result = likePatternCache.get(node);

            if (result == null) {
                StringLiteral pattern = (StringLiteral) node.getPattern();
                StringLiteral escape = (StringLiteral) node.getEscape();

                if (escape == null) {
                    result = LikeFunctions.likePattern(pattern.getSlice());
                }
                else {
                    result = LikeFunctions.likePattern(pattern.getSlice(), escape.getSlice());
                }

                likePatternCache.put(node, result);
            }

            return result;
        }

        @Override
        protected Object visitTryExpression(TryExpression node, Object context)
        {
            try {
                Object innerExpression = process(node.getInnerExpression(), context);
                if (innerExpression instanceof Expression) {
                    return new TryExpression((Expression) innerExpression);
                }

                return innerExpression;
            }
            catch (PrestoException e) {
                tryExpressionExceptionHandler(e);
            }
            return null;
        }

        @Override
        public Object visitCast(Cast node, Object context)
        {
            Object value = process(node.getExpression(), context);

            if (value instanceof Expression) {
                return new Cast((Expression) value, node.getType(), node.isSafe(), node.isTypeOnly());
            }

            if (node.isTypeOnly()) {
                return value;
            }

            // hack!!! don't optimize CASTs for types that cannot be represented in the SQL AST
            // TODO: this will not be an issue when we migrate to RowExpression tree for this, which allows arbitrary literals.
            if (optimize && !FunctionRegistry.isSupportedLiteralType(expressionTypes.get(node))) {
                return new Cast(toExpression(value, expressionTypes.get(node.getExpression())), node.getType(), node.isSafe(), node.isTypeOnly());
            }

            if (value == null) {
                return null;
            }

            Type type = metadata.getType(parseTypeSignature(node.getType()));
            if (type == null) {
                throw new IllegalArgumentException("Unsupported type: " + node.getType());
            }

            Signature operator = metadata.getFunctionRegistry().getCoercion(expressionTypes.get(node.getExpression()), type);

            try {
                return invoke(session, metadata.getFunctionRegistry().getScalarFunctionImplementation(operator), ImmutableList.of(value));
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
            Type elementType = ((ArrayType) expressionTypes.get(node)).getElementType();
            BlockBuilder arrayBlockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), node.getValues().size());

            for (Expression expression : node.getValues()) {
                Object value = process(expression, context);
                if (value instanceof Expression) {
                    return visitFunctionCall(new FunctionCall(QualifiedName.of(ArrayConstructor.ARRAY_CONSTRUCTOR), node.getValues()), context);
                }
                writeNativeValue(elementType, arrayBlockBuilder, value);
            }

            return arrayBlockBuilder.build();
        }

        @Override
        protected Object visitRow(Row node, Object context)
        {
            RowType rowType = checkType(expressionTypes.get(node), RowType.class, "type");
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
                BlockBuilder blockBuilder = new InterleavedBlockBuilder(parameterTypes, new BlockBuilderStatus(), cardinality);
                for (int i = 0; i < cardinality; ++i) {
                    writeNativeValue(parameterTypes.get(i), blockBuilder, values.get(i));
                }
                return blockBuilder.build();
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
            if ((index instanceof Long) && isArray(expressionTypes.get(node.getBase()))) {
                ArraySubscriptOperator.checkArrayIndex((Long) index);
            }

            if (hasUnresolvedValue(base, index)) {
                return new SubscriptExpression(toExpression(base, expressionTypes.get(node.getBase())), toExpression(index, expressionTypes.get(node.getIndex())));
            }

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
            return ImmutableList.copyOf(Iterables.transform(ImmutableList.copyOf(types), Functions.forMap(expressionTypes)));
        }

        private boolean hasUnresolvedValue(Object... values)
        {
            return hasUnresolvedValue(ImmutableList.copyOf(values));
        }

        private boolean hasUnresolvedValue(List<Object> values)
        {
            return any(values, instanceOf(Expression.class));
        }

        private Object invokeOperator(OperatorType operatorType, List<? extends Type> argumentTypes, List<Object> argumentValues)
        {
            Signature operatorSignature = metadata.getFunctionRegistry().resolveOperator(operatorType, argumentTypes);
            return invoke(session, metadata.getFunctionRegistry().getScalarFunctionImplementation(operatorSignature), argumentValues);
        }
    }

    private interface PagePositionContext
    {
        public Block getBlock(int channel);

        public int getPosition(int channel);
    }

    private static class SinglePagePositionContext
            implements PagePositionContext
    {
        private final int position;
        private final Block[] blocks;

        private SinglePagePositionContext(int position, Block[] blocks)
        {
            this.position = position;
            this.blocks = blocks;
        }

        public Block getBlock(int channel)
        {
            return blocks[channel];
        }

        public int getPosition(int channel)
        {
            return position;
        }
    }

    private static class TwoPagesPositionContext
            implements PagePositionContext
    {
        private final int leftPosition;
        private final int rightPosition;
        private final Block[] leftBlocks;
        private final Block[] rightBlocks;

        private TwoPagesPositionContext(int leftPosition, Block[] leftBlocks, int rightPosition, Block[] rightBlocks)
        {
            this.leftPosition = leftPosition;
            this.rightPosition = rightPosition;
            this.leftBlocks = leftBlocks;
            this.rightBlocks = rightBlocks;
        }

        @Override
        public Block getBlock(int channel)
        {
            if (channel < leftBlocks.length) {
                return leftBlocks[channel];
            }
            else {
                return rightBlocks[channel - leftBlocks.length];
            }
        }

        @Override
        public int getPosition(int channel)
        {
            if (channel < leftBlocks.length) {
                return leftPosition;
            }
            else {
                return rightPosition;
            }
        }
    }

    public static Object invoke(ConnectorSession session, ScalarFunctionImplementation function, List<Object> argumentValues)
    {
        MethodHandle handle = function.getMethodHandle();
        if (function.getInstanceFactory().isPresent()) {
            try {
                handle = handle.bindTo(function.getInstanceFactory().get().invoke());
            }
            catch (Throwable throwable) {
                if (throwable instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw Throwables.propagate(throwable);
            }
        }
        if (handle.type().parameterCount() > 0 && handle.type().parameterType(0) == ConnectorSession.class) {
            handle = handle.bindTo(session);
        }
        try {
            List<Object> actualArguments = new ArrayList<>();
            Class<?>[] parameterArray = handle.type().parameterArray();
            for (int i = 0; i < argumentValues.size(); i++) {
                Object argument = argumentValues.get(i);
                if (function.getNullFlags().get(i)) {
                    boolean isNull = argument == null;
                    if (isNull) {
                        argument = Defaults.defaultValue(parameterArray[actualArguments.size()]);
                    }
                    actualArguments.add(argument);
                    actualArguments.add(isNull);
                }
                else {
                    actualArguments.add(argument);
                }
            }
            return handle.invokeWithArguments(actualArguments);
        }
        catch (Throwable throwable) {
            if (throwable instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw Throwables.propagate(throwable);
        }
    }

    @VisibleForTesting
    public static Expression createFailureFunction(RuntimeException exception, Type type)
    {
        requireNonNull(exception, "Exception is null");

        String failureInfo = JsonCodec.jsonCodec(FailureInfo.class).toJson(Failures.toFailure(exception).toFailureInfo());
        FunctionCall jsonParse = new FunctionCall(QualifiedName.of("json_parse"), ImmutableList.of(new StringLiteral(failureInfo)));
        FunctionCall failureFunction = new FunctionCall(QualifiedName.of("fail"), ImmutableList.of(jsonParse));

        return new Cast(failureFunction, type.getTypeSignature().toString());
    }

    private static boolean isArray(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.ARRAY);
    }
}
