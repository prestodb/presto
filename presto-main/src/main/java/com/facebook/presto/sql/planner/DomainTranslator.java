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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Marker;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.math.DoubleMath;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionRegistry.getMagicLiteralFunctionSignature;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.combineDisjunctsWithDefault;
import static com.facebook.presto.sql.ExpressionUtils.or;
import static com.facebook.presto.sql.planner.LiteralInterpreter.toExpression;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.LESS_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.NOT_EQUAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.primitives.Primitives.wrap;
import static java.math.RoundingMode.CEILING;
import static java.math.RoundingMode.FLOOR;

public final class DomainTranslator
{
    private static final String DATE_LITERAL = getMagicLiteralFunctionSignature(DATE).getName();
    private static final String TIMESTAMP_LITERAL = getMagicLiteralFunctionSignature(TIMESTAMP).getName();

    private DomainTranslator()
    {
    }

    public static Expression toPredicate(TupleDomain<Symbol> tupleDomain, Map<Symbol, Type> symbolTypes)
    {
        if (tupleDomain.isNone()) {
            return FALSE_LITERAL;
        }
        ImmutableList.Builder<Expression> conjunctBuilder = ImmutableList.builder();
        for (Map.Entry<Symbol, Domain> entry : tupleDomain.getDomains().entrySet()) {
            Symbol symbol = entry.getKey();
            QualifiedNameReference reference = new QualifiedNameReference(symbol.toQualifiedName());
            Type type = symbolTypes.get(symbol);
            conjunctBuilder.add(toPredicate(entry.getValue(), reference, type));
        }
        return combineConjuncts(conjunctBuilder.build());
    }

    private static Expression toPredicate(Domain domain, QualifiedNameReference reference, Type type)
    {
        if (domain.getRanges().isNone()) {
            return domain.isNullAllowed() ? new IsNullPredicate(reference) : FALSE_LITERAL;
        }

        if (domain.getRanges().isAll()) {
            return domain.isNullAllowed() ? TRUE_LITERAL : new NotExpression(new IsNullPredicate(reference));
        }

        // Add disjuncts for ranges
        List<Expression> disjuncts = new ArrayList<>();
        List<Expression> singleValues = new ArrayList<>();
        for (Range range : domain.getRanges()) {
            checkState(!range.isAll()); // Already checked
            if (range.isSingleValue()) {
                singleValues.add(toExpression(range.getLow().getValue(), type));
            }
            else if (isBetween(range)) {
                // Specialize the range with BETWEEN expression if possible b/c it is currently more efficient
                disjuncts.add(new BetweenPredicate(reference, toExpression(range.getLow().getValue(), type), toExpression(range.getHigh().getValue(), type)));
            }
            else {
                List<Expression> rangeConjuncts = new ArrayList<>();
                if (!range.getLow().isLowerUnbounded()) {
                    switch (range.getLow().getBound()) {
                        case ABOVE:
                            rangeConjuncts.add(new ComparisonExpression(GREATER_THAN, reference, toExpression(range.getLow().getValue(), type)));
                            break;
                        case EXACTLY:
                            rangeConjuncts.add(new ComparisonExpression(GREATER_THAN_OR_EQUAL, reference, toExpression(range.getLow().getValue(),
                                    type)));
                            break;
                        case BELOW:
                            throw new IllegalStateException("Low Marker should never use BELOW bound: " + range);
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                    }
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    switch (range.getHigh().getBound()) {
                        case ABOVE:
                            throw new IllegalStateException("High Marker should never use ABOVE bound: " + range);
                        case EXACTLY:
                            rangeConjuncts.add(new ComparisonExpression(LESS_THAN_OR_EQUAL, reference, toExpression(range.getHigh().getValue(), type)));
                            break;
                        case BELOW:
                            rangeConjuncts.add(new ComparisonExpression(LESS_THAN, reference, toExpression(range.getHigh().getValue(), type)));
                            break;
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                    }
                }
                // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                checkState(!rangeConjuncts.isEmpty());
                disjuncts.add(combineConjuncts(rangeConjuncts));
            }
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(new ComparisonExpression(EQUAL, reference, getOnlyElement(singleValues)));
        }
        else if (singleValues.size() > 1) {
            disjuncts.add(new InPredicate(reference, new InListExpression(singleValues)));
        }

        // Add nullability disjuncts
        checkState(!disjuncts.isEmpty());
        if (domain.isNullAllowed()) {
            disjuncts.add(new IsNullPredicate(reference));
        }
        return combineDisjunctsWithDefault(disjuncts, TRUE_LITERAL);
    }

    private static boolean isBetween(Range range)
    {
        return !range.getLow().isLowerUnbounded() && range.getLow().getBound() == Marker.Bound.EXACTLY
                && !range.getHigh().isUpperUnbounded() && range.getHigh().getBound() == Marker.Bound.EXACTLY;
    }

    /**
     * Convert an Expression predicate into an ExtractionResult consisting of:
     * 1) A successfully extracted TupleDomain
     * 2) An Expression fragment which represents the part of the original Expression that will need to be re-evaluated
     * after filtering with the TupleDomain.
     */
    public static ExtractionResult fromPredicate(
            Metadata metadata,
            Session session,
            Expression predicate,
            Map<Symbol, Type> types)
    {
        return new Visitor(metadata, session, types).process(predicate, false);
    }

    private static class Visitor
            extends AstVisitor<ExtractionResult, Boolean>
    {
        private final Metadata metadata;
        private final ConnectorSession session;
        private final Map<Symbol, Type> types;

        private Visitor(Metadata metadata, Session session, Map<Symbol, Type> types)
        {
            this.metadata = checkNotNull(metadata, "metadata is null");
            this.session = checkNotNull(session, "session is null").toConnectorSession();
            this.types = ImmutableMap.copyOf(checkNotNull(types, "types is null"));
        }

        private Type checkedTypeLookup(Symbol symbol)
        {
            Type type = types.get(symbol);
            checkArgument(type != null, "Types is missing info for symbol: %s", symbol);
            return type;
        }

        private static SortedRangeSet complementIfNecessary(SortedRangeSet range, boolean complement)
        {
            return complement ? range.complement() : range;
        }

        private static Domain complementIfNecessary(Domain domain, boolean complement)
        {
            return complement ? domain.complement() : domain;
        }

        private static Expression complementIfNecessary(Expression expression, boolean complement)
        {
            return complement ? new NotExpression(expression) : expression;
        }

        @Override
        protected ExtractionResult visitExpression(Expression node, Boolean complement)
        {
            // If we don't know how to process this node, the default response is to say that the TupleDomain is "all"
            return new ExtractionResult(TupleDomain.all(), complementIfNecessary(node, complement));
        }

        @Override
        protected ExtractionResult visitLogicalBinaryExpression(LogicalBinaryExpression node, Boolean complement)
        {
            ExtractionResult leftResult = process(node.getLeft(), complement);
            ExtractionResult rightResult = process(node.getRight(), complement);

            LogicalBinaryExpression.Type type = complement ? flipLogicalBinaryType(node.getType()) : node.getType();
            switch (type) {
                case AND:
                    return new ExtractionResult(
                            leftResult.getTupleDomain().intersect(rightResult.getTupleDomain()),
                            combineConjuncts(leftResult.getRemainingExpression(), rightResult.getRemainingExpression()));

                case OR:
                    TupleDomain<Symbol> columnUnionedTupleDomain = TupleDomain.columnWiseUnion(leftResult.getTupleDomain(), rightResult.getTupleDomain());

                    // In most cases, the columnUnionedTupleDomain is only a superset of the actual strict union
                    // and so we can return the current node as the remainingExpression so that all bounds will be double checked again at execution time.
                    Expression remainingExpression = complementIfNecessary(node, complement);

                    // However, there are a few cases where the column-wise union is actually equivalent to the strict union, so we if can detect
                    // some of these cases, we won't have to double check the bounds unnecessarily at execution time.

                    // We can only make inferences if the remaining expressions on both side are equal and deterministic
                    if (leftResult.getRemainingExpression().equals(rightResult.getRemainingExpression()) &&
                            DeterminismEvaluator.isDeterministic(leftResult.getRemainingExpression())) {
                        // The column-wise union is equivalent to the strict union if
                        // 1) If both TupleDomains consist of the same exact single column (e.g. left TupleDomain => (a > 0), right TupleDomain => (a < 10))
                        // 2) If one TupleDomain is a superset of the other (e.g. left TupleDomain => (a > 0, b > 0 && b < 10), right TupleDomain => (a > 5, b = 5))
                        boolean matchingSingleSymbolDomains = !leftResult.getTupleDomain().isNone()
                                && !rightResult.getTupleDomain().isNone()
                                && leftResult.getTupleDomain().getDomains().size() == 1
                                && rightResult.getTupleDomain().getDomains().size() == 1
                                && leftResult.getTupleDomain().getDomains().keySet().equals(rightResult.getTupleDomain().getDomains().keySet());
                        boolean oneSideIsSuperSet = leftResult.getTupleDomain().contains(rightResult.getTupleDomain()) || rightResult.getTupleDomain().contains(leftResult.getTupleDomain());

                        if (matchingSingleSymbolDomains || oneSideIsSuperSet) {
                            remainingExpression = leftResult.getRemainingExpression();
                        }
                    }

                    return new ExtractionResult(columnUnionedTupleDomain, remainingExpression);

                default:
                    throw new AssertionError("Unknown type: " + node.getType());
            }
        }

        private static LogicalBinaryExpression.Type flipLogicalBinaryType(LogicalBinaryExpression.Type type)
        {
            switch (type) {
                case AND:
                    return LogicalBinaryExpression.Type.OR;
                case OR:
                    return LogicalBinaryExpression.Type.AND;
                default:
                    throw new AssertionError("Unknown type: " + type);
            }
        }

        @Override
        protected ExtractionResult visitNotExpression(NotExpression node, Boolean complement)
        {
            return process(node.getValue(), !complement);
        }

        @Override
        protected ExtractionResult visitComparisonExpression(ComparisonExpression node, Boolean complement)
        {
            if (isSimpleMagicLiteralComparison(node)) {
                node = normalizeSimpleComparison(node);
                node = convertMagicLiteralComparison(node);
            }
            else if (isSimpleComparison(node)) {
                node = normalizeSimpleComparison(node);
            }
            else {
                return super.visitComparisonExpression(node, complement);
            }

            Symbol symbol = Symbol.fromQualifiedName(((QualifiedNameReference) node.getLeft()).getName());
            Type columnType = checkedTypeLookup(symbol);
            Object value = LiteralInterpreter.evaluate(metadata, session, node.getRight());

            // Handle the cases where implicit coercions can happen in comparisons
            // TODO: how to abstract this out
            if (value instanceof Double && columnType.equals(BIGINT)) {
                return process(coerceDoubleToLongComparison(node), complement);
            }
            if (value instanceof Long && columnType.equals(DoubleType.DOUBLE)) {
                value = ((Long) value).doubleValue();
            }
            verifyType(columnType, value);
            return createComparisonExtractionResult(node.getType(), symbol, columnType, objectToComparable(value), complement);
        }

        private ExtractionResult createComparisonExtractionResult(ComparisonExpression.Type comparisonType, Symbol column, Type columnType, Comparable<?> value, boolean complement)
        {
            if (value == null) {
                switch (comparisonType) {
                    case EQUAL:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                    case NOT_EQUAL:
                        return new ExtractionResult(TupleDomain.none(), TRUE_LITERAL);

                    case IS_DISTINCT_FROM:
                        Domain domain = complementIfNecessary(Domain.notNull(wrap(columnType.getJavaType())), complement);
                        return new ExtractionResult(
                                TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)),
                                TRUE_LITERAL);

                    default:
                        throw new AssertionError("Unhandled type: " + comparisonType);
                }
            }

            Domain domain;
            switch (comparisonType) {
                case EQUAL:
                    domain = Domain.create(complementIfNecessary(SortedRangeSet.of(Range.equal(value)), complement), false);
                    break;
                case GREATER_THAN:
                    domain = Domain.create(complementIfNecessary(SortedRangeSet.of(Range.greaterThan(value)), complement), false);
                    break;
                case GREATER_THAN_OR_EQUAL:
                    domain = Domain.create(complementIfNecessary(SortedRangeSet.of(Range.greaterThanOrEqual(value)), complement), false);
                    break;
                case LESS_THAN:
                    domain = Domain.create(complementIfNecessary(SortedRangeSet.of(Range.lessThan(value)), complement), false);
                    break;
                case LESS_THAN_OR_EQUAL:
                    domain = Domain.create(complementIfNecessary(SortedRangeSet.of(Range.lessThanOrEqual(value)), complement), false);
                    break;
                case NOT_EQUAL:
                    domain = Domain.create(complementIfNecessary(SortedRangeSet.of(Range.lessThan(value), Range.greaterThan(value)), complement), false);
                    break;
                case IS_DISTINCT_FROM:
                    // Need to potential complement the whole domain for IS_DISTINCT_FROM since it is null-aware
                    domain = complementIfNecessary(Domain.create(SortedRangeSet.of(Range.lessThan(value), Range.greaterThan(value)), true), complement);
                    break;
                default:
                    throw new AssertionError("Unhandled type: " + comparisonType);
            }

            return new ExtractionResult(
                    TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)),
                    TRUE_LITERAL);
        }

        private static void verifyType(Type type, Object value)
        {
            checkState(value == null || wrap(type.getJavaType()).isInstance(value), "Value %s is not of expected type %s", value, type);
        }

        private static Comparable<?> objectToComparable(Object value)
        {
            return (Comparable<?>) value;
        }

        @Override
        protected ExtractionResult visitInPredicate(InPredicate node, Boolean complement)
        {
            if (!(node.getValue() instanceof QualifiedNameReference) || !(node.getValueList() instanceof InListExpression)) {
                return super.visitInPredicate(node, complement);
            }

            InListExpression valueList = (InListExpression) node.getValueList();
            checkState(!valueList.getValues().isEmpty(), "InListExpression should never be empty");

            ImmutableList.Builder<Expression> disjuncts = ImmutableList.builder();
            for (Expression expression : valueList.getValues()) {
                disjuncts.add(new ComparisonExpression(EQUAL, node.getValue(), expression));
            }
            return process(or(disjuncts.build()), complement);
        }

        @Override
        protected ExtractionResult visitBetweenPredicate(BetweenPredicate node, Boolean complement)
        {
            // Re-write as two comparison expressions
            return process(and(
                    new ComparisonExpression(GREATER_THAN_OR_EQUAL, node.getValue(), node.getMin()),
                    new ComparisonExpression(LESS_THAN_OR_EQUAL, node.getValue(), node.getMax())), complement);
        }

        @Override
        protected ExtractionResult visitIsNullPredicate(IsNullPredicate node, Boolean complement)
        {
            if (!(node.getValue() instanceof QualifiedNameReference)) {
                return super.visitIsNullPredicate(node, complement);
            }

            Symbol symbol = Symbol.fromQualifiedName(((QualifiedNameReference) node.getValue()).getName());
            Type columnType = checkedTypeLookup(symbol);
            Domain domain = complementIfNecessary(Domain.onlyNull(wrap(columnType.getJavaType())), complement);
            return new ExtractionResult(
                    TupleDomain.withColumnDomains(ImmutableMap.of(symbol, domain)),
                    TRUE_LITERAL);
        }

        @Override
        protected ExtractionResult visitIsNotNullPredicate(IsNotNullPredicate node, Boolean complement)
        {
            if (!(node.getValue() instanceof QualifiedNameReference)) {
                return super.visitIsNotNullPredicate(node, complement);
            }

            Symbol symbol = Symbol.fromQualifiedName(((QualifiedNameReference) node.getValue()).getName());
            Type columnType = checkedTypeLookup(symbol);

            Domain domain = complementIfNecessary(Domain.notNull(wrap(columnType.getJavaType())), complement);
            return new ExtractionResult(
                    TupleDomain.withColumnDomains(ImmutableMap.of(symbol, domain)),
                    TRUE_LITERAL);
        }

        @Override
        protected ExtractionResult visitBooleanLiteral(BooleanLiteral node, Boolean complement)
        {
            boolean value = complement ? !node.getValue() : node.getValue();
            return new ExtractionResult(value ? TupleDomain.all() : TupleDomain.none(), TRUE_LITERAL);
        }

        @Override
        protected ExtractionResult visitNullLiteral(NullLiteral node, Boolean complement)
        {
            return new ExtractionResult(TupleDomain.none(), TRUE_LITERAL);
        }
    }

    private static boolean isSimpleComparison(ComparisonExpression comparison)
    {
        return (comparison.getLeft() instanceof QualifiedNameReference && comparison.getRight() instanceof Literal) ||
                (comparison.getLeft() instanceof Literal && comparison.getRight() instanceof QualifiedNameReference);
    }

    /**
     * Normalize a simple comparison between a QualifiedNameReference and a Literal such that the QualifiedNameReference will always be on the left and the Literal on the right.
     */
    private static ComparisonExpression normalizeSimpleComparison(ComparisonExpression comparison)
    {
        if (comparison.getLeft() instanceof QualifiedNameReference) {
            return comparison;
        }
        if (comparison.getRight() instanceof QualifiedNameReference) {
            return new ComparisonExpression(flipComparisonDirection(comparison.getType()), comparison.getRight(), comparison.getLeft());
        }
        throw new IllegalArgumentException("ComparisonExpression not a simple literal comparison: " + comparison);
    }

    private static ComparisonExpression.Type flipComparisonDirection(ComparisonExpression.Type type)
    {
        switch (type) {
            case LESS_THAN_OR_EQUAL:
                return GREATER_THAN_OR_EQUAL;
            case LESS_THAN:
                return GREATER_THAN;
            case GREATER_THAN_OR_EQUAL:
                return LESS_THAN_OR_EQUAL;
            case GREATER_THAN:
                return LESS_THAN;
            default:
                // The remaining types have no direction association
                return type;
        }
    }

    private static Expression coerceDoubleToLongComparison(ComparisonExpression comparison)
    {
        comparison = normalizeSimpleComparison(comparison);

        checkArgument(comparison.getLeft() instanceof QualifiedNameReference, "Left must be a QualifiedNameReference");
        checkArgument(comparison.getRight() instanceof DoubleLiteral, "Right must be a DoubleLiteral");

        QualifiedNameReference reference = (QualifiedNameReference) comparison.getLeft();
        Double value = ((DoubleLiteral) comparison.getRight()).getValue();

        switch (comparison.getType()) {
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
                return new ComparisonExpression(comparison.getType(), reference, toExpression(DoubleMath.roundToLong(value, CEILING), BIGINT));

            case GREATER_THAN:
            case LESS_THAN_OR_EQUAL:
                return new ComparisonExpression(comparison.getType(), reference, toExpression(DoubleMath.roundToLong(value, FLOOR), BIGINT));

            case EQUAL:
                Long equalValue = DoubleMath.roundToLong(value, FLOOR);
                if (equalValue.doubleValue() != value) {
                    // Return something that is false for all non-null values
                    return and(new ComparisonExpression(EQUAL, reference, new LongLiteral("0")),
                            new ComparisonExpression(NOT_EQUAL, reference, new LongLiteral("0")));
                }
                return new ComparisonExpression(comparison.getType(), reference, toExpression(equalValue, BIGINT));

            case NOT_EQUAL:
                Long notEqualValue = DoubleMath.roundToLong(value, FLOOR);
                if (notEqualValue.doubleValue() != value) {
                    // Return something that is true for all non-null values
                    return or(new ComparisonExpression(EQUAL, reference, new LongLiteral("0")),
                            new ComparisonExpression(NOT_EQUAL, reference, new LongLiteral("0")));
                }
                return new ComparisonExpression(comparison.getType(), reference, toExpression(notEqualValue, BIGINT));

            case IS_DISTINCT_FROM:
                Long distinctValue = DoubleMath.roundToLong(value, FLOOR);
                if (distinctValue.doubleValue() != value) {
                    return TRUE_LITERAL;
                }
                return new ComparisonExpression(comparison.getType(), reference, toExpression(distinctValue, BIGINT));

            default:
                throw new AssertionError("Unhandled type: " + comparison.getType());
        }
    }

    public static class ExtractionResult
    {
        private final TupleDomain<Symbol> tupleDomain;
        private final Expression remainingExpression;

        public ExtractionResult(TupleDomain<Symbol> tupleDomain, Expression remainingExpression)
        {
            this.tupleDomain = checkNotNull(tupleDomain, "tupleDomain is null");
            this.remainingExpression = checkNotNull(remainingExpression, "remainingExpression is null");
        }

        public TupleDomain<Symbol> getTupleDomain()
        {
            return tupleDomain;
        }

        public Expression getRemainingExpression()
        {
            return remainingExpression;
        }
    }

    // TODO: remove this horrible hack
    private static boolean isSimpleMagicLiteralComparison(ComparisonExpression node)
    {
        FunctionCall call;
        if ((node.getLeft() instanceof QualifiedNameReference) && (node.getRight() instanceof FunctionCall)) {
            call = (FunctionCall) node.getRight();
        }
        else if ((node.getLeft() instanceof FunctionCall) && (node.getRight() instanceof QualifiedNameReference)) {
            call = (FunctionCall) node.getLeft();
        }
        else {
            return false;
        }

        if (call.getName().getPrefix().isPresent()) {
            return false;
        }
        String name = call.getName().getSuffix();

        return name.equals(DATE_LITERAL) || name.equals(TIMESTAMP_LITERAL);
    }

    private static ComparisonExpression convertMagicLiteralComparison(ComparisonExpression node)
    {
        // "magic literal" functions use the stack type value for the argument
        checkArgument(isSimpleMagicLiteralComparison(node), "not a simple magic literal comparison");
        FunctionCall call = (FunctionCall) node.getRight();
        Expression value = call.getArguments().get(0);
        return new ComparisonExpression(node.getType(), node.getLeft(), value);
    }
}
