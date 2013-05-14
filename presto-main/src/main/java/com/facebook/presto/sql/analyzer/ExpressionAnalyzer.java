package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.tree.AliasedExpression;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.DateLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Extract;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NegativeExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.sql.tree.WhenClause;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.analyzer.SemanticErrorCode.AMBIGUOUS_ATTRIBUTE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_ATTRIBUTE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;

public class ExpressionAnalyzer
{
    private final Metadata metadata;
    private final Map<QualifiedName, Integer> resolvedNames = new HashMap<>();
    private final IdentityHashMap<FunctionCall, FunctionInfo> resolvedFunctions = new IdentityHashMap<>();
    private final IdentityHashMap<Expression, Type> subExpressionTypes = new IdentityHashMap<>();

    public ExpressionAnalyzer(Metadata metadata)
    {
        this.metadata = metadata;
    }

    public Map<QualifiedName, Integer> getResolvedNames()
    {
        return resolvedNames;
    }

    public IdentityHashMap<FunctionCall, FunctionInfo> getResolvedFunctions()
    {
        return resolvedFunctions;
    }

    public IdentityHashMap<Expression, Type> getSubExpressionTypes()
    {
        return subExpressionTypes;
    }

    /**
     * @param tupleDescriptor the tuple descriptor to use to resolve QualifiedNames
     */
    public Type analyze(Expression expression, TupleDescriptor tupleDescriptor)
    {
        Visitor visitor = new Visitor(tupleDescriptor);

        return expression.accept(visitor, null);
    }

    private class Visitor
            extends AstVisitor<Type, Void>
    {
        private final TupleDescriptor tupleDescriptor;

        private Visitor(TupleDescriptor tupleDescriptor)
        {
            this.tupleDescriptor = tupleDescriptor;
        }

        @Override
        protected Type visitCurrentTime(CurrentTime node, Void context)
        {
            if (node.getType() != CurrentTime.Type.TIMESTAMP) {
                throw new SemanticException(NOT_SUPPORTED, node, "%s not yet supported", node.getType().getName());
            }

            if (node.getPrecision() != null) {
                throw new SemanticException(NOT_SUPPORTED, node, "non-default precision not yet supported");
            }

            subExpressionTypes.put(node, Type.LONG);
            return Type.LONG;
        }

        @Override
        protected Type visitQualifiedNameReference(QualifiedNameReference node, Void context)
        {
            List<Integer> matches = tupleDescriptor.resolveFieldIndexes(node.getName());
            if (matches.isEmpty()) {
                throw new SemanticException(MISSING_ATTRIBUTE, node, "Attribute '%s' cannot be resolved", node.getName());
            }
            else if (matches.size() > 1) {
                throw new SemanticException(AMBIGUOUS_ATTRIBUTE, node, "Attribute '%s' is ambiguous", node.getName());
            }

            int fieldIndex = Iterables.getOnlyElement(matches);
            Field field = tupleDescriptor.getFields().get(fieldIndex);
            resolvedNames.put(node.getName(), fieldIndex);
            subExpressionTypes.put(node, field.getType());

            return field.getType();
        }

        @Override
        protected Type visitNotExpression(NotExpression node, Void context)
        {
            Type value = process(node.getValue(), context);
            if (value != Type.BOOLEAN) {
                throw new SemanticException(TYPE_MISMATCH, node.getValue(), "Value of logical NOT expression must evaluate to a BOOLEAN (actual: %s)", value);
            }

            subExpressionTypes.put(node, Type.BOOLEAN);
            return Type.BOOLEAN;
        }

        @Override
        protected Type visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            Type left = process(node.getLeft(), context);
            if (left != Type.BOOLEAN) {
                throw new SemanticException(TYPE_MISMATCH, node.getLeft(), "Left side of logical expression must evaluate to a BOOLEAN (actual: %s)", left);
            }
            Type right = process(node.getRight(), context);
            if (right != Type.BOOLEAN) {
                throw new SemanticException(TYPE_MISMATCH, node.getRight(), "Right side of logical expression must evaluate to a BOOLEAN (actual: %s)", right);
            }

            subExpressionTypes.put(node, Type.BOOLEAN);
            return Type.BOOLEAN;
        }

        @Override
        protected Type visitComparisonExpression(ComparisonExpression node, Void context)
        {
            Type left = process(node.getLeft(), context);
            Type right = process(node.getRight(), context);

            if (left != right && !(Type.isNumeric(left) && Type.isNumeric(right))) {
                throw new SemanticException(TYPE_MISMATCH, node, "Types are not comparable with '%s': %s vs %s", node.getType().getValue(), left, right);
            }

            subExpressionTypes.put(node, Type.BOOLEAN);
            return Type.BOOLEAN;
        }

        @Override
        protected Type visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            process(node.getValue(), context);

            subExpressionTypes.put(node, Type.BOOLEAN);
            return Type.BOOLEAN;
        }

        @Override
        protected Type visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            process(node.getValue(), context);

            subExpressionTypes.put(node, Type.BOOLEAN);
            return Type.BOOLEAN;
        }

        @Override
        protected Type visitNullIfExpression(NullIfExpression node, Void context)
        {
            Type first = process(node.getFirst(), context);
            Type second = process(node.getSecond(), context);

            if (first != second && !(Type.isNumeric(first) && Type.isNumeric(second))) {
                throw new SemanticException(TYPE_MISMATCH, node, "Types are not comparable with nullif: %s vs %s", first, second);
            }

            subExpressionTypes.put(node, first);
            return first;
        }

        @Override
        protected Type visitIfExpression(IfExpression node, Void context)
        {
            Type condition = process(node.getCondition(), context);
            if (!isBooleanOrNull(condition)) {
                throw new SemanticException(TYPE_MISMATCH, node, "IF condition must be a boolean type: %s", condition);
            }

            Type first = process(node.getTrueValue(), context);
            if (!node.getFalseValue().isPresent()) {
                subExpressionTypes.put(node, first);
                return first;
            }

            Type second = process(node.getFalseValue().get(), context);
            if (!sameType(first, second)) {
                throw new SemanticException(TYPE_MISMATCH, node, "Result types for IF must be the same: %s vs %s", first, second);
            }

            Type type = (first != Type.NULL) ? first : second;
            subExpressionTypes.put(node, type);
            return type;
        }

        @Override
        protected Type visitSearchedCaseExpression(SearchedCaseExpression node, Void context)
        {
            for (WhenClause whenClause : node.getWhenClauses()) {
                Type whenOperand = process(whenClause.getOperand(), context);
                if (!isBooleanOrNull(whenOperand)) {
                    throw new SemanticException(TYPE_MISMATCH, node, "WHEN clause must be a boolean type: %s", whenOperand);
                }
            }

            List<Type> types = new ArrayList<>();
            for (WhenClause whenClause : node.getWhenClauses()) {
                types.add(process(whenClause.getResult(), context));
            }
            if (node.getDefaultValue() != null) {
                types.add(process(node.getDefaultValue(), context));
            }

            Type type = getSingleType(node, "clauses", types);
            subExpressionTypes.put(node, type);

            return type;
        }

        @Override
        protected Type visitSimpleCaseExpression(SimpleCaseExpression node, Void context)
        {
            Type operand = process(node.getOperand(), context);
            for (WhenClause whenClause : node.getWhenClauses()) {
                Type whenOperand = process(whenClause.getOperand(), context);
                if (!sameType(operand, whenOperand)) {
                    throw new SemanticException(TYPE_MISMATCH, node, "CASE operand type does not match WHEN clause operand type: %s vs %s", operand, whenOperand);
                }
            }

            List<Type> types = new ArrayList<>();
            for (WhenClause whenClause : node.getWhenClauses()) {
                types.add(process(whenClause.getResult(), context));
            }
            if (node.getDefaultValue() != null) {
                types.add(process(node.getDefaultValue(), context));
            }

            Type type = getSingleType(node, "clauses", types);
            subExpressionTypes.put(node, type);

            return type;
        }

        @Override
        protected Type visitCoalesceExpression(CoalesceExpression node, Void context)
        {
            List<Type> operandTypes = new ArrayList<>();
            for (Expression expression : node.getOperands()) {
                operandTypes.add(process(expression, context));
            }

            Type type = getSingleType(node, "operands", operandTypes);
            subExpressionTypes.put(node, type);

            return type;
        }

        private Type getSingleType(Node node, String subTypeName, List<Type> subTypes)
        {
            subTypes = ImmutableList.copyOf(filter(subTypes, not(equalTo(Type.NULL))));
            Type firstOperand = Iterables.get(subTypes, 0);
            if (!Iterables.all(subTypes, sameTypePredicate(firstOperand))) {
                throw new SemanticException(TYPE_MISMATCH, node, "All %s must be the same type: %s", subTypeName, subTypes);
            }
            return firstOperand;
        }

        @Override
        protected Type visitNegativeExpression(NegativeExpression node, Void context)
        {
            Type type = process(node.getValue(), context);
            if (!Type.isNumeric(type)) {
                throw new SemanticException(TYPE_MISMATCH, node.getValue(), "Value of negative operator must be numeric (actual: %s)", type);
            }

            subExpressionTypes.put(node, type);
            return type;
        }

        @Override
        protected Type visitArithmeticExpression(ArithmeticExpression node, Void context)
        {
            Type left = process(node.getLeft(), context);
            Type right = process(node.getRight(), context);

            if (!Type.isNumeric(left)) {
                throw new SemanticException(TYPE_MISMATCH, node.getLeft(), "Left side of '%s' must be numeric (actual: %s)", node.getType().getValue(), left);
            }
            if (!Type.isNumeric(right)) {
                throw new SemanticException(TYPE_MISMATCH, node.getRight(), "Right side of '%s' must be numeric (actual: %s)", node.getType().getValue(), right);
            }

            if (left == Type.LONG && right == Type.LONG) {
                subExpressionTypes.put(node, Type.LONG);
                return Type.LONG;
            }

            subExpressionTypes.put(node, Type.DOUBLE);
            return Type.DOUBLE;
        }

        @Override
        protected Type visitLikePredicate(LikePredicate node, Void context)
        {
            Type value = process(node.getValue(), context);
            if (value != Type.STRING && value != Type.NULL) {
                throw new SemanticException(TYPE_MISMATCH, node.getValue(), "Left side of LIKE expression must be a STRING (actual: %s)", value);
            }

            Type pattern = process(node.getPattern(), context);
            if (pattern != Type.STRING && pattern != Type.NULL) {
                throw new SemanticException(TYPE_MISMATCH, node.getValue(), "Pattern for LIKE expression must be a STRING (actual: %s)", pattern);
            }

            if (node.getEscape() != null) {
                Type escape = process(node.getEscape(), context);
                if (escape != Type.STRING && escape != Type.NULL) {
                    throw new SemanticException(TYPE_MISMATCH, node.getValue(), "Escape for LIKE expression must be a STRING (actual: %s)", escape);
                }
            }

            subExpressionTypes.put(node, Type.BOOLEAN);
            return Type.BOOLEAN;
        }

        @Override
        protected Type visitStringLiteral(StringLiteral node, Void context)
        {
            subExpressionTypes.put(node, Type.STRING);
            return Type.STRING;
        }

        @Override
        protected Type visitLongLiteral(LongLiteral node, Void context)
        {
            subExpressionTypes.put(node, Type.LONG);
            return Type.LONG;
        }

        @Override
        protected Type visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            subExpressionTypes.put(node, Type.DOUBLE);
            return Type.DOUBLE;
        }

        @Override
        protected Type visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            subExpressionTypes.put(node, Type.BOOLEAN);
            return Type.BOOLEAN;
        }

        @Override
        protected Type visitDateLiteral(DateLiteral node, Void context)
        {
            subExpressionTypes.put(node, Type.LONG);
            return Type.LONG;
        }

        @Override
        protected Type visitTimestampLiteral(TimestampLiteral node, Void context)
        {
            subExpressionTypes.put(node, Type.LONG);
            return Type.LONG;
        }

        @Override
        protected Type visitIntervalLiteral(IntervalLiteral node, Void context)
        {
            subExpressionTypes.put(node, Type.LONG);
            return Type.LONG;
        }

        @Override
        protected Type visitNullLiteral(NullLiteral node, Void context)
        {
            subExpressionTypes.put(node, Type.NULL);
            return Type.NULL;
        }

        @Override
        protected Type visitFunctionCall(FunctionCall node, Void context)
        {
            if (node.getWindow().isPresent()) {
                for (Expression expression : node.getWindow().get().getPartitionBy()) {
                    process(expression, context);
                }

                for (SortItem sortItem : node.getWindow().get().getOrderBy()) {
                    process(sortItem.getSortKey(), context);
                }
            }

            ImmutableList.Builder<Type> argumentTypes = ImmutableList.builder();
            for (Expression expression : node.getArguments()) {
                argumentTypes.add(process(expression, context));
            }

            FunctionInfo function = metadata.getFunction(node.getName(), Lists.transform(argumentTypes.build(), Type.toRaw()));

            resolvedFunctions.put(node, function);

            Type type = Type.fromRaw(function.getReturnType());
            subExpressionTypes.put(node, type);

            return type;
        }

        @Override
        protected Type visitExtract(Extract node, Void context)
        {
            Type type = process(node.getExpression(), context);
            if (type != Type.LONG) {
                throw new SemanticException(TYPE_MISMATCH, node.getExpression(), "Type of argument to extract must be LONG (actual %s)", type);
            }

            subExpressionTypes.put(node, Type.LONG);
            return Type.LONG;
        }

        @Override
        protected Type visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            Type value = process(node.getValue(), context);
            Type min = process(node.getMin(), context);
            Type max = process(node.getMax(), context);

            if (isStringTypeOrNull(value) && isStringTypeOrNull(min) && isStringTypeOrNull(max)) {
                subExpressionTypes.put(node, Type.BOOLEAN);
                return Type.BOOLEAN;
            }
            if (isNumericOrNull(value) && isNumericOrNull(min) && isNumericOrNull(max)) {
                subExpressionTypes.put(node, Type.BOOLEAN);
                return Type.BOOLEAN;
            }
            throw new SemanticException(TYPE_MISMATCH, node.getValue(), "Between value, min and max must be the same type (value: %s, min: %s, max: %s)", value, min, max);
        }

        @Override
        public Type visitCast(Cast node, Void context)
        {
            process(node.getExpression(), context);

            Type type;
            switch (node.getType()) {
                case "BOOLEAN":
                    type = Type.BOOLEAN;
                    break;
                case "DOUBLE":
                    type = Type.DOUBLE;
                    break;
                case "BIGINT":
                    type = Type.LONG;
                    break;
                case "VARCHAR":
                    type = Type.STRING;
                    break;
                default:
                    throw new SemanticException(TYPE_MISMATCH, node, "Cannot cast to type: " + node.getType());
            }
            subExpressionTypes.put(node, type);
            return type;
        }

        @Override
        protected Type visitInPredicate(InPredicate node, Void context)
        {
            Type valueType = process(node.getValue(), context);
            Type listType = process(node.getValueList(), context);

            if (valueType == Type.NULL) {
                subExpressionTypes.put(node, Type.NULL);
            }
            else if (valueType != listType && !(Type.isNumeric(valueType) && Type.isNumeric(listType))) {
                throw new SemanticException(TYPE_MISMATCH, node, "Types are not comparable for 'IN': %s vs %s", valueType, listType);
            }

            subExpressionTypes.put(node, Type.BOOLEAN);
            return Type.BOOLEAN;
        }

        @Override
        protected Type visitAliasedExpression(AliasedExpression node, Void context)
        {
            Type type = process(node.getExpression(), context);
            subExpressionTypes.put(node, type);

            return type;
        }

        @Override
        protected Type visitInListExpression(InListExpression node, Void context)
        {
            List<Type> types = new ArrayList<>();
            for (Expression value : node.getValues()) {
                types.add(process(value, context));
            }

            // make sure all types are the same
            Type type = getSingleType(node, "values", types);

            subExpressionTypes.put(node, type);
            return type; // TODO: this really should a be relation type
        }

        @Override
        protected Type visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }
    }

    public static Predicate<Type> sameTypePredicate(final Type type)
    {
        return new Predicate<Type>()
        {
            public boolean apply(Type input)
            {
                return sameType(type, input);
            }
        };
    }

    public static boolean sameType(Type type1, Type type2)
    {
        return type1 == type2 || type1 == Type.NULL || type2 == Type.NULL;
    }


    public static boolean isBooleanOrNull(Type type)
    {
        return type == Type.BOOLEAN || type == Type.NULL;
    }

    public static boolean isNumericOrNull(Type type)
    {
        return Type.isNumeric(type) || type == Type.NULL;
    }

    public static boolean isStringTypeOrNull(Type type)
    {
        return type == Type.STRING || type == Type.NULL;
    }
}
