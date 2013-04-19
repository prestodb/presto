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
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Extract;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.InPredicate;
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
import com.facebook.presto.sql.tree.Window;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;

public class ExpressionAnalyzer
{
    private final Metadata metadata;
    private final Map<QualifiedName, Field> resolvedNames = new HashMap<>();
    private final IdentityHashMap<FunctionCall, FunctionInfo> resolvedFunctions = new IdentityHashMap<>();
    private final IdentityHashMap<Expression, Type> subExpressionTypes = new IdentityHashMap<>();

    public ExpressionAnalyzer(Metadata metadata)
    {
        this.metadata = metadata;
    }

    public Map<QualifiedName, Field> getResolvedNames()
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

    public Type analyze(Expression expression, Scope scope)
    {
        Visitor visitor = new Visitor(scope);

        return expression.accept(visitor, null);
    }

    private class Visitor
            extends AstVisitor<Type, Void>
    {
        private final Scope scope;

        private Visitor(Scope scope)
        {
            this.scope = scope;
        }

        @Override
        protected Type visitCurrentTime(CurrentTime node, Void context)
        {
            if (node.getType() != CurrentTime.Type.TIMESTAMP) {
                throw new SemanticException(node, "%s not yet supported", node.getType().getName());
            }

            if (node.getPrecision() != null) {
                throw new SemanticException(node, "non-default precision not yet supported");
            }

            subExpressionTypes.put(node, Type.LONG);
            return Type.LONG;
        }

        @Override
        protected Type visitQualifiedNameReference(QualifiedNameReference node, Void context)
        {
            List<Field> matches = scope.resolve(node.getName());
            if (matches.isEmpty()) {
                throw new SemanticException(node, "Attribute '%s' cannot be resolved", node.getName());
            }
            else if (matches.size() > 1) {
                List<String> names = Lists.transform(matches, new Function<Field, String>()
                {
                    @Override
                    public String apply(Field input)
                    {
                        if (input.getRelationAlias().isPresent()) {
                            return input.getRelationAlias().get() + "." + input.getName().get();
                        }
                        else {
                            return input.getName().get();
                        }
                    }
                });

                throw new SemanticException(node, "Attribute '%s' is ambiguous: %s", node.getName(), names);
            }

            Field field = Iterables.getOnlyElement(matches);
            resolvedNames.put(node.getName(), field);

            subExpressionTypes.put(node, field.getType());
            return field.getType();
        }

        @Override
        protected Type visitNotExpression(NotExpression node, Void context)
        {
            Type value = process(node.getValue(), context);
            if (value != Type.BOOLEAN) {
                throw new SemanticException(node.getValue(), "Value of logical NOT expression must evaluate to a BOOLEAN (actual: %s)", value);
            }

            subExpressionTypes.put(node, Type.BOOLEAN);
            return Type.BOOLEAN;
        }

        @Override
        protected Type visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            Type left = process(node.getLeft(), context);
            if (left != Type.BOOLEAN) {
                throw new SemanticException(node.getLeft(), "Left side of logical expression must evaluate to a BOOLEAN (actual: %s)", left);
            }
            Type right = process(node.getRight(), context);
            if (right != Type.BOOLEAN) {
                throw new SemanticException(node.getRight(), "Right side of logical expression must evaluate to a BOOLEAN (actual: %s)", right);
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
                throw new SemanticException(node, "Types are not comparable with '%s': %s vs %s", node.getType().getValue(), left, right);
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
                throw new SemanticException(node, "Types are not comparable with nullif: %s vs %s", first, second);
            }

            subExpressionTypes.put(node, first);
            return first;
        }

        @Override
        protected Type visitSearchedCaseExpression(SearchedCaseExpression node, Void context)
        {
            for (WhenClause whenClause : node.getWhenClauses()) {
                Type whenOperand = process(whenClause.getOperand(), context);
                if (!isBooleanOrNull(whenOperand)) {
                    throw new SemanticException(node, "WHEN clause must be a boolean type: %s", whenOperand);
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
                    throw new SemanticException(node, "CASE operand type does not match WHEN clause operand type: %s vs %s", operand, whenOperand);
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
                throw new SemanticException(node, "All %s must be the same type: %s", subTypeName, subTypes);
            }
            return firstOperand;
        }

        @Override
        protected Type visitNegativeExpression(NegativeExpression node, Void context)
        {
            Type type = process(node.getValue(), context);
            if (!Type.isNumeric(type)) {
                throw new SemanticException(node.getValue(), "Value of negative operator must be numeric (actual: %s)", type);
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
                throw new SemanticException(node.getLeft(), "Left side of '%s' must be numeric (actual: %s)", node.getType().getValue(), left);
            }
            if (!Type.isNumeric(right)) {
                throw new SemanticException(node.getRight(), "Right side of '%s' must be numeric (actual: %s)", node.getType().getValue(), right);
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
                throw new SemanticException(node.getValue(), "Left side of LIKE expression must be a STRING (actual: %s)", value);
            }

            Type pattern = process(node.getPattern(), context);
            if (pattern != Type.STRING && pattern != Type.NULL) {
                throw new SemanticException(node.getValue(), "Pattern for LIKE expression must be a STRING (actual: %s)", pattern);
            }

            if (node.getEscape() != null) {
                Type escape = process(node.getEscape(), context);
                if (escape != Type.STRING && escape != Type.NULL) {
                    throw new SemanticException(node.getValue(), "Escape for LIKE expression must be a STRING (actual: %s)", escape);
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
        protected Type visitTimestampLiteral(TimestampLiteral node, Void context)
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
            Window window = node.getWindow().orNull();
            if (window != null) {
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
                throw new SemanticException(node.getExpression(), "Type of argument to extract must be LONG (actual %s)", type);
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
            throw new SemanticException(node.getValue(), "Between value, min and max must be the same type (value: %s, min: %s, max: %s)", value, min, max);
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
                    throw new SemanticException(node, "Cannot cast to type: " + node.getType());
            }
            subExpressionTypes.put(node, type);
            return type;
        }

        @Override
        protected Type visitInPredicate(InPredicate node, Void context)
        {
            // todo should values be the same type?
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
        protected Type visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + getClass().getName());
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
