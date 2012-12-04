package com.facebook.presto.sql.compiler;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.compiler.Field.nameGetter;
import static com.google.common.base.Predicates.equalTo;

public class ExpressionAnalyzer
{
    private final Metadata metadata;
    private final Map<Symbol, Type> symbols;

    public ExpressionAnalyzer(Metadata metadata, Map<Symbol, Type> symbols)
    {
        this.metadata = metadata;
        this.symbols = symbols;
    }

    public AnalyzedExpression analyze(Expression expression, TupleDescriptor sourceDescriptor)
    {
        Type type = new Visitor(metadata, symbols, sourceDescriptor).process(expression, null);

        Expression rewritten = TreeRewriter.rewriteWith(new NameToSymbolRewriter(sourceDescriptor), expression);

        return new AnalyzedExpression(type, rewritten);
    }

    private static class Visitor
            extends AstVisitor<Type, Void>
    {
        private final Metadata metadata;
        private final TupleDescriptor descriptor;
        private final Map<Symbol, Type> symbols;

        private Visitor(Metadata metadata, Map<Symbol, Type> symbols, TupleDescriptor descriptor)
        {
            this.metadata = metadata;
            this.descriptor = descriptor;
            this.symbols = symbols;
        }

        @Override
        protected Type visitQualifiedNameReference(QualifiedNameReference node, Void context)
        {
            // is this a known symbol?
            if (!node.getName().getPrefix().isPresent()) { // symbols can't have prefixes
                Type type = symbols.get(Symbol.fromQualifiedName(node.getName()));
                if (type != null) {
                    return type;
                }
            }

            List<Field> matches = descriptor.resolve(node.getName());
            if (matches.isEmpty()) {
                throw new SemanticException(node, "Attribute '%s' cannot be resolved", node.getName());
            }
            else if (matches.size() > 1) {
                throw new SemanticException(node, "Attribute '%s' is ambiguous. Possible matches: %s", node.getName(), Iterables.transform(matches, nameGetter()));
            }

            return Iterables.getOnlyElement(matches).getType();
        }

        @Override
        protected Type visitNotExpression(NotExpression node, Void context)
        {
            Type value = process(node.getValue(), context);
            if (value != Type.BOOLEAN) {
                throw new SemanticException(node.getValue(), "Value of logical NOT expression must evaluate to a BOOLEAN (actual: %s)", value);
            }
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

            return Type.BOOLEAN;
        }

        @Override
        protected Type visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            return Type.BOOLEAN;
        }

        @Override
        protected Type visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
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

            return first;
        }

        @Override
        protected Type visitCoalesceExpression(CoalesceExpression node, Void context)
        {
            List<Type> operandTypes = new ArrayList<>();
            for (Expression expression : node.getOperands()) {
                operandTypes.add(process(expression, context));
            }
            Type firstOperand = Iterables.get(operandTypes, 0);
            if (!Iterables.all(operandTypes, equalTo(firstOperand))) {
                // if they are all numeric return DOUBLE
                // todo rewrite this when we add proper type hierarchy
                if (Iterables.all(operandTypes, Predicates.or(equalTo(Type.DOUBLE), equalTo(Type.LONG)))){
                    return Type.DOUBLE;
                }
                throw new SemanticException(node, "All operands of coalesce must be the same type: %s", operandTypes);
            }
            return firstOperand;
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
                return Type.LONG;
            }

            return Type.DOUBLE;
        }

        @Override
        protected Type visitLikePredicate(LikePredicate node, Void context)
        {
            if (node.getEscape() != null) {
                throw new UnsupportedOperationException("not yet implemented: LIKE with ESCAPE");
            }

            Type value = process(node.getValue(), context);
            if (value != Type.STRING) {
                throw new SemanticException(node.getValue(), "Left side of LIKE expression must be a STRING (actual: %s)", value);
            }

            Type pattern = process(node.getPattern(), context);
            if (pattern != Type.STRING) {
                throw new SemanticException(node.getValue(), "Pattern for LIKE expression must be a STRING (actual: %s)", pattern);
            }

            return Type.BOOLEAN;
        }

        @Override
        protected Type visitStringLiteral(StringLiteral node, Void context)
        {
            return Type.STRING;
        }

        @Override
        protected Type visitLongLiteral(LongLiteral node, Void context)
        {
            return Type.LONG;
        }

        @Override
        protected Type visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            return Type.DOUBLE;
        }

        @Override
        protected Type visitNullLiteral(NullLiteral node, Void context)
        {
            return Type.NULL;
        }

        @Override
        protected Type visitFunctionCall(FunctionCall node, Void context)
        {
            ImmutableList.Builder<Type> argumentTypes = ImmutableList.builder();
            for (Expression expression : node.getArguments()) {
                argumentTypes.add(process(expression, context));
            }

            FunctionInfo function = metadata.getFunction(node.getName(), Lists.transform(argumentTypes.build(), Type.toRaw()));
            return Type.fromRaw(function.getReturnType());
        }

        @Override
        protected Type visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: BETWEEN");
        }

        @Override
        protected Type visitInPredicate(InPredicate node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: IN");
        }

        @Override
        protected Type visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + getClass().getName());
        }
    }
}
