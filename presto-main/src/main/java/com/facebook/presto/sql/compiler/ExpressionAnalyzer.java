package com.facebook.presto.sql.compiler;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;

import static com.facebook.presto.sql.compiler.NamedSlot.nameGetter;

public class ExpressionAnalyzer
{
    private final SessionMetadata metadata;

    public ExpressionAnalyzer(SessionMetadata metadata)
    {
        this.metadata = metadata;
    }

    public AnalyzedExpression analyze(Expression expression, TupleDescriptor sourceDescriptor)
    {
        Type type = new Visitor(metadata).process(expression, sourceDescriptor);

        Expression rewritten = TreeRewriter.rewriteWith(new NameToSlotRewriter(sourceDescriptor), expression);

        return new AnalyzedExpression(type, rewritten);
    }

    private static class Visitor
            extends AstVisitor<Type, TupleDescriptor>
    {
        private final SessionMetadata metadata;

        private Visitor(SessionMetadata metadata)
        {
            this.metadata = metadata;
        }

        @Override
        protected Type visitQualifiedNameReference(QualifiedNameReference node, TupleDescriptor descriptor)
        {
            List<NamedSlot> matches = descriptor.resolve(node.getName());
            if (matches.isEmpty()) {
                throw new SemanticException(node, "Attribute '%s' cannot be resolved", node.getName());
            }
            else if (matches.size() > 1) {
                throw new SemanticException(node, "Attribute '%s' is ambiguous. Possible matches: %s", Iterables.transform(matches, nameGetter()));
            }

            return Iterables.getOnlyElement(matches).getSlot().getType();
        }

        @Override
        public Type visitSlotReference(SlotReference node, TupleDescriptor context)
        {
            return node.getSlot().getType();
        }

        @Override
        protected Type visitLogicalBinaryExpression(LogicalBinaryExpression node, TupleDescriptor context)
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
        protected Type visitComparisonExpression(ComparisonExpression node, TupleDescriptor context)
        {
            Type left = process(node.getLeft(), context);
            Type right = process(node.getRight(), context);

            if (left != right && !(Type.isNumeric(left) && Type.isNumeric(right))) {
                throw new SemanticException(node, "Types are not comparable with '%s': %s vs %s", node.getType().getValue(), left, right);
            }

            return Type.BOOLEAN;
        }

        @Override
        protected Type visitArithmeticExpression(ArithmeticExpression node, TupleDescriptor context)
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
        protected Type visitLikePredicate(LikePredicate node, TupleDescriptor context)
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
        protected Type visitStringLiteral(StringLiteral node, TupleDescriptor context)
        {
            return Type.STRING;
        }

        @Override
        protected Type visitLongLiteral(LongLiteral node, TupleDescriptor context)
        {
            return Type.LONG;
        }

        @Override
        protected Type visitDoubleLiteral(DoubleLiteral node, TupleDescriptor context)
        {
            return Type.DOUBLE;
        }

        @Override
        protected Type visitFunctionCall(FunctionCall node, TupleDescriptor context)
        {
            ImmutableList.Builder<Type> argumentTypes = ImmutableList.builder();
            for (Expression expression : node.getArguments()) {
                argumentTypes.add(process(expression, context));
            }

            FunctionInfo function = metadata.getFunction(node.getName(), argumentTypes.build());
            return Type.fromRaw(function.getReturnType());
        }

        @Override
        protected Type visitBetweenPredicate(BetweenPredicate node, TupleDescriptor context)
        {
            throw new UnsupportedOperationException("not yet implemented: BETWEEN");
        }

        @Override
        protected Type visitInPredicate(InPredicate node, TupleDescriptor context)
        {
            throw new UnsupportedOperationException("not yet implemented: IN");
        }

        @Override
        protected Type visitExpression(Expression node, TupleDescriptor context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + getClass().getName());
        }
    }
}
