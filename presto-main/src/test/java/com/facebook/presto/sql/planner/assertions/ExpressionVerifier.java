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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Expression visitor which verifies if given expression (actual) is matching other expression given as context (expected).
 * Visitor returns true if plans match to each other.
 * <p/>
 * Note that actual expression is using real name references (table columns etc) while expected expression is using symbol aliases.
 * Given symbol alias can point only to one real name reference.
 * <p/>
 * Example:
 * <pre>
 * NOT (orderkey = 3 AND custkey = 3 AND orderkey < 10)
 * </pre>
 * will match to:
 * <pre>
 * NOT (X = 3 AND Y = 3 AND X < 10)
 * </pre>
 * , but will not match to:
 * <pre>
 * NOT (X = 3 AND Y = 3 AND Z < 10)
 * </pre>
 * nor  to
 * <pre>
 * NOT (X = 3 AND X = 3 AND X < 10)
 * </pre>
 */
final class ExpressionVerifier
        extends AstVisitor<Boolean, Expression>
{
    private final SymbolAliases symbolAliases;

    ExpressionVerifier(SymbolAliases symbolAliases)
    {
        this.symbolAliases = requireNonNull(symbolAliases, "symbolAliases is null");
    }

    @Override
    protected Boolean visitNode(Node node, Expression context)
    {
        throw new IllegalStateException(format("Node %s is not supported", node));
    }

    @Override
    protected Boolean visitCast(Cast actual, Expression expectedExpression)
    {
        if (!(expectedExpression instanceof Cast)) {
            return false;
        }

        Cast expected = (Cast) expectedExpression;

        if (!actual.getType().equals(expected.getType())) {
            return false;
        }

        return process(actual.getExpression(), expected.getExpression());
    }

    @Override
    protected Boolean visitInPredicate(InPredicate actual, Expression expectedExpression)
    {
        if (expectedExpression instanceof InPredicate) {
            InPredicate expected = (InPredicate) expectedExpression;

            if (actual.getValueList() instanceof InListExpression) {
                return process(actual.getValue(), expected.getValue()) && process(actual.getValueList(), expected.getValueList());
            }
            else {
                checkState(
                        expected.getValueList() instanceof InListExpression,
                        "ExpressionVerifier doesn't support unpacked expected values. Feel free to add support if needed");
                /*
                 * If the expected value is a value list, but the actual is e.g. a SymbolReference,
                 * we need to unpack the value from the list so that when we hit visitSymbolReference, the
                 * expected.toString() call returns something that the symbolAliases actually contains.
                 * For example, InListExpression.toString returns "(onlyitem)" rather than "onlyitem".
                 *
                 * This is required because actual passes through the analyzer, planner, and possibly optimizers,
                 * one of which sometimes takes the liberty of unpacking the InListExpression.
                 *
                 * Since the expected value doesn't go through all of that, we have to deal with the case
                 * of the actual value being unpacked, but the expected value being an InListExpression.
                 */
                List<Expression> values = ((InListExpression) expected.getValueList()).getValues();
                checkState(values.size() == 1, "Multiple expressions in expected value list %s, but actual value is not a list", values, actual.getValue());
                Expression onlyExpectedExpression = values.get(0);
                return process(actual.getValue(), expected.getValue()) && process(actual.getValueList(), onlyExpectedExpression);
            }
        }
        return false;
    }

    @Override
    protected Boolean visitComparisonExpression(ComparisonExpression actual, Expression expectedExpression)
    {
        if (expectedExpression instanceof ComparisonExpression) {
            ComparisonExpression expected = (ComparisonExpression) expectedExpression;
            if (actual.getType() == expected.getType()) {
                return process(actual.getLeft(), expected.getLeft()) && process(actual.getRight(), expected.getRight());
            }
        }
        return false;
    }

    @Override
    protected Boolean visitArithmeticBinary(ArithmeticBinaryExpression actual, Expression expectedExpression)
    {
        if (expectedExpression instanceof ArithmeticBinaryExpression) {
            ArithmeticBinaryExpression expected = (ArithmeticBinaryExpression) expectedExpression;
            if (actual.getType() == expected.getType()) {
                return process(actual.getLeft(), expected.getLeft()) && process(actual.getRight(), expected.getRight());
            }
        }
        return false;
    }

    protected Boolean visitGenericLiteral(GenericLiteral actual, Expression expected)
    {
        return getValueFromLiteral(actual).equals(getValueFromLiteral(expected));
    }

    @Override
    protected Boolean visitLongLiteral(LongLiteral actual, Expression expected)
    {
        return getValueFromLiteral(actual).equals(getValueFromLiteral(expected));
    }

    @Override
    protected Boolean visitDoubleLiteral(DoubleLiteral actual, Expression expected)
    {
        if (expected instanceof DoubleLiteral) {
            return getValueFromLiteral(actual).equals(getValueFromLiteral(expected));
        }

        return false;
    }

    @Override
    protected Boolean visitBooleanLiteral(BooleanLiteral actual, Expression expected)
    {
        if (expected instanceof BooleanLiteral) {
            return getValueFromLiteral(actual).equals(getValueFromLiteral(expected));
        }
        return false;
    }

    private static String getValueFromLiteral(Expression expression)
    {
        if (expression instanceof LongLiteral) {
            return String.valueOf(((LongLiteral) expression).getValue());
        }
        else if (expression instanceof BooleanLiteral) {
            return String.valueOf(((BooleanLiteral) expression).getValue());
        }
        else if (expression instanceof DoubleLiteral) {
            return String.valueOf(((DoubleLiteral) expression).getValue());
        }
        else if (expression instanceof GenericLiteral) {
            return ((GenericLiteral) expression).getValue();
        }
        else {
            throw new IllegalArgumentException("Unsupported literal expression type: " + expression.getClass().getName());
        }
    }

    @Override
    protected Boolean visitStringLiteral(StringLiteral actual, Expression expectedExpression)
    {
        if (expectedExpression instanceof StringLiteral) {
            StringLiteral expected = (StringLiteral) expectedExpression;
            return actual.getValue().equals(expected.getValue());
        }
        return false;
    }

    @Override
    protected Boolean visitLogicalBinaryExpression(LogicalBinaryExpression actual, Expression expectedExpression)
    {
        if (expectedExpression instanceof LogicalBinaryExpression) {
            LogicalBinaryExpression expected = (LogicalBinaryExpression) expectedExpression;
            if (actual.getType() == expected.getType()) {
                return process(actual.getLeft(), expected.getLeft()) && process(actual.getRight(), expected.getRight());
            }
        }
        return false;
    }

    @Override
    protected Boolean visitNotExpression(NotExpression actual, Expression expected)
    {
        if (expected instanceof NotExpression) {
            return process(actual.getValue(), ((NotExpression) expected).getValue());
        }
        return false;
    }

    @Override
    protected Boolean visitSymbolReference(SymbolReference actual, Expression expected)
    {
        if (!(expected instanceof SymbolReference)) {
            return false;
        }
        return symbolAliases.get(((SymbolReference) expected).getName()).equals(actual);
    }

    @Override
    protected Boolean visitCoalesceExpression(CoalesceExpression actual, Expression expected)
    {
        if (!(expected instanceof CoalesceExpression)) {
            return false;
        }

        CoalesceExpression expectedCoalesce = (CoalesceExpression) expected;
        if (actual.getOperands().size() == expectedCoalesce.getOperands().size()) {
            boolean verified = true;
            for (int i = 0; i < actual.getOperands().size(); i++) {
                verified &= process(actual.getOperands().get(i), expectedCoalesce.getOperands().get(i));
            }
            return verified;
        }
        return false;
    }
}
