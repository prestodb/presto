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

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SymbolReference;

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
    protected Boolean visitLongLiteral(LongLiteral actual, Expression expectedExpression)
    {
        if (expectedExpression instanceof LongLiteral) {
            LongLiteral expected = (LongLiteral) expectedExpression;
            return actual.getValue() == expected.getValue();
        }
        return false;
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
    protected Boolean visitQualifiedNameReference(QualifiedNameReference actual, Expression expected)
    {
        if (isReference(expected)) {
            symbolAliases.put(asQualifiedName(expected).toString(), Symbol.from(actual));
            return true;
        }
        return false;
    }

    @Override
    protected Boolean visitSymbolReference(SymbolReference actual, Expression expected)
    {
        if (isReference(expected)) {
            symbolAliases.put(asQualifiedName(expected).toString(), Symbol.from(actual));
            return true;
        }
        return false;
    }

    private boolean isReference(Expression expression)
    {
        return expression instanceof DereferenceExpression || expression instanceof QualifiedNameReference || expression instanceof SymbolReference;
    }

    private QualifiedName asQualifiedName(Expression expression)
    {
        if (expression instanceof DereferenceExpression) {
            return DereferenceExpression.getQualifiedName((DereferenceExpression) expression);
        }
        else if (expression instanceof QualifiedNameReference) {
            return ((QualifiedNameReference) expression).getName();
        }
        else if (expression instanceof SymbolReference) {
            return QualifiedName.of(((SymbolReference) expression).getName());
        }
        else {
            throw new IllegalArgumentException("Expression is not a DereferenceExpression or QualifiedNameReference");
        }
    }
}
