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
package com.facebook.presto.split;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Optional;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.tree.ComparisonExpression.matchesPattern;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Predicates.or;

public final class ExpressionUtil
{
    private ExpressionUtil()
    {
    }

    /**
     * Extract constant values from expression.
     *
     * @return the constant values or absent if expression will always be false
     */
    public static Optional<Map<ColumnHandle, Object>> extractConstantValues(Expression predicate, Map<Symbol, ColumnHandle> symbolToColumnName)
    {
        // Look for any sub-expression in an AND expression of the form <partition key> = 'value'
        Set<ComparisonExpression> comparisons = IterableTransformer.on(extractConjuncts(predicate))
                .select(instanceOf(ComparisonExpression.class))
                .cast(ComparisonExpression.class)
                .select(or(
                        matchesPattern(ComparisonExpression.Type.EQUAL, QualifiedNameReference.class, StringLiteral.class),
                        matchesPattern(ComparisonExpression.Type.EQUAL, StringLiteral.class, QualifiedNameReference.class),
                        matchesPattern(ComparisonExpression.Type.EQUAL, QualifiedNameReference.class, LongLiteral.class),
                        matchesPattern(ComparisonExpression.Type.EQUAL, LongLiteral.class, QualifiedNameReference.class),
                        matchesPattern(ComparisonExpression.Type.EQUAL, QualifiedNameReference.class, DoubleLiteral.class),
                        matchesPattern(ComparisonExpression.Type.EQUAL, DoubleLiteral.class, QualifiedNameReference.class),
                        matchesPattern(ComparisonExpression.Type.EQUAL, QualifiedNameReference.class, BooleanLiteral.class),
                        matchesPattern(ComparisonExpression.Type.EQUAL, BooleanLiteral.class, QualifiedNameReference.class)))
                .set();

        final Map<ColumnHandle, Object> bindings = new HashMap<>();
        for (ComparisonExpression comparison : comparisons) {
            // Record binding if condition is an equality comparison over a partition key
            QualifiedNameReference reference = extractReference(comparison);
            Symbol symbol = Symbol.fromQualifiedName(reference.getName());

            ColumnHandle column = symbolToColumnName.get(symbol);
            if (column != null) {
                Literal literal = extractLiteral(comparison);

                Object value;
                if (literal instanceof DoubleLiteral) {
                    value = ((DoubleLiteral) literal).getValue();
                }
                else if (literal instanceof LongLiteral) {
                    value = ((LongLiteral) literal).getValue();
                }
                else if (literal instanceof StringLiteral) {
                    value = ((StringLiteral) literal).getValue();
                }
                else if (literal instanceof BooleanLiteral) {
                    value = ((BooleanLiteral) literal).getValue();
                }
                else {
                    throw new AssertionError(String.format("Literal type (%s) not currently handled", literal.getClass().getName()));
                }

                // if there is a different constant value already bound for this column, the expression will always be false
                Object previous = bindings.get(column);
                if (previous != null && !previous.equals(value)) {
                    return Optional.absent();
                }
                bindings.put(column, value);
            }
        }
        return Optional.of(bindings);
    }

    private static Literal extractLiteral(ComparisonExpression expression)
    {
        if (expression.getLeft() instanceof Literal) {
            return (Literal) expression.getLeft();
        }
        else if (expression.getRight() instanceof Literal) {
            return (Literal) expression.getRight();
        }

        throw new IllegalArgumentException("Comparison does not have a child of type Literal");
    }

    private static QualifiedNameReference extractReference(ComparisonExpression expression)
    {
        if (expression.getLeft() instanceof QualifiedNameReference) {
            return (QualifiedNameReference) expression.getLeft();
        }
        else if (expression.getRight() instanceof QualifiedNameReference) {
            return (QualifiedNameReference) expression.getRight();
        }

        throw new IllegalArgumentException("Comparison does not have a child of type QualifiedNameReference");
    }
}
