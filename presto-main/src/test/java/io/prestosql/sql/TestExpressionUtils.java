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
package io.prestosql.sql;

import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.IsNullPredicate;
import io.prestosql.sql.tree.LikePredicate;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.NotExpression;
import io.prestosql.sql.tree.StringLiteral;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.sql.ExpressionUtils.normalize;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.NOT_EQUAL;
import static org.testng.Assert.assertEquals;

public class TestExpressionUtils
{
    @Test
    public void testAnd()
    {
        Expression a = name("a");
        Expression b = name("b");
        Expression c = name("c");
        Expression d = name("d");
        Expression e = name("e");

        assertEquals(
                ExpressionUtils.and(a, b, c, d, e),
                and(and(and(a, b), and(c, d)), e));

        assertEquals(
                ExpressionUtils.combineConjuncts(a, b, a, c, d, c, e),
                and(and(and(a, b), and(c, d)), e));
    }

    @Test
    public void testNormalize()
    {
        assertNormalize(new ComparisonExpression(EQUAL, name("a"), new LongLiteral("1")));
        assertNormalize(new IsNullPredicate(name("a")));
        assertNormalize(new NotExpression(new LikePredicate(name("a"), new StringLiteral("x%"), Optional.empty())));
        assertNormalize(
                new NotExpression(new ComparisonExpression(EQUAL, name("a"), new LongLiteral("1"))),
                new ComparisonExpression(NOT_EQUAL, name("a"), new LongLiteral("1")));
        assertNormalize(
                new NotExpression(new ComparisonExpression(NOT_EQUAL, name("a"), new LongLiteral("1"))),
                new ComparisonExpression(EQUAL, name("a"), new LongLiteral("1")));
        // Cannot normalize IS DISTINCT FROM yet
        assertNormalize(new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, name("a"), new LongLiteral("1"))));
    }

    private static void assertNormalize(Expression expression)
    {
        assertNormalize(expression, expression);
    }

    private static void assertNormalize(Expression expression, Expression normalized)
    {
        assertEquals(normalize(expression), normalized);
    }

    private static Identifier name(String name)
    {
        return new Identifier(name);
    }

    private LogicalBinaryExpression and(Expression left, Expression right)
    {
        return new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, left, right);
    }
}
