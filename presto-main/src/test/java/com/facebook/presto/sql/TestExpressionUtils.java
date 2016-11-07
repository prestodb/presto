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
package com.facebook.presto.sql;

import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.StringLiteral;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.ExpressionUtils.normalize;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.IS_DISTINCT_FROM;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.NOT_EQUAL;
import static org.testng.Assert.assertEquals;

public class TestExpressionUtils
{
    @Test
    public void testNormalize()
            throws Exception
    {
        assertNormalize(new ComparisonExpression(EQUAL, name("a"), new LongLiteral("1")));
        assertNormalize(new IsNullPredicate(name("a")));
        assertNormalize(new NotExpression(new LikePredicate(name("a"), new StringLiteral("x%"), null)));
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

    private static QualifiedNameReference name(String name)
    {
        return new QualifiedNameReference(QualifiedName.of(name));
    }
}
