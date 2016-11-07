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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.scalar.FunctionAssertions.createExpression;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestInterpretedFilterFunction
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();

    @Test
    public void testNullLiteral()
    {
        assertFilter("null", false);
    }

    @Test
    public void testBooleanLiteral()
    {
        assertFilter("true", true);
        assertFilter("false", false);
    }

    @Test
    public void testNotExpression()
    {
        assertFilter("not true", false);
        assertFilter("not false", true);
        assertFilter("not null", false);
    }

    @Test
    public void testAndExpression()
    {
        assertFilter("true and true", true);
        assertFilter("true and false", false);
        assertFilter("true and null", false);

        assertFilter("false and true", false);
        assertFilter("false and false", false);
        assertFilter("false and null", false);

        assertFilter("null and true", false);
        assertFilter("null and false", false);
        assertFilter("null and null", false);
    }

    @Test
    public void testORExpression()
    {
        assertFilter("true or true", true);
        assertFilter("true or false", true);
        assertFilter("true or null", true);

        assertFilter("false or true", true);
        assertFilter("false or false", false);
        assertFilter("false or null", false);

        assertFilter("null or true", true);
        assertFilter("null or false", false);
        assertFilter("null or null", false);
    }

    @Test
    public void testIsNullExpression()
    {
        assertFilter("null is null", true);
        assertFilter("42 is null", false);
    }

    @Test
    public void testIsNotNullExpression()
    {
        assertFilter("42 is not null", true);
        assertFilter("null is not null", false);
    }

    @Test
    public void testComparisonExpression()
    {
        assertFilter("42 = 42", true);
        assertFilter("42 = 42.0", true);
        assertFilter("42.42 = 42.42", true);
        assertFilter("'foo' = 'foo'", true);

        assertFilter("42 = 87", false);
        assertFilter("42 = 22.2", false);
        assertFilter("42.42 = 22.2", false);
        assertFilter("'foo' = 'bar'", false);

        assertFilter("42 != 87", true);
        assertFilter("42 != 22.2", true);
        assertFilter("42.42 != 22.22", true);
        assertFilter("'foo' != 'bar'", true);

        assertFilter("42 != 42", false);
        assertFilter("42 != 42.0", false);
        assertFilter("42.42 != 42.42", false);
        assertFilter("'foo' != 'foo'", false);

        assertFilter("42 < 88", true);
        assertFilter("42 < 88.8", true);
        assertFilter("42.42 < 88.8", true);
        assertFilter("'bar' < 'foo'", true);

        assertFilter("88 < 42", false);
        assertFilter("88 < 42.42", false);
        assertFilter("88.8 < 42.42", false);
        assertFilter("'foo' < 'bar'", false);

        assertFilter("42 <= 88", true);
        assertFilter("42 <= 88.8", true);
        assertFilter("42.42 <= 88.8", true);
        assertFilter("'bar' <= 'foo'", true);

        assertFilter("42 <= 42", true);
        assertFilter("42 <= 42.0", true);
        assertFilter("42.42 <= 42.42", true);
        assertFilter("'foo' <= 'foo'", true);

        assertFilter("88 <= 42", false);
        assertFilter("88 <= 42.42", false);
        assertFilter("88.8 <= 42.42", false);
        assertFilter("'foo' <= 'bar'", false);

        assertFilter("88 >= 42", true);
        assertFilter("88.8 >= 42.0", true);
        assertFilter("88.8 >= 42.42", true);
        assertFilter("'foo' >= 'bar'", true);

        assertFilter("42 >= 88", false);
        assertFilter("42.42 >= 88.0", false);
        assertFilter("42.42 >= 88.88", false);
        assertFilter("'bar' >= 'foo'", false);

        assertFilter("88 >= 42", true);
        assertFilter("88.8 >= 42.0", true);
        assertFilter("88.8 >= 42.42", true);
        assertFilter("'foo' >= 'bar'", true);
        assertFilter("42 >= 42", true);
        assertFilter("42 >= 42.0", true);
        assertFilter("42.42 >= 42.42", true);
        assertFilter("'foo' >= 'foo'", true);

        assertFilter("42 >= 88", false);
        assertFilter("42.42 >= 88.0", false);
        assertFilter("42.42 >= 88.88", false);
        assertFilter("'bar' >= 'foo'", false);
    }

    @Test
    public void testComparisonExpressionWithNulls()
    {
        for (ComparisonExpressionType type : ComparisonExpressionType.values()) {
            if (type == ComparisonExpressionType.IS_DISTINCT_FROM) {
                // IS DISTINCT FROM has different NULL semantics
                continue;
            }

            assertFilter(format("NULL %s NULL", type.getValue()), false);

            assertFilter(format("42 %s NULL", type.getValue()), false);
            assertFilter(format("NULL %s 42", type.getValue()), false);

            assertFilter(format("11.1 %s NULL", type.getValue()), false);
            assertFilter(format("NULL %s 11.1", type.getValue()), false);
        }
    }

    public static void assertFilter(String expression, boolean expectedValue)
    {
        Expression parsed = createExpression(expression, METADATA, ImmutableMap.<Symbol, Type>of());

        InterpretedInternalFilterFunction filterFunction = new InterpretedInternalFilterFunction(parsed,
                ImmutableMap.<Symbol, Type>of(),
                ImmutableMap.<Symbol, Integer>of(),
                METADATA,
                SQL_PARSER,
                TEST_SESSION
        );

        boolean result = filterFunction.filter(0);
        assertEquals(result, expectedValue);
    }
}
