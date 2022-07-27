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
package com.facebook.presto.sql.tree;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.sql.QueryUtil.identifier;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.facebook.presto.sql.tree.IntervalLiteral.IntervalField.DAY;
import static com.facebook.presto.sql.tree.IntervalLiteral.Sign.POSITIVE;
import static org.testng.Assert.assertEquals;

public class TestTemplateExpressionRewriter
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testRewriteBinaryLiteral()
    {
        assertRewritten(new BinaryLiteral("abcdef"), new Parameter(0));
        assertRewritten("binField = X'abcdef'", "binField = ?");
    }

    @Test
    public void testRewriteBooleanLiteral()
    {
        assertRewritten(new BooleanLiteral("false"), new Parameter(0));
        assertRewritten("NOT false", "NOT ?");
    }

    @Test
    public void testRewriteCharLiteral()
    {
        assertRewritten(new CharLiteral("255"), new Parameter(0));
        assertRewritten("randomChar = CHAR '225'", "randomChar = ?");
    }

    @Test
    public void testRewriteDecimalLiteral()
    {
        assertRewritten(new DecimalLiteral("1.0"), new Parameter(0));
        assertRewritten("id > DECIMAL '1.0'", "id > ?");
        assertRewritten("2.0 > 1.0", "? > ?", new ParsingOptions(AS_DECIMAL));
    }

    @Test
    public void testRewriteDoubleLiteral()
    {
        assertEquals(TemplateExpressionRewriter.rewrite(new DoubleLiteral("1.0")), new Parameter(0));
        assertRewritten("id < 0.0", "id < ?", new ParsingOptions(AS_DOUBLE));
    }

    @Test
    public void testRewriteGenericLiteral()
    {
        assertRewritten(new GenericLiteral("varchar", "test"), new Parameter(0));
    }

    @Test
    public void testRewriteIntervalLiteral()
    {
        assertRewritten(new IntervalLiteral("33", POSITIVE, DAY, Optional.empty()), new Parameter(0));
        assertRewritten("date1 < date2 + INTERVAL '33' day to second", "date1 < date2 + ?");
    }

    @Test
    public void testRewriteLongLiteral()
    {
        assertRewritten(new LongLiteral("1"), new Parameter(0));

        Row values = new Row(Lists.newArrayList(new LongLiteral("11"), new LongLiteral("12")));
        Cast castValuesToIntegerColumns = new Cast(values, "ROW(col0 INTEGER,col1 INTEGER)");
        Expression before = new DereferenceExpression(castValuesToIntegerColumns, identifier("col0"));

        String after = "CAST(ROW(?, ?) AS ROW(col0 INTEGER, col1 INTEGER)).col0";

        assertRewritten(before, after);
    }

    @Test
    public void testRewriteStringLiteral()
    {
        assertRewritten(new StringLiteral("abc"), new Parameter(0));
        assertRewritten("p = 'x'", "p = ?");
    }

    @Test
    public void testRewriteTimeLiteral()
    {
        assertRewritten(new TimeLiteral("01:02:03.1234"), new Parameter(0));
        assertRewritten("time = '01:02:03.1234'", "time = ?");
    }

    @Test
    public void testRewriteTimestampLiteral()
    {
        assertRewritten(new TimestampLiteral("2022-01-01 01:02:03.1234"), new Parameter(0));
        assertRewritten("dateTime > '2022-01-01 01:02:03.1234'", "dateTime > ?");
    }

    @Test
    public void testRewriteNullLiteral()
    {
        assertRewritten(new NullLiteral(), new Parameter(0));
        assertRewritten("NOT null", "NOT ?");
    }

    @Test
    public void testRewriteTimeZoneExpression()
    {
        Expression before = new AtTimeZone(new TimestampLiteral("2022-01-01 01:02:03.1234"), new StringLiteral("US/Eastern"));
        Expression after = new AtTimeZone(new Parameter(0), new Parameter(1));
        assertRewritten(before, after);
    }

    private static void assertRewritten(Expression before, String after)
    {
        assertRewritten(TemplateExpressionRewriter.rewrite(before), SQL_PARSER.createExpression(after));
    }

    private static void assertRewritten(Expression before, Expression after)
    {
        assertEquals(TemplateExpressionRewriter.rewrite(before), after);
    }

    private static void assertRewritten(String before, String after)
    {
        assertRewritten(SQL_PARSER.createExpression(before), SQL_PARSER.createExpression(after));
    }

    private static void assertRewritten(String before, String after, ParsingOptions parsingOptions)
    {
        assertRewritten(SQL_PARSER.createExpression(before, new ParsingOptions(AS_DECIMAL)), SQL_PARSER.createExpression(after, parsingOptions));
    }
}
