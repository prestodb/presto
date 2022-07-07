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
    public void testRewriteBinaryLiteralNode()
    {
        Expression before = new BinaryLiteral("abcdef");
        Expression after = new Parameter(0);
        assertRewritten(before, after);
    }

    @Test
    public void testRewriteBinaryLiteralExpression()
    {
        assertRewrittenWithDecimalLiteralTreatment("BinField=X'abcdef'", "BinField=?");
    }

    @Test
    public void testRewriteBooleanLiteralNode()
    {
        Expression before = new BooleanLiteral("false");
        Expression after = new Parameter(0);
        assertRewritten(before, after);
    }

    @Test
    public void testRewriteBooleanLiteralExpression()
    {
        assertRewritten("NOT false", "NOT ?");
    }

    @Test
    public void testRewriteCharLiteralNode()
    {
        Expression before = new CharLiteral("255");
        Expression after = new Parameter(0);
        assertRewritten(before, after);
    }

    @Test
    public void testRewriteCharLiteralExpression()
    {
        assertRewritten("RandomChar = CHAR '225'", "RandomChar = ?");
    }

    @Test
    public void testRewriteDecimalLiteralNode()
    {
        Expression before = new DecimalLiteral("1.0");
        Expression after = new Parameter(0);
        assertRewritten(before, after);
    }

    @Test
    public void testRewriteDecimalLiteralExpression()
    {
        assertRewrittenWithDecimalLiteralTreatment("ID>1.0", "ID>?");
    }

    @Test
    public void testRewriteDecimalLiteralExpressionWithTwoLiterals()
    {
        assertRewrittenWithDecimalLiteralTreatment("2.0>1.0", "?>?");
    }

    @Test
    public void testRewriteDoubleLiteralNode()
    {
        Expression before = new DecimalLiteral("1.0");
        Expression after = new Parameter(0);
        assertEquals(TemplateExpressionRewriter.rewrite(before), after);
    }

    @Test
    public void testRewriteDoubleLiteralExpression()
    {
        assertRewrittenWithDoubleLiteralTreatment("ID<0.0", "ID<?");
    }

    @Test
    public void testRewriteGenericLiteralNode()
    {
        Expression before = new GenericLiteral("varchar", "test");
        Expression after = new Parameter(0);
        assertRewritten(before, after);
    }

    @Test
    public void testRewriteIntervalLiteralNode()
    {
        Expression before = new IntervalLiteral("33", POSITIVE, DAY, Optional.empty());
        Expression after = new Parameter(0);
        assertRewritten(before, after);
    }

    @Test
    public void testRewriteIntervalLiteralExpression()
    {
        assertRewritten("Date1 < Date2 + INTERVAL '33' day to second", "Date1 < Date2 + ?");
    }

    @Test
    public void testRewriteLongLiteralNode()
    {
        Expression before = new LongLiteral("1");
        Expression after = new Parameter(0);
        assertRewritten(before, after);
    }

    @Test
    public void testRewriteLongLiteralExpression()
    {
        Expression before = new DereferenceExpression(new Cast(new Row(Lists.newArrayList(new LongLiteral("11"), new LongLiteral("12"))), "ROW(COL0 INTEGER,COL1 INTEGER)"), identifier("col0"));
        Expression after = SQL_PARSER.createExpression("CAST(ROW(?, ?) AS ROW(COL0 INTEGER, COL1 INTEGER)).col0");
        assertRewritten(before, after);
    }

    @Test
    public void testRewriteStringLiteralNode()
    {
        Expression before = new StringLiteral("abc");
        Expression after = new Parameter(0);
        assertRewritten(before, after);
    }

    @Test
    public void testRewriteStringLiteralExpression()
    {
        Expression before = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new Identifier("p"), new StringLiteral("x"));
        Expression after = SQL_PARSER.createExpression("p = ?");
        assertRewritten(before, after);
    }

    @Test
    public void testRewriteTimeLiteralNode()
    {
        Expression before = new TimeLiteral("01:02:03.1234");
        Expression after = new Parameter(0);
        assertRewritten(before, after);
    }

    @Test
    public void testRewriteTimeLiteralExpression()
    {
        assertRewritten("Time = '01:02:03.1234'", "Time = ?");
    }

    @Test
    public void testRewriteTimestampLiteralNode()
    {
        Expression before = new TimestampLiteral("2022-01-01 01:02:03.1234");
        Expression after = new Parameter(0);
        assertRewritten(before, after);
    }

    @Test
    public void testRewriteTimestampLiteralExpression()
    {
        assertRewritten("DateTime > '2022-01-01 01:02:03.1234'", "DateTime > ?");
    }

    @Test
    public void testRewriteNullLiteralNode()
    {
        Expression before = new NullLiteral();
        Expression after = new Parameter(0);
        assertRewritten(before, after);
    }

    @Test
    public void testRewriteNullLiteralExpression()
    {
        assertRewritten("NOT null", "NOT ?");
    }

    private static void assertRewritten(Expression before, Expression after)
    {
        assertEquals(TemplateExpressionRewriter.rewrite(before), after);
    }

    private static void assertRewritten(String before, String after)
    {
        assertRewritten(SQL_PARSER.createExpression(before), SQL_PARSER.createExpression(after));
    }

    private static void assertRewrittenWithDecimalLiteralTreatment(String before, String after)
    {
        assertRewritten(SQL_PARSER.createExpression(before, new ParsingOptions(AS_DECIMAL)), SQL_PARSER.createExpression(after, new ParsingOptions(AS_DECIMAL)));
    }

    private static void assertRewrittenWithDoubleLiteralTreatment(String before, String after)
    {
        assertRewritten(SQL_PARSER.createExpression(before, new ParsingOptions(AS_DOUBLE)), SQL_PARSER.createExpression(after, new ParsingOptions(AS_DOUBLE)));
    }
}
