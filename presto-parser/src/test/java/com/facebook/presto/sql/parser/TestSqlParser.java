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
package com.facebook.presto.sql.parser;

import com.facebook.presto.sql.tree.AddColumn;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.Approximate;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.AtTimeZone;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BinaryLiteral;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Call;
import com.facebook.presto.sql.tree.CallArgument;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CharLiteral;
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.Commit;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CreateSchema;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Cube;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.Deallocate;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.DescribeInput;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.DropSchema;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.Execute;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.ExplainFormat;
import com.facebook.presto.sql.tree.ExplainType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.Grant;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingSets;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.IntervalLiteral.IntervalField;
import com.facebook.presto.sql.tree.IntervalLiteral.Sign;
import com.facebook.presto.sql.tree.Isolation;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.LikeClause;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.RenameColumn;
import com.facebook.presto.sql.tree.RenameSchema;
import com.facebook.presto.sql.tree.RenameTable;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.sql.tree.Revoke;
import com.facebook.presto.sql.tree.Rollback;
import com.facebook.presto.sql.tree.Rollup;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.ShowCatalogs;
import com.facebook.presto.sql.tree.ShowPartitions;
import com.facebook.presto.sql.tree.ShowSchemas;
import com.facebook.presto.sql.tree.ShowSession;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.StartTransaction;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.sql.tree.TransactionAccessMode;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.sql.QueryUtil.query;
import static com.facebook.presto.sql.QueryUtil.row;
import static com.facebook.presto.sql.QueryUtil.selectList;
import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.sql.QueryUtil.subquery;
import static com.facebook.presto.sql.QueryUtil.table;
import static com.facebook.presto.sql.QueryUtil.values;
import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.sql.parser.IdentifierSymbol.AT_SIGN;
import static com.facebook.presto.sql.parser.IdentifierSymbol.COLON;
import static com.facebook.presto.sql.testing.TreeAssertions.assertFormattedSql;
import static com.facebook.presto.sql.tree.ArithmeticUnaryExpression.negative;
import static com.facebook.presto.sql.tree.ArithmeticUnaryExpression.positive;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.nCopies;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSqlParser
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testPosition()
            throws Exception
    {
        assertExpression("position('a' in 'b')",
                new FunctionCall(QualifiedName.of("strpos"), ImmutableList.of(
                        new StringLiteral("b"),
                        new StringLiteral("a"))));

        assertExpression("position('a' in ('b'))",
                new FunctionCall(QualifiedName.of("strpos"), ImmutableList.of(
                        new StringLiteral("b"),
                        new StringLiteral("a"))));
    }

    @Test
    public void testPossibleExponentialBacktracking()
            throws Exception
    {
        SQL_PARSER.createExpression("(((((((((((((((((((((((((((true)))))))))))))))))))))))))))");
    }

    @Test
    public void testQualifiedName()
    {
        assertEquals(QualifiedName.of("a", "b", "c", "d").toString(), "a.b.c.d");
        assertEquals(QualifiedName.of("A", "b", "C", "d").toString(), "a.b.c.d");
        assertTrue(QualifiedName.of("a", "b", "c", "d").hasSuffix(QualifiedName.of("b", "c", "d")));
        assertTrue(QualifiedName.of("a", "b", "c", "d").hasSuffix(QualifiedName.of("a", "b", "c", "d")));
        assertFalse(QualifiedName.of("a", "b", "c", "d").hasSuffix(QualifiedName.of("a", "c", "d")));
        assertFalse(QualifiedName.of("a", "b", "c", "d").hasSuffix(QualifiedName.of("z", "a", "b", "c", "d")));
        assertEquals(QualifiedName.of("a", "b", "c", "d"), QualifiedName.of("a", "b", "c", "d"));
    }

    @Test
    public void testGenericLiteral()
            throws Exception
    {
        assertGenericLiteral("VARCHAR");
        assertGenericLiteral("BIGINT");
        assertGenericLiteral("DOUBLE");
        assertGenericLiteral("BOOLEAN");
        assertGenericLiteral("DATE");
        assertGenericLiteral("foo");
    }

    @Test
    public void testBinaryLiteral()
            throws Exception
    {
        assertExpression("x' '", new BinaryLiteral(""));
        assertExpression("x''", new BinaryLiteral(""));
        assertExpression("X'abcdef1234567890ABCDEF'", new BinaryLiteral("abcdef1234567890ABCDEF"));

        // forms such as "X 'a b' " may look like BinaryLiteral
        // but they do not pass the syntax rule for BinaryLiteral
        // but instead conform to TypeConstructor, which generates a GenericLiteral expression
        assertInvalidExpression("X 'a b'", "Spaces are not allowed.*");
        assertInvalidExpression("X'a b c'", "Binary literal must contain an even number of digits.*");
        assertInvalidExpression("X'a z'", "Binary literal can only contain hexadecimal digits.*");
    }

    public static void assertGenericLiteral(String type)
    {
        assertExpression(type + " 'abc'", new GenericLiteral(type, "abc"));
    }

    @Test
    public void testLiterals()
            throws Exception
    {
        assertExpression("TIME" + " 'abc'", new TimeLiteral("abc"));
        assertExpression("TIMESTAMP" + " 'abc'", new TimestampLiteral("abc"));
        assertExpression("INTERVAL '33' day", new IntervalLiteral("33", Sign.POSITIVE, IntervalField.DAY, Optional.empty()));
        assertExpression("INTERVAL '33' day to second", new IntervalLiteral("33", Sign.POSITIVE, IntervalField.DAY, Optional.of(IntervalField.SECOND)));
        assertExpression("CHAR 'abc'", new CharLiteral("abc"));
    }

    @Test
    public void testArrayConstructor()
            throws Exception
    {
        assertExpression("ARRAY []", new ArrayConstructor(ImmutableList.<Expression>of()));
        assertExpression("ARRAY [1, 2]", new ArrayConstructor(ImmutableList.<Expression>of(new LongLiteral("1"), new LongLiteral("2"))));
        assertExpression("ARRAY [1.0, 2.5]", new ArrayConstructor(ImmutableList.<Expression>of(new DoubleLiteral("1.0"), new DoubleLiteral("2.5"))));
        assertExpression("ARRAY ['hi']", new ArrayConstructor(ImmutableList.<Expression>of(new StringLiteral("hi"))));
        assertExpression("ARRAY ['hi', 'hello']", new ArrayConstructor(ImmutableList.<Expression>of(new StringLiteral("hi"), new StringLiteral("hello"))));
    }

    @Test
    public void testArraySubscript()
            throws Exception
    {
        assertExpression("ARRAY [1, 2][1]", new SubscriptExpression(
                new ArrayConstructor(ImmutableList.<Expression>of(new LongLiteral("1"), new LongLiteral("2"))),
                new LongLiteral("1"))
        );
        try {
            assertExpression("CASE WHEN TRUE THEN ARRAY[1,2] END[1]", null);
            fail();
        }
        catch (RuntimeException e) {
            // Expected
        }
    }

    @Test
    public void testDouble()
            throws Exception
    {
        assertExpression("123.", new DoubleLiteral("123"));
        assertExpression("123.0", new DoubleLiteral("123"));
        assertExpression(".5", new DoubleLiteral(".5"));
        assertExpression("123.5", new DoubleLiteral("123.5"));

        assertExpression("123E7", new DoubleLiteral("123E7"));
        assertExpression("123.E7", new DoubleLiteral("123E7"));
        assertExpression("123.0E7", new DoubleLiteral("123E7"));
        assertExpression("123E+7", new DoubleLiteral("123E7"));
        assertExpression("123E-7", new DoubleLiteral("123E-7"));

        assertExpression("123.456E7", new DoubleLiteral("123.456E7"));
        assertExpression("123.456E+7", new DoubleLiteral("123.456E7"));
        assertExpression("123.456E-7", new DoubleLiteral("123.456E-7"));

        assertExpression(".4E42", new DoubleLiteral(".4E42"));
        assertExpression(".4E+42", new DoubleLiteral(".4E42"));
        assertExpression(".4E-42", new DoubleLiteral(".4E-42"));
    }

    @Test
    public void testCast()
            throws Exception
    {
        assertCast("foo(42, 55) ARRAY", "ARRAY(foo(42,55))");
        assertCast("varchar");
        assertCast("bigint");
        assertCast("BIGINT");
        assertCast("double");
        assertCast("DOUBLE");
        assertCast("DOUBLE PRECISION", "DOUBLE");
        assertCast("DOUBLE   PRECISION", "DOUBLE");
        assertCast("double precision", "DOUBLE");
        assertCast("boolean");
        assertCast("date");
        assertCast("time");
        assertCast("timestamp");
        assertCast("time with time zone");
        assertCast("timestamp with time zone");
        assertCast("foo");
        assertCast("FOO");

        assertCast("ARRAY<bigint>", "ARRAY(bigint)");
        assertCast("ARRAY<BIGINT>", "ARRAY(BIGINT)");
        assertCast("array<bigint>", "array(bigint)");
        assertCast("array < bigint  >", "ARRAY(bigint)");

        assertCast("ARRAY(bigint)");
        assertCast("ARRAY(BIGINT)");
        assertCast("array(bigint)");
        assertCast("array ( bigint  )", "ARRAY(bigint)");

        assertCast("array<array<bigint>>", "array(array(bigint))");
        assertCast("array(array(bigint))");

        assertCast("foo ARRAY", "ARRAY(foo)");
        assertCast("boolean array  array ARRAY", "ARRAY(ARRAY(ARRAY(boolean)))");
        assertCast("boolean ARRAY ARRAY ARRAY", "ARRAY(ARRAY(ARRAY(boolean)))");
        assertCast("ARRAY<boolean> ARRAY ARRAY", "ARRAY(ARRAY(ARRAY(boolean)))");

        assertCast("map(BIGINT,array(VARCHAR))");
        assertCast("map<BIGINT,array<VARCHAR>>", "map(BIGINT,array(VARCHAR))");

        assertCast("varchar(42)");
        assertCast("foo(42,55)");
        assertCast("foo(BIGINT,array(VARCHAR))");
        assertCast("ARRAY<varchar(42)>", "ARRAY(varchar(42))");
        assertCast("ARRAY<foo(42,55)>", "ARRAY(foo(42,55))");
        assertCast("varchar(42) ARRAY", "ARRAY(varchar(42))");
        assertCast("foo(42, 55) ARRAY", "ARRAY(foo(42,55))");

        assertCast("ROW(m DOUBLE)", "ROW(m DOUBLE)");
        assertCast("ROW(m DOUBLE)");
        assertCast("ROW(x BIGINT,y DOUBLE)");
        assertCast("ROW(x BIGINT, y DOUBLE)", "ROW(x bigint,y double)");
        assertCast("ROW(x BIGINT, y DOUBLE, z ROW(m array<bigint>,n map<double,timestamp>))", "ROW(x BIGINT,y DOUBLE,z ROW(m array(bigint),n map(double,timestamp)))");
        assertCast("array<ROW(x BIGINT, y TIMESTAMP)>", "ARRAY(ROW(x BIGINT,y TIMESTAMP))");
    }

    @Test
    public void testArithmeticUnary()
    {
        assertExpression("9", new LongLiteral("9"));

        assertExpression("+9", positive(new LongLiteral("9")));
        assertExpression("+ 9", positive(new LongLiteral("9")));

        assertExpression("++9", positive(positive(new LongLiteral("9"))));
        assertExpression("+ +9", positive(positive(new LongLiteral("9"))));
        assertExpression("+ + 9", positive(positive(new LongLiteral("9"))));

        assertExpression("+++9", positive(positive(positive(new LongLiteral("9")))));
        assertExpression("+ + +9", positive(positive(positive(new LongLiteral("9")))));
        assertExpression("+ + + 9", positive(positive(positive(new LongLiteral("9")))));

        assertExpression("-9", negative(new LongLiteral("9")));
        assertExpression("- 9", negative(new LongLiteral("9")));

        assertExpression("- + 9", negative(positive(new LongLiteral("9"))));
        assertExpression("-+9", negative(positive(new LongLiteral("9"))));

        assertExpression("+ - + 9", positive(negative(positive(new LongLiteral("9")))));
        assertExpression("+-+9", positive(negative(positive(new LongLiteral("9")))));

        assertExpression("- -9", negative(negative(new LongLiteral("9"))));
        assertExpression("- - 9", negative(negative(new LongLiteral("9"))));

        assertExpression("- + - + 9", negative(positive(negative(positive(new LongLiteral("9"))))));
        assertExpression("-+-+9", negative(positive(negative(positive(new LongLiteral("9"))))));

        assertExpression("+ - + - + 9", positive(negative(positive(negative(positive(new LongLiteral("9")))))));
        assertExpression("+-+-+9", positive(negative(positive(negative(positive(new LongLiteral("9")))))));

        assertExpression("- - -9", negative(negative(negative(new LongLiteral("9")))));
        assertExpression("- - - 9", negative(negative(negative(new LongLiteral("9")))));
    }

    @Test
    public void testDoubleInQuery()
    {
        assertStatement("SELECT 123.456E7 FROM DUAL",
                simpleQuery(
                        selectList(new DoubleLiteral("123.456E7")),
                        table(QualifiedName.of("DUAL"))));
    }

    @Test
    public void testIntersect()
    {
        assertStatement("SELECT 123 INTERSECT DISTINCT SELECT 123 INTERSECT ALL SELECT 123",
                new Query(
                        Optional.empty(),
                        new Intersect(ImmutableList.of(
                                new Intersect(ImmutableList.of(createSelect123(), createSelect123()), true),
                                createSelect123()
                        ), false),
                        ImmutableList.of(),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Test
    public void testUnion()
    {
        assertStatement("SELECT 123 UNION DISTINCT SELECT 123 UNION ALL SELECT 123",
                new Query(
                        Optional.empty(),
                        new Union(ImmutableList.of(
                                new Union(ImmutableList.of(createSelect123(), createSelect123()), true),
                                createSelect123()
                        ), false),
                        ImmutableList.<SortItem>of(),
                        Optional.empty(),
                        Optional.empty()));
    }

    private static QuerySpecification createSelect123()
    {
        return new QuerySpecification(
                selectList(new LongLiteral("123")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                Optional.empty()
        );
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertExpression("1 BETWEEN 2 AND 3", new BetweenPredicate(new LongLiteral("1"), new LongLiteral("2"), new LongLiteral("3")));
        assertExpression("1 NOT BETWEEN 2 AND 3", new NotExpression(new BetweenPredicate(new LongLiteral("1"), new LongLiteral("2"), new LongLiteral("3"))));
    }

    @Test
    public void testLimitAll()
    {
        Query valuesQuery = query(values(
                row(new LongLiteral("1"), new StringLiteral("1")),
                row(new LongLiteral("2"), new StringLiteral("2"))));

        assertStatement("SELECT * FROM (VALUES (1, '1'), (2, '2')) LIMIT ALL",
                simpleQuery(selectList(new AllColumns()),
                        subquery(valuesQuery),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(),
                        Optional.of("ALL")));
    }

    @Test
    public void testValues()
    {
        Query valuesQuery = query(values(
                row(new StringLiteral("a"), new LongLiteral("1"), new DoubleLiteral("2.2")),
                row(new StringLiteral("b"), new LongLiteral("2"), new DoubleLiteral("3.3"))));

        assertStatement("VALUES ('a', 1, 2.2), ('b', 2, 3.3)", valuesQuery);

        assertStatement("SELECT * FROM (VALUES ('a', 1, 2.2), ('b', 2, 3.3))",
                simpleQuery(
                        selectList(new AllColumns()),
                        subquery(valuesQuery)));
    }

    @Test
    public void testPrecedenceAndAssociativity()
            throws Exception
    {
        assertExpression("1 AND 2 OR 3", new LogicalBinaryExpression(LogicalBinaryExpression.Type.OR,
                new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND,
                        new LongLiteral("1"),
                        new LongLiteral("2")),
                new LongLiteral("3")));

        assertExpression("1 OR 2 AND 3", new LogicalBinaryExpression(LogicalBinaryExpression.Type.OR,
                new LongLiteral("1"),
                new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND,
                        new LongLiteral("2"),
                        new LongLiteral("3"))));

        assertExpression("NOT 1 AND 2", new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND,
                new NotExpression(new LongLiteral("1")),
                new LongLiteral("2")));

        assertExpression("NOT 1 OR 2", new LogicalBinaryExpression(LogicalBinaryExpression.Type.OR,
                new NotExpression(new LongLiteral("1")),
                new LongLiteral("2")));

        assertExpression("-1 + 2", new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.ADD,
                negative(new LongLiteral("1")),
                new LongLiteral("2")));

        assertExpression("1 - 2 - 3", new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.SUBTRACT,
                new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.SUBTRACT,
                        new LongLiteral("1"),
                        new LongLiteral("2")),
                new LongLiteral("3")));

        assertExpression("1 / 2 / 3", new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.DIVIDE,
                new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.DIVIDE,
                        new LongLiteral("1"),
                        new LongLiteral("2")),
                new LongLiteral("3")));

        assertExpression("1 + 2 * 3", new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.ADD,
                new LongLiteral("1"),
                new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.MULTIPLY,
                        new LongLiteral("2"),
                        new LongLiteral("3"))));
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:1: no viable alternative at input '<EOF>'")
    public void testEmptyExpression()
    {
        SQL_PARSER.createExpression("");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:1: no viable alternative at input '<EOF>'")
    public void testEmptyStatement()
    {
        SQL_PARSER.createStatement("");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:7: extraneous input 'x' expecting\\E.*")
    public void testExpressionWithTrailingJunk()
    {
        SQL_PARSER.createExpression("1 + 1 x");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:1: no viable alternative at input '@'")
    public void testTokenizeErrorStartOfLine()
    {
        SQL_PARSER.createStatement("@select");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:25: no viable alternative at input '@'")
    public void testTokenizeErrorMiddleOfLine()
    {
        SQL_PARSER.createStatement("select * from foo where @what");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:15: no viable alternative at input\\E.*")
    public void testTokenizeErrorIncompleteToken()
    {
        SQL_PARSER.createStatement("select * from 'oops");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 3:1: extraneous input 'from' expecting\\E.*")
    public void testParseErrorStartOfLine()
    {
        SQL_PARSER.createStatement("select *\nfrom x\nfrom");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 3:7: no viable alternative at input 'from'")
    public void testParseErrorMiddleOfLine()
    {
        SQL_PARSER.createStatement("select *\nfrom x\nwhere from");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:14: no viable alternative at input '<EOF>'")
    public void testParseErrorEndOfInput()
    {
        SQL_PARSER.createStatement("select * from");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:16: no viable alternative at input '<EOF>'")
    public void testParseErrorEndOfInputWhitespace()
    {
        SQL_PARSER.createStatement("select * from  ");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:15: backquoted identifiers are not supported; use double quotes to quote identifiers")
    public void testParseErrorBackquotes()
    {
        SQL_PARSER.createStatement("select * from `foo`");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:19: backquoted identifiers are not supported; use double quotes to quote identifiers")
    public void testParseErrorBackquotesEndOfInput()
    {
        SQL_PARSER.createStatement("select * from foo `bar`");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:8: identifiers must not start with a digit; surround the identifier with double quotes")
    public void testParseErrorDigitIdentifiers()
    {
        SQL_PARSER.createStatement("select 1x from dual");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:15: identifiers must not contain '@'")
    public void testIdentifierWithAtSign()
    {
        SQL_PARSER.createStatement("select * from foo@bar");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:15: identifiers must not contain ':'")
    public void testIdentifierWithColon()
    {
        SQL_PARSER.createStatement("select * from foo:bar");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:35: mismatched input 'order' expecting .*")
    public void testParseErrorDualOrderBy()
    {
        SQL_PARSER.createStatement("select fuu from dual order by fuu order by fuu");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:31: mismatched input 'order' expecting <EOF>")
    public void testParseErrorReverseOrderByLimit()
    {
        SQL_PARSER.createStatement("select fuu from dual limit 10 order by fuu");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:1: Invalid numeric literal: 12223222232535343423232435343")
    public void testParseErrorInvalidPositiveLongCast()
    {
        SQL_PARSER.createStatement("select CAST(12223222232535343423232435343 AS BIGINT)");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:1: Invalid numeric literal: 12223222232535343423232435343")
    public void testParseErrorInvalidNegativeLongCast()
    {
        SQL_PARSER.createStatement("select CAST(-12223222232535343423232435343 AS BIGINT)");
    }

    @Test
    public void testParsingExceptionPositionInfo()
    {
        try {
            SQL_PARSER.createStatement("select *\nfrom x\nwhere from");
            fail("expected exception");
        }
        catch (ParsingException e) {
            assertEquals(e.getMessage(), "line 3:7: no viable alternative at input 'from'");
            assertEquals(e.getErrorMessage(), "no viable alternative at input 'from'");
            assertEquals(e.getLineNumber(), 3);
            assertEquals(e.getColumnNumber(), 7);
        }
    }

    @Test
    public void testAllowIdentifierColon()
    {
        SqlParser sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(COLON));
        sqlParser.createStatement("select * from foo:bar");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:12: no viable alternative at input\\E.*")
    public void testInvalidArguments()
    {
        SQL_PARSER.createStatement("select foo(,1)");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:20: no viable alternative at input\\E.*")
    public void testInvalidArguments2()
    {
        SQL_PARSER.createStatement("select foo(DISTINCT)");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:21: no viable alternative at input\\E.*")
    public void testInvalidArguments3()
    {
        SQL_PARSER.createStatement("select foo(DISTINCT ,1)");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testAllowIdentifierAtSign()
    {
        SqlParser sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(AT_SIGN));
        sqlParser.createStatement("select * from foo@bar");
    }

    @Test
    public void testInterval()
            throws Exception
    {
        assertExpression("INTERVAL '123' YEAR", new IntervalLiteral("123", Sign.POSITIVE, IntervalField.YEAR));
        assertExpression("INTERVAL '123-3' YEAR TO MONTH", new IntervalLiteral("123-3", Sign.POSITIVE, IntervalField.YEAR, Optional.of(IntervalField.MONTH)));
        assertExpression("INTERVAL '123' MONTH", new IntervalLiteral("123", Sign.POSITIVE, IntervalField.MONTH));
        assertExpression("INTERVAL '123' DAY", new IntervalLiteral("123", Sign.POSITIVE, IntervalField.DAY));
        assertExpression("INTERVAL '123 23:58:53.456' DAY TO SECOND", new IntervalLiteral("123 23:58:53.456", Sign.POSITIVE, IntervalField.DAY, Optional.of(IntervalField.SECOND)));
        assertExpression("INTERVAL '123' HOUR", new IntervalLiteral("123", Sign.POSITIVE, IntervalField.HOUR));
        assertExpression("INTERVAL '23:59' HOUR TO MINUTE", new IntervalLiteral("23:59", Sign.POSITIVE, IntervalField.HOUR, Optional.of(IntervalField.MINUTE)));
        assertExpression("INTERVAL '123' MINUTE", new IntervalLiteral("123", Sign.POSITIVE, IntervalField.MINUTE));
        assertExpression("INTERVAL '123' SECOND", new IntervalLiteral("123", Sign.POSITIVE, IntervalField.SECOND));
    }

    @Test
    public void testDecimal()
            throws Exception
    {
        assertExpression("DECIMAL '12.34'", new DecimalLiteral("12.34"));
        assertExpression("DECIMAL '12.'", new DecimalLiteral("12."));
        assertExpression("DECIMAL '12'", new DecimalLiteral("12"));
        assertExpression("DECIMAL '.34'", new DecimalLiteral(".34"));
        assertExpression("DECIMAL '+12.34'", new DecimalLiteral("+12.34"));
        assertExpression("DECIMAL '+12'", new DecimalLiteral("+12"));
        assertExpression("DECIMAL '-12.34'", new DecimalLiteral("-12.34"));
        assertExpression("DECIMAL '-12'", new DecimalLiteral("-12"));
        assertExpression("DECIMAL '+.34'", new DecimalLiteral("+.34"));
        assertExpression("DECIMAL '-.34'", new DecimalLiteral("-.34"));
    }

    @Test
    public void testTime()
            throws Exception
    {
        assertExpression("TIME '03:04:05'", new TimeLiteral("03:04:05"));
    }

    @Test
    public void testCurrentTimestamp()
            throws Exception
    {
        assertExpression("CURRENT_TIMESTAMP", new CurrentTime(CurrentTime.Type.TIMESTAMP));
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:1: expression is too large \\(stack overflow while parsing\\)")
    public void testStackOverflowExpression()
    {
        for (int size = 3000; size <= 100_000; size *= 2) {
            SQL_PARSER.createExpression(Joiner.on(" OR ").join(nCopies(size, "x = y")));
        }
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:1: statement is too large \\(stack overflow while parsing\\)")
    public void testStackOverflowStatement()
    {
        for (int size = 6000; size <= 100_000; size *= 2) {
            SQL_PARSER.createStatement("SELECT " + Joiner.on(" OR ").join(nCopies(size, "x = y")));
        }
    }

    @Test
    public void testSetSession()
            throws Exception
    {
        assertStatement("SET SESSION foo = 'bar'", new SetSession(QualifiedName.of("foo"), new StringLiteral("bar")));
        assertStatement("SET SESSION foo.bar = 'baz'", new SetSession(QualifiedName.of("foo", "bar"), new StringLiteral("baz")));
        assertStatement("SET SESSION foo.bar.boo = 'baz'", new SetSession(QualifiedName.of("foo", "bar", "boo"), new StringLiteral("baz")));

        assertStatement("SET SESSION foo.bar = 'ban' || 'ana'", new SetSession(
                QualifiedName.of("foo", "bar"),
                new FunctionCall(QualifiedName.of("concat"), ImmutableList.of(
                        new StringLiteral("ban"),
                        new StringLiteral("ana")))));
    }

    @Test
    public void testResetSession()
            throws Exception
    {
        assertStatement("RESET SESSION foo.bar", new ResetSession(QualifiedName.of("foo", "bar")));
        assertStatement("RESET SESSION foo", new ResetSession(QualifiedName.of("foo")));
    }

    @Test
    public void testShowSession()
            throws Exception
    {
        assertStatement("SHOW SESSION", new ShowSession());
    }

    @Test
    public void testShowCatalogs()
            throws Exception
    {
        assertStatement("SHOW CATALOGS", new ShowCatalogs(Optional.empty()));
        assertStatement("SHOW CATALOGS LIKE '%'", new ShowCatalogs(Optional.of("%")));
    }

    @Test
    public void testShowSchemas()
            throws Exception
    {
        assertStatement("SHOW SCHEMAS", new ShowSchemas(Optional.<String>empty(), Optional.empty()));
        assertStatement("SHOW SCHEMAS FROM foo", new ShowSchemas(Optional.of("foo"), Optional.empty()));
        assertStatement("SHOW SCHEMAS IN foo LIKE '%'", new ShowSchemas(Optional.of("foo"), Optional.of("%")));
    }

    @Test
    public void testShowTables()
            throws Exception
    {
        assertStatement("SHOW TABLES", new ShowTables(Optional.empty(), Optional.empty()));
        assertStatement("SHOW TABLES FROM a", new ShowTables(Optional.of(QualifiedName.of("a")), Optional.empty()));
        assertStatement("SHOW TABLES IN a LIKE '%'", new ShowTables(Optional.of(QualifiedName.of("a")), Optional.of("%")));
    }

    @Test
    public void testShowPartitions()
    {
        assertStatement("SHOW PARTITIONS FROM t", new ShowPartitions(QualifiedName.of("t"), Optional.empty(), ImmutableList.of(), Optional.empty()));

        assertStatement("SHOW PARTITIONS FROM t WHERE x = 1",
                new ShowPartitions(
                        QualifiedName.of("t"),
                        Optional.of(new ComparisonExpression(ComparisonExpression.Type.EQUAL, new QualifiedNameReference(QualifiedName.of("x")), new LongLiteral("1"))),
                        ImmutableList.of(),
                        Optional.empty()));

        assertStatement("SHOW PARTITIONS FROM t WHERE x = 1 ORDER BY y",
                new ShowPartitions(
                        QualifiedName.of("t"),
                        Optional.of(new ComparisonExpression(ComparisonExpression.Type.EQUAL, new QualifiedNameReference(QualifiedName.of("x")), new LongLiteral("1"))),
                        ImmutableList.of(new SortItem(new QualifiedNameReference(QualifiedName.of("y")), SortItem.Ordering.ASCENDING, SortItem.NullOrdering.UNDEFINED)),
                        Optional.empty()));

        assertStatement("SHOW PARTITIONS FROM t WHERE x = 1 ORDER BY y LIMIT 10",
                new ShowPartitions(
                        QualifiedName.of("t"),
                        Optional.of(new ComparisonExpression(ComparisonExpression.Type.EQUAL, new QualifiedNameReference(QualifiedName.of("x")), new LongLiteral("1"))),
                        ImmutableList.of(new SortItem(new QualifiedNameReference(QualifiedName.of("y")), SortItem.Ordering.ASCENDING, SortItem.NullOrdering.UNDEFINED)),
                        Optional.of("10")));

        assertStatement("SHOW PARTITIONS FROM t WHERE x = 1 ORDER BY y LIMIT ALL",
                new ShowPartitions(
                        QualifiedName.of("t"),
                        Optional.of(new ComparisonExpression(ComparisonExpression.Type.EQUAL, new QualifiedNameReference(QualifiedName.of("x")), new LongLiteral("1"))),
                        ImmutableList.of(new SortItem(new QualifiedNameReference(QualifiedName.of("y")), SortItem.Ordering.ASCENDING, SortItem.NullOrdering.UNDEFINED)),
                        Optional.of("ALL")));
    }

    @Test
    public void testSubstringBuiltInFunction()
    {
        final String givenString = "ABCDEF";
        assertStatement(format("SELECT substring('%s' FROM 2)", givenString),
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(new FunctionCall(QualifiedName.of("substr"), Lists.newArrayList(new StringLiteral(givenString), new LongLiteral("2")))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty()),
                        ImmutableList.<SortItem>of(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement(format("SELECT substring('%s' FROM 2 FOR 3)", givenString),
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(new FunctionCall(QualifiedName.of("substr"), Lists.newArrayList(new StringLiteral(givenString), new LongLiteral("2"), new LongLiteral("3")))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty()),
                        ImmutableList.<SortItem>of(),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Test
    public void testSubstringRegisteredFunction()
    {
        final String givenString = "ABCDEF";
        assertStatement(format("SELECT substring('%s', 2)", givenString),
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(new FunctionCall(QualifiedName.of("substring"), Lists.newArrayList(new StringLiteral(givenString), new LongLiteral("2")))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty()),
                        ImmutableList.<SortItem>of(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement(format("SELECT substring('%s', 2, 3)", givenString),
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(new FunctionCall(QualifiedName.of("substring"), Lists.newArrayList(new StringLiteral(givenString), new LongLiteral("2"), new LongLiteral("3")))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty()),
                        ImmutableList.<SortItem>of(),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Test
    public void testSelectWithRowType()
            throws Exception
    {
        assertStatement("SELECT col1.f1, col2, col3.f1.f2.f3 FROM table1",
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(
                                        new DereferenceExpression(new QualifiedNameReference(QualifiedName.of("col1")), "f1"),
                                        new QualifiedNameReference(QualifiedName.of("col2")),
                                        new DereferenceExpression(
                                                new DereferenceExpression(new DereferenceExpression(new QualifiedNameReference(QualifiedName.of("col3")), "f1"), "f2"), "f3")),
                                Optional.of(new Table(QualifiedName.of("table1"))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty()),
                        ImmutableList.<SortItem>of(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT col1.f1[0], col2, col3[2].f2.f3, col4[4] FROM table1",
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(
                                        new SubscriptExpression(new DereferenceExpression(new QualifiedNameReference(QualifiedName.of("col1")), "f1"), new LongLiteral("0")),
                                        new QualifiedNameReference(QualifiedName.of("col2")),
                                        new DereferenceExpression(new DereferenceExpression(new SubscriptExpression(new QualifiedNameReference(QualifiedName.of("col3")), new LongLiteral("2")), "f2"), "f3"),
                                        new SubscriptExpression(new QualifiedNameReference(QualifiedName.of("col4")), new LongLiteral("4"))
                                ),
                                Optional.of(new Table(QualifiedName.of("table1"))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty()),
                        ImmutableList.<SortItem>of(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT CAST(ROW(11, 12) AS ROW(COL0 INTEGER, COL1 INTEGER)).col0",
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(
                                        new DereferenceExpression(new Cast(new Row(Lists.newArrayList(new LongLiteral("11"), new LongLiteral("12"))), "ROW(COL0 INTEGER,COL1 INTEGER)"), "col0")
                                ),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty()),
                        ImmutableList.<SortItem>of(),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Test
    public void testSelectWithGroupBy()
            throws Exception
    {
        assertStatement("SELECT * FROM table1 GROUP BY a",
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(new AllColumns()),
                                Optional.of(new Table(QualifiedName.of("table1"))),
                                Optional.empty(),
                                Optional.of(new GroupBy(false, ImmutableList.of(new SimpleGroupBy(ImmutableList.of(new QualifiedNameReference(QualifiedName.of("a"))))))),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty()),
                        ImmutableList.<SortItem>of(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 GROUP BY a, b",
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(new AllColumns()),
                                Optional.of(new Table(QualifiedName.of("table1"))),
                                Optional.empty(),
                                Optional.of(new GroupBy(false, ImmutableList.of(
                                        new SimpleGroupBy(ImmutableList.of(new QualifiedNameReference(QualifiedName.of("a")))),
                                        new SimpleGroupBy(ImmutableList.of(new QualifiedNameReference(QualifiedName.of("b"))))))),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty()),
                        ImmutableList.<SortItem>of(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 GROUP BY ()",
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(new AllColumns()),
                                Optional.of(new Table(QualifiedName.of("table1"))),
                                Optional.empty(),
                                Optional.of(new GroupBy(false, ImmutableList.of(new SimpleGroupBy(ImmutableList.of())))),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty()),
                        ImmutableList.<SortItem>of(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 GROUP BY GROUPING SETS (a)",
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(new AllColumns()),
                                Optional.of(new Table(QualifiedName.of("table1"))),
                                Optional.empty(),
                                Optional.of(new GroupBy(false, ImmutableList.of(new GroupingSets(ImmutableList.of(ImmutableList.of(QualifiedName.of("a"))))))),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty()),
                        ImmutableList.<SortItem>of(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 GROUP BY ALL GROUPING SETS ((a, b), (a), ()), CUBE (c), ROLLUP (d)",
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(new AllColumns()),
                                Optional.of(new Table(QualifiedName.of("table1"))),
                                Optional.empty(),
                                Optional.of(new GroupBy(false, ImmutableList.of(
                                        new GroupingSets(
                                                ImmutableList.of(ImmutableList.of(QualifiedName.of("a"), QualifiedName.of("b")),
                                                        ImmutableList.of(QualifiedName.of("a")),
                                                        ImmutableList.of())),
                                        new Cube(ImmutableList.of(QualifiedName.of("c"))),
                                        new Rollup(ImmutableList.of(QualifiedName.of("d")))))),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty()),
                        ImmutableList.<SortItem>of(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 GROUP BY DISTINCT GROUPING SETS ((a, b), (a), ()), CUBE (c), ROLLUP (d)",
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(new AllColumns()),
                                Optional.of(new Table(QualifiedName.of("table1"))),
                                Optional.empty(),
                                Optional.of(new GroupBy(true, ImmutableList.of(
                                        new GroupingSets(
                                                ImmutableList.of(ImmutableList.of(QualifiedName.of("a"), QualifiedName.of("b")),
                                                        ImmutableList.of(QualifiedName.of("a")),
                                                        ImmutableList.of())),
                                        new Cube(ImmutableList.of(QualifiedName.of("c"))),
                                        new Rollup(ImmutableList.of(QualifiedName.of("d")))))),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty()),
                        ImmutableList.<SortItem>of(),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Test
    public void testCreateSchema()
    {
        assertStatement("CREATE SCHEMA test",
                new CreateSchema(QualifiedName.of("test"), false, ImmutableMap.of()));

        assertStatement("CREATE SCHEMA IF NOT EXISTS test",
                new CreateSchema(QualifiedName.of("test"), true, ImmutableMap.of()));

        assertStatement("CREATE SCHEMA test WITH (a = 'apple', b = 123)",
                new CreateSchema(
                        QualifiedName.of("test"),
                        false,
                        ImmutableMap.of(
                                "a", new StringLiteral("apple"),
                                "b", new LongLiteral("123"))));
    }

    @Test
    public void testDropSchema()
    {
        assertStatement("DROP SCHEMA test",
                new DropSchema(QualifiedName.of("test"), false, false));

        assertStatement("DROP SCHEMA test CASCADE",
                new DropSchema(QualifiedName.of("test"), false, true));

        assertStatement("DROP SCHEMA IF EXISTS test",
                new DropSchema(QualifiedName.of("test"), true, false));

        assertStatement("DROP SCHEMA IF EXISTS test RESTRICT",
                new DropSchema(QualifiedName.of("test"), true, false));
    }

    @Test
    public void testRenameSchema()
    {
        assertStatement("ALTER SCHEMA foo RENAME TO bar",
                new RenameSchema(QualifiedName.of("foo"), "bar"));

        assertStatement("ALTER SCHEMA foo.bar RENAME TO baz",
                new RenameSchema(QualifiedName.of("foo", "bar"), "baz"));
    }

    @Test
    public void testCreateTable()
            throws Exception
    {
        assertStatement("CREATE TABLE foo (a VARCHAR, b BIGINT)",
                new CreateTable(QualifiedName.of("foo"),
                        ImmutableList.of(new ColumnDefinition("a", "VARCHAR"), new ColumnDefinition("b", "BIGINT")),
                        false,
                        ImmutableMap.of()));
        assertStatement("CREATE TABLE IF NOT EXISTS bar (c TIMESTAMP)",
                new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(new ColumnDefinition("c", "TIMESTAMP")),
                        true,
                        ImmutableMap.of()));

        // with LIKE
        assertStatement("CREATE TABLE IF NOT EXISTS bar (LIKE like_table)",
                new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(
                                new LikeClause(QualifiedName.of("like_table"),
                                        Optional.empty())),
                        true,
                        ImmutableMap.of()));
        assertStatement("CREATE TABLE IF NOT EXISTS bar (c TIMESTAMP, LIKE like_table)",
                new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(
                                new ColumnDefinition("c", "TIMESTAMP"),
                                new LikeClause(QualifiedName.of("like_table"),
                                        Optional.empty())),
                        true,
                        ImmutableMap.of()));
        assertStatement("CREATE TABLE IF NOT EXISTS bar (c TIMESTAMP, LIKE like_table, d DATE)",
                new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(
                                new ColumnDefinition("c", "TIMESTAMP"),
                                new LikeClause(QualifiedName.of("like_table"),
                                        Optional.empty()),
                                new ColumnDefinition("d", "DATE")),
                        true,
                        ImmutableMap.of()));
        assertStatement("CREATE TABLE IF NOT EXISTS bar (LIKE like_table INCLUDING PROPERTIES)",
                new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(
                                new LikeClause(QualifiedName.of("like_table"),
                                        Optional.of(LikeClause.PropertiesOption.INCLUDING))),
                        true,
                        ImmutableMap.of()));
        assertStatement("CREATE TABLE IF NOT EXISTS bar (c TIMESTAMP, LIKE like_table EXCLUDING PROPERTIES)",
                new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(
                                new ColumnDefinition("c", "TIMESTAMP"),
                                new LikeClause(QualifiedName.of("like_table"),
                                        Optional.of(LikeClause.PropertiesOption.EXCLUDING))),
                        true,
                        ImmutableMap.of()));
    }

    @Test
    public void testCreateTableAsSelect()
            throws Exception
    {
        Query query = simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t")));
        QualifiedName table = QualifiedName.of("foo");

        assertStatement("CREATE TABLE foo AS SELECT * FROM t",
                new CreateTableAsSelect(table, query, false, ImmutableMap.of(), true));

        assertStatement("CREATE TABLE IF NOT EXISTS foo AS SELECT * FROM t",
                new CreateTableAsSelect(table, query, true, ImmutableMap.of(), true));

        assertStatement("CREATE TABLE foo AS SELECT * FROM t WITH NO DATA",
                new CreateTableAsSelect(table, query, false, ImmutableMap.of(), false));

        ImmutableMap<String, Expression> properties = ImmutableMap.<String, Expression>builder()
                .put("string", new StringLiteral("bar"))
                .put("long", new LongLiteral("42"))
                .put("computed", new FunctionCall(QualifiedName.of("concat"), ImmutableList.of(
                        new StringLiteral("ban"),
                        new StringLiteral("ana"))))
                .put("a", new ArrayConstructor(ImmutableList.of(new StringLiteral("v1"), new StringLiteral("v2"))))
                .build();

        assertStatement("CREATE TABLE foo " +
                        "WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] ) " +
                        "AS " +
                        "SELECT * FROM t",
                new CreateTableAsSelect(table, query, false, properties, true));

        assertStatement("CREATE TABLE foo " +
                        "WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] ) " +
                        "AS " +
                        "SELECT * FROM t " +
                        "WITH NO DATA",
                new CreateTableAsSelect(table, query, false, properties, false));
    }

    @Test
    public void testDropTable()
            throws Exception
    {
        assertStatement("DROP TABLE a", new DropTable(QualifiedName.of("a"), false));
        assertStatement("DROP TABLE a.b", new DropTable(QualifiedName.of("a", "b"), false));
        assertStatement("DROP TABLE a.b.c", new DropTable(QualifiedName.of("a", "b", "c"), false));

        assertStatement("DROP TABLE IF EXISTS a", new DropTable(QualifiedName.of("a"), true));
        assertStatement("DROP TABLE IF EXISTS a.b", new DropTable(QualifiedName.of("a", "b"), true));
        assertStatement("DROP TABLE IF EXISTS a.b.c", new DropTable(QualifiedName.of("a", "b", "c"), true));
    }

    @Test
    public void testDropView()
            throws Exception
    {
        assertStatement("DROP VIEW a", new DropView(QualifiedName.of("a"), false));
        assertStatement("DROP VIEW a.b", new DropView(QualifiedName.of("a", "b"), false));
        assertStatement("DROP VIEW a.b.c", new DropView(QualifiedName.of("a", "b", "c"), false));

        assertStatement("DROP VIEW IF EXISTS a", new DropView(QualifiedName.of("a"), true));
        assertStatement("DROP VIEW IF EXISTS a.b", new DropView(QualifiedName.of("a", "b"), true));
        assertStatement("DROP VIEW IF EXISTS a.b.c", new DropView(QualifiedName.of("a", "b", "c"), true));
    }

    @Test
    public void testInsertInto()
            throws Exception
    {
        QualifiedName table = QualifiedName.of("a");
        Query query = simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t")));

        assertStatement("INSERT INTO a SELECT * FROM t",
                new Insert(table, Optional.empty(), query));

        assertStatement("INSERT INTO a (c1, c2) SELECT * FROM t",
                new Insert(table, Optional.of(ImmutableList.of("c1", "c2")), query));
    }

    @Test
    public void testDelete()
    {
        assertStatement("DELETE FROM t", new Delete(table(QualifiedName.of("t")), Optional.empty()));

        assertStatement("DELETE FROM t WHERE a = b", new Delete(table(QualifiedName.of("t")), Optional.of(
                new ComparisonExpression(ComparisonExpression.Type.EQUAL,
                        new QualifiedNameReference(QualifiedName.of("a")),
                        new QualifiedNameReference(QualifiedName.of("b"))))));
    }

    @Test
    public void testRenameTable()
            throws Exception
    {
        assertStatement("ALTER TABLE a RENAME TO b", new RenameTable(QualifiedName.of("a"), QualifiedName.of("b")));
    }

    @Test
    public void testRenameColumn()
            throws Exception
    {
        assertStatement("ALTER TABLE foo.t RENAME COLUMN a TO b", new RenameColumn(QualifiedName.of("foo", "t"), "a", "b"));
    }

    @Test
    public void testAddColumn()
            throws Exception
    {
        assertStatement("ALTER TABLE foo.t ADD COLUMN c bigint", new AddColumn(QualifiedName.of("foo", "t"), new ColumnDefinition("c", "bigint")));
    }

    @Test
    public void testCreateView()
            throws Exception
    {
        assertStatement("CREATE VIEW a AS SELECT * FROM t", new CreateView(
                QualifiedName.of("a"),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))),
                false));

        assertStatement("CREATE OR REPLACE VIEW a AS SELECT * FROM t", new CreateView(
                QualifiedName.of("a"),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))),
                true));
    }

    @Test
    public void testGrant()
            throws Exception
    {
        assertStatement("GRANT INSERT, DELETE ON t TO u",
                new Grant(Optional.of(ImmutableList.of("INSERT", "DELETE")), false, QualifiedName.of("t"), "u", false));
        assertStatement("GRANT SELECT ON t TO PUBLIC WITH GRANT OPTION",
                new Grant(Optional.of(ImmutableList.of("SELECT")), false, QualifiedName.of("t"), "PUBLIC", true));
        assertStatement("GRANT ALL PRIVILEGES ON t TO u",
                new Grant(Optional.empty(), false, QualifiedName.of("t"), "u", false));
        assertStatement("GRANT taco ON t TO PUBLIC WITH GRANT OPTION",
                new Grant(Optional.of(ImmutableList.of("taco")), false, QualifiedName.of("t"), "PUBLIC", true));
    }

    @Test
    public void testRevoke()
            throws Exception
    {
        assertStatement("REVOKE INSERT, DELETE ON t FROM u",
                new Revoke(false, Optional.of(ImmutableList.of("INSERT", "DELETE")), false, QualifiedName.of("t"), "u"));
        assertStatement("REVOKE GRANT OPTION FOR SELECT ON t FROM PUBLIC",
                new Revoke(true, Optional.of(ImmutableList.of("SELECT")), false, QualifiedName.of("t"), "PUBLIC"));
        assertStatement("REVOKE ALL PRIVILEGES ON TABLE t FROM u",
                new Revoke(false, Optional.empty(), true, QualifiedName.of("t"), "u"));
        assertStatement("REVOKE taco ON TABLE t FROM u",
                new Revoke(false, Optional.of(ImmutableList.of("taco")), true, QualifiedName.of("t"), "u"));
    }

    @Test
    public void testWith()
            throws Exception
    {
        assertStatement("WITH a (t, u) AS (SELECT * FROM x), b AS (SELECT * FROM y) TABLE z",
                new Query(Optional.of(new With(false, ImmutableList.of(
                        new WithQuery("a", simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("x"))), Optional.of(ImmutableList.of("t", "u"))),
                        new WithQuery("b", simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("y"))), Optional.empty())))),
                        new Table(QualifiedName.of("z")),
                        ImmutableList.of(),
                        Optional.<String>empty(),
                        Optional.<Approximate>empty()));

        assertStatement("WITH RECURSIVE a AS (SELECT * FROM x) TABLE y",
                new Query(Optional.of(new With(true, ImmutableList.of(
                        new WithQuery("a", simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("x"))), Optional.empty())))),
                        new Table(QualifiedName.of("y")),
                        ImmutableList.of(),
                        Optional.<String>empty(),
                        Optional.<Approximate>empty()));
    }

    @Test
    public void testImplicitJoin()
            throws Exception
    {
        assertStatement("SELECT * FROM a, b",
                simpleQuery(selectList(new AllColumns()),
                        new Join(Join.Type.IMPLICIT,
                                new Table(QualifiedName.of("a")),
                                new Table(QualifiedName.of("b")),
                                Optional.<JoinCriteria>empty())));
    }

    @Test
    public void testExplain()
            throws Exception
    {
        assertStatement("EXPLAIN SELECT * FROM t",
                new Explain(simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))), false, ImmutableList.of()));
        assertStatement("EXPLAIN (TYPE LOGICAL) SELECT * FROM t",
                new Explain(
                        simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))),
                        false,
                        ImmutableList.of(new ExplainType(ExplainType.Type.LOGICAL))));
        assertStatement("EXPLAIN (TYPE LOGICAL, FORMAT TEXT) SELECT * FROM t",
                new Explain(
                        simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))),
                        false,
                        ImmutableList.of(
                                new ExplainType(ExplainType.Type.LOGICAL),
                                new ExplainFormat(ExplainFormat.Type.TEXT))));
    }

    @Test
    public void testExplainAnalyze()
            throws Exception
    {
        assertStatement("EXPLAIN ANALYZE SELECT * FROM t",
                new Explain(simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))), true, ImmutableList.of()));
    }

    @Test
    public void testJoinPrecedence()
    {
        assertStatement("SELECT * FROM a CROSS JOIN b LEFT JOIN c ON true",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Join(
                                Join.Type.LEFT,
                                new Join(
                                        Join.Type.CROSS,
                                        new Table(QualifiedName.of("a")),
                                        new Table(QualifiedName.of("b")),
                                        Optional.empty()
                                ),
                                new Table(QualifiedName.of("c")),
                                Optional.of(new JoinOn(BooleanLiteral.TRUE_LITERAL)))));
        assertStatement("SELECT * FROM a CROSS JOIN b NATURAL JOIN c CROSS JOIN d NATURAL JOIN e",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Join(
                                Join.Type.INNER,
                                new Join(
                                        Join.Type.CROSS,
                                        new Join(
                                                Join.Type.INNER,
                                                new Join(
                                                        Join.Type.CROSS,
                                                        new Table(QualifiedName.of("a")),
                                                        new Table(QualifiedName.of("b")),
                                                        Optional.empty()
                                                ),
                                                new Table(QualifiedName.of("c")),
                                                Optional.of(new NaturalJoin())),
                                        new Table(QualifiedName.of("d")),
                                        Optional.empty()
                                ),
                                new Table(QualifiedName.of("e")),
                                Optional.of(new NaturalJoin()))));
    }

    @Test
    public void testUnnest()
            throws Exception
    {
        assertStatement("SELECT * FROM t CROSS JOIN UNNEST(a)",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Join(
                                Join.Type.CROSS,
                                new Table(QualifiedName.of("t")),
                                new Unnest(ImmutableList.of(new QualifiedNameReference(QualifiedName.of("a"))), false),
                                Optional.empty())));
        assertStatement("SELECT * FROM t CROSS JOIN UNNEST(a) WITH ORDINALITY",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Join(
                                Join.Type.CROSS,
                                new Table(QualifiedName.of("t")),
                                new Unnest(ImmutableList.of(new QualifiedNameReference(QualifiedName.of("a"))), true),
                                Optional.empty())));
    }

    @Test
    public void testStartTransaction()
            throws Exception
    {
        assertStatement("START TRANSACTION",
                new StartTransaction(ImmutableList.of()));
        assertStatement("START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
                new StartTransaction(ImmutableList.of(
                        new Isolation(Isolation.Level.READ_UNCOMMITTED))));
        assertStatement("START TRANSACTION ISOLATION LEVEL READ COMMITTED",
                new StartTransaction(ImmutableList.of(
                        new Isolation(Isolation.Level.READ_COMMITTED))));
        assertStatement("START TRANSACTION ISOLATION LEVEL REPEATABLE READ",
                new StartTransaction(ImmutableList.of(
                        new Isolation(Isolation.Level.REPEATABLE_READ))));
        assertStatement("START TRANSACTION ISOLATION LEVEL SERIALIZABLE",
                new StartTransaction(ImmutableList.of(
                        new Isolation(Isolation.Level.SERIALIZABLE))));
        assertStatement("START TRANSACTION READ ONLY",
                new StartTransaction(ImmutableList.of(
                        new TransactionAccessMode(true))));
        assertStatement("START TRANSACTION READ WRITE",
                new StartTransaction(ImmutableList.of(
                        new TransactionAccessMode(false))));
        assertStatement("START TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY",
                new StartTransaction(ImmutableList.of(
                        new Isolation(Isolation.Level.READ_COMMITTED),
                        new TransactionAccessMode(true))));
        assertStatement("START TRANSACTION READ ONLY, ISOLATION LEVEL READ COMMITTED",
                new StartTransaction(ImmutableList.of(
                        new TransactionAccessMode(true),
                        new Isolation(Isolation.Level.READ_COMMITTED))));
        assertStatement("START TRANSACTION READ WRITE, ISOLATION LEVEL SERIALIZABLE",
                new StartTransaction(ImmutableList.of(
                        new TransactionAccessMode(false),
                        new Isolation(Isolation.Level.SERIALIZABLE))));
    }

    @Test
    public void testCommit()
            throws Exception
    {
        assertStatement("COMMIT", new Commit());
        assertStatement("COMMIT WORK", new Commit());
    }

    @Test
    public void testRollback()
            throws Exception
    {
        assertStatement("ROLLBACK", new Rollback());
        assertStatement("ROLLBACK WORK", new Rollback());
    }

    @Test
    public void testAtTimeZone()
    {
        assertStatement("SELECT timestamp '2012-10-31 01:00 UTC' AT TIME ZONE 'America/Los_Angeles'",
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(
                                        new AtTimeZone(new TimestampLiteral("2012-10-31 01:00 UTC"), new StringLiteral("America/Los_Angeles"))
                                ),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty()),
                        ImmutableList.<SortItem>of(),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Test
    public void testLambda()
            throws Exception
    {
        assertExpression("x -> sin(x)",
                new LambdaExpression(
                        ImmutableList.of("x"),
                        new FunctionCall(QualifiedName.of("sin"), ImmutableList.of(new QualifiedNameReference(QualifiedName.of("x"))))));
        assertExpression("(x, y) -> mod(x, y)",
                new LambdaExpression(
                        ImmutableList.of("x", "y"),
                        new FunctionCall(
                                QualifiedName.of("mod"),
                                ImmutableList.of(new QualifiedNameReference(QualifiedName.of("x")), new QualifiedNameReference(QualifiedName.of("y"))))));
    }

    @Test
    public void testNonReserved()
            throws Exception
    {
        assertStatement("SELECT zone FROM t",
                simpleQuery(
                        selectList(new QualifiedNameReference(QualifiedName.of("zone"))),
                        table(QualifiedName.of("t"))));
    }

    @Test
    public void testBinaryLiteralToHex()
            throws Exception
    {
        // note that toHexString() always outputs in upper case
        assertEquals(new BinaryLiteral("ab 01").toHexString(), "AB01");
    }

    @Test
    public void testCall()
            throws Exception
    {
        assertStatement("CALL foo()", new Call(QualifiedName.of("foo"), ImmutableList.of()));
        assertStatement("CALL foo(123, a => 1, b => 'go', 456)", new Call(QualifiedName.of("foo"), ImmutableList.of(
                new CallArgument(new LongLiteral("123")),
                new CallArgument("a", new LongLiteral("1")),
                new CallArgument("b", new StringLiteral("go")),
                new CallArgument(new LongLiteral("456")))));
    }

    @Test
    public void testPrepare()
    {
        assertStatement("PREPARE myquery FROM select * from foo",
                new Prepare("myquery", simpleQuery(
                        selectList(new AllColumns()),
                        table(QualifiedName.of("foo")))));
    }

    @Test
    public void testPrepareWithParameters()
    {
        assertStatement("PREPARE myquery FROM SELECT ?, ? FROM foo",
                new Prepare("myquery", simpleQuery(
                        selectList(new Parameter(0), new Parameter(1)),
                        table(QualifiedName.of("foo")))));
    }

    @Test
    public void testDeallocatePrepare()
    {
        assertStatement("DEALLOCATE PREPARE myquery", new Deallocate("myquery"));
    }

    @Test
    public void testExecute()
    {
        assertStatement("EXECUTE myquery", new Execute("myquery", emptyList()));
    }

    @Test
    public void testExecuteWithUsing()
    {
        assertStatement("EXECUTE myquery USING 1, 'abc', ARRAY ['hello']",
                new Execute("myquery", ImmutableList.of(new LongLiteral("1"), new StringLiteral("abc"), new ArrayConstructor(ImmutableList.of(new StringLiteral("hello"))))));
    }

    @Test
    public void testExists()
    {
        assertStatement("SELECT EXISTS(SELECT 1)", simpleQuery(selectList(new ExistsPredicate(simpleQuery(selectList(new LongLiteral("1")))))));

        assertStatement(
                "SELECT EXISTS(SELECT 1) = EXISTS(SELECT 2)",
                simpleQuery(
                        selectList(new ComparisonExpression(
                                ComparisonExpression.Type.EQUAL,
                                new ExistsPredicate(simpleQuery(selectList(new LongLiteral("1")))),
                                new ExistsPredicate(simpleQuery(selectList(new LongLiteral("2"))))))));

        assertStatement(
                "SELECT NOT EXISTS(SELECT 1) = EXISTS(SELECT 2)",
                simpleQuery(
                        selectList(
                                new NotExpression(
                                        new ComparisonExpression(
                                                ComparisonExpression.Type.EQUAL,
                                                new ExistsPredicate(simpleQuery(selectList(new LongLiteral("1")))),
                                                new ExistsPredicate(simpleQuery(selectList(new LongLiteral("2")))))))));

        assertStatement(
                "SELECT (NOT EXISTS(SELECT 1)) = EXISTS(SELECT 2)",
                simpleQuery(
                        selectList(
                                new ComparisonExpression(
                                        ComparisonExpression.Type.EQUAL,
                                        new NotExpression(new ExistsPredicate(simpleQuery(selectList(new LongLiteral("1"))))),
                                        new ExistsPredicate(simpleQuery(selectList(new LongLiteral("2"))))))));
    }

    @Test
    public void testDescribeInput()
    {
        assertStatement("DESCRIBE INPUT myquery", new DescribeInput("myquery"));
    }

    private static void assertCast(String type)
    {
        assertCast(type, type);
    }

    private static void assertCast(String type, String expected)
    {
        assertExpression("CAST(null AS " + type + ")", new Cast(new NullLiteral(), expected));
    }

    private static void assertStatement(String query, Statement expected)
    {
        assertParsed(query, expected, SQL_PARSER.createStatement(query));
        assertFormattedSql(SQL_PARSER, expected);
    }

    private static void assertExpression(String expression, Expression expected)
    {
        assertParsed(expression, expected, SQL_PARSER.createExpression(expression));
    }

    private static void assertParsed(String input, Node expected, Node parsed)
    {
        if (!parsed.equals(expected)) {
            fail(format("expected\n\n%s\n\nto parse as\n\n%s\n\nbut was\n\n%s\n",
                    indent(input),
                    indent(formatSql(expected, Optional.empty())),
                    indent(formatSql(parsed, Optional.empty()))));
        }
    }

    private static void assertInvalidExpression(String expression, String expectedErrorMessageRegex)
    {
        try {
            Expression result = SQL_PARSER.createExpression(expression);
            fail("Expected to throw ParsingException for input:[" + expression + "], but got: " + result);
        }
        catch (ParsingException e) {
            if (!e.getErrorMessage().matches(expectedErrorMessageRegex)) {
                fail(format("Expected error message to match '%s', but was: '%s'", expectedErrorMessageRegex, e.getErrorMessage()));
            }
        }
    }

    private static String indent(String value)
    {
        String indent = "    ";
        return indent + value.trim().replaceAll("\n", "\n" + indent);
    }
}
