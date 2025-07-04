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
import com.facebook.presto.sql.tree.AddConstraint;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.AlterColumnNotNull;
import com.facebook.presto.sql.tree.AlterFunction;
import com.facebook.presto.sql.tree.AlterRoutineCharacteristics;
import com.facebook.presto.sql.tree.Analyze;
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
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.Commit;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ConstraintSpecification;
import com.facebook.presto.sql.tree.CreateFunction;
import com.facebook.presto.sql.tree.CreateMaterializedView;
import com.facebook.presto.sql.tree.CreateRole;
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
import com.facebook.presto.sql.tree.DescribeOutput;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.DropColumn;
import com.facebook.presto.sql.tree.DropConstraint;
import com.facebook.presto.sql.tree.DropFunction;
import com.facebook.presto.sql.tree.DropMaterializedView;
import com.facebook.presto.sql.tree.DropRole;
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
import com.facebook.presto.sql.tree.GrantRoles;
import com.facebook.presto.sql.tree.GrantorSpecification;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.facebook.presto.sql.tree.GroupingSets;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.IntervalLiteral.IntervalField;
import com.facebook.presto.sql.tree.IntervalLiteral.Sign;
import com.facebook.presto.sql.tree.Isolation;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.Lateral;
import com.facebook.presto.sql.tree.LikeClause;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Merge;
import com.facebook.presto.sql.tree.MergeInsert;
import com.facebook.presto.sql.tree.MergeUpdate;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.Offset;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.PrincipalSpecification;
import com.facebook.presto.sql.tree.Property;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QuantifiedComparisonExpression;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.RefreshMaterializedView;
import com.facebook.presto.sql.tree.RenameColumn;
import com.facebook.presto.sql.tree.RenameSchema;
import com.facebook.presto.sql.tree.RenameTable;
import com.facebook.presto.sql.tree.RenameView;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.sql.tree.Return;
import com.facebook.presto.sql.tree.Revoke;
import com.facebook.presto.sql.tree.RevokeRoles;
import com.facebook.presto.sql.tree.Rollback;
import com.facebook.presto.sql.tree.Rollup;
import com.facebook.presto.sql.tree.RoutineCharacteristics;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SetProperties;
import com.facebook.presto.sql.tree.SetRole;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.ShowCatalogs;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowCreate;
import com.facebook.presto.sql.tree.ShowCreateFunction;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowGrants;
import com.facebook.presto.sql.tree.ShowRoleGrants;
import com.facebook.presto.sql.tree.ShowRoles;
import com.facebook.presto.sql.tree.ShowSchemas;
import com.facebook.presto.sql.tree.ShowSession;
import com.facebook.presto.sql.tree.ShowStats;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.SqlParameterDeclaration;
import com.facebook.presto.sql.tree.StartTransaction;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.TableVersionExpression;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.sql.tree.TransactionAccessMode;
import com.facebook.presto.sql.tree.TruncateTable;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.Update;
import com.facebook.presto.sql.tree.UpdateAssignment;
import com.facebook.presto.sql.tree.Use;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.QueryUtil.aliased;
import static com.facebook.presto.sql.QueryUtil.equal;
import static com.facebook.presto.sql.QueryUtil.identifier;
import static com.facebook.presto.sql.QueryUtil.nameReference;
import static com.facebook.presto.sql.QueryUtil.query;
import static com.facebook.presto.sql.QueryUtil.quotedIdentifier;
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
import static com.facebook.presto.sql.tree.ArithmeticBinaryExpression.Operator.DIVIDE;
import static com.facebook.presto.sql.tree.ArithmeticUnaryExpression.negative;
import static com.facebook.presto.sql.tree.ArithmeticUnaryExpression.positive;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static com.facebook.presto.sql.tree.ConstraintSpecification.ConstraintType.PRIMARY_KEY;
import static com.facebook.presto.sql.tree.ConstraintSpecification.ConstraintType.UNIQUE;
import static com.facebook.presto.sql.tree.RoutineCharacteristics.Determinism.DETERMINISTIC;
import static com.facebook.presto.sql.tree.RoutineCharacteristics.Determinism.NOT_DETERMINISTIC;
import static com.facebook.presto.sql.tree.RoutineCharacteristics.Language.SQL;
import static com.facebook.presto.sql.tree.RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT;
import static com.facebook.presto.sql.tree.RoutineCharacteristics.NullCallClause.RETURNS_NULL_ON_NULL_INPUT;
import static com.facebook.presto.sql.tree.ShowCreate.Type.SCHEMA;
import static com.facebook.presto.sql.tree.SortItem.NullOrdering.UNDEFINED;
import static com.facebook.presto.sql.tree.SortItem.Ordering.ASCENDING;
import static com.facebook.presto.sql.tree.SortItem.Ordering.DESCENDING;
import static com.facebook.presto.sql.tree.TableVersionExpression.TableVersionOperator;
import static com.facebook.presto.sql.tree.TableVersionExpression.TableVersionType.TIMESTAMP;
import static com.facebook.presto.sql.tree.TableVersionExpression.TableVersionType.VERSION;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSqlParser
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testPosition()
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
    {
        SQL_PARSER.createExpression("(((((((((((((((((((((((((((true)))))))))))))))))))))))))))");
    }

    @Test(timeOut = 2_000)
    public void testPotentialUnboundedLookahead()
    {
        SQL_PARSER.createExpression("(\n" +
                "      1 * -1 +\n" +
                "      1 * -2 +\n" +
                "      1 * -3 +\n" +
                "      1 * -4 +\n" +
                "      1 * -5 +\n" +
                "      1 * -6 +\n" +
                "      1 * -7 +\n" +
                "      1 * -8 +\n" +
                "      1 * -9 +\n" +
                "      1 * -10 +\n" +
                "      1 * -11 +\n" +
                "      1 * -12 \n" +
                ")\n");
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
    {
        assertExpression("TIME" + " 'abc'", new TimeLiteral("abc"));
        assertExpression("TIMESTAMP" + " 'abc'", new TimestampLiteral("abc"));
        assertExpression("INTERVAL '33' day", new IntervalLiteral("33", Sign.POSITIVE, IntervalField.DAY, Optional.empty()));
        assertExpression("INTERVAL '33' day to second", new IntervalLiteral("33", Sign.POSITIVE, IntervalField.DAY, Optional.of(IntervalField.SECOND)));
        assertExpression("CHAR 'abc'", new CharLiteral("abc"));
    }

    @Test
    public void testArrayConstructor()
    {
        assertExpression("ARRAY []", new ArrayConstructor(ImmutableList.of()));
        assertExpression("ARRAY [1, 2]", new ArrayConstructor(ImmutableList.of(new LongLiteral("1"), new LongLiteral("2"))));
        assertExpression("ARRAY [1e0, 2.5e0]", new ArrayConstructor(ImmutableList.of(new DoubleLiteral("1.0"), new DoubleLiteral("2.5"))));
        assertExpression("ARRAY ['hi']", new ArrayConstructor(ImmutableList.of(new StringLiteral("hi"))));
        assertExpression("ARRAY ['hi', 'hello']", new ArrayConstructor(ImmutableList.of(new StringLiteral("hi"), new StringLiteral("hello"))));
    }

    @Test
    public void testArraySubscript()
    {
        assertExpression("ARRAY [1, 2][1]", new SubscriptExpression(
                new ArrayConstructor(ImmutableList.of(new LongLiteral("1"), new LongLiteral("2"))),
                new LongLiteral("1")));
        try {
            assertExpression("CASE WHEN TRUE THEN ARRAY[1,2] END[1]", null);
            fail();
        }
        catch (RuntimeException e) {
            // Expected
        }
    }

    @Test
    public void testRowSubscript()
    {
        assertExpression("ROW (1, 'a', true)[1]", new SubscriptExpression(
                new Row(ImmutableList.of(new LongLiteral("1"), new StringLiteral("a"), new BooleanLiteral("true"))),
                new LongLiteral("1")));
    }

    @Test
    public void testDouble()
    {
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

        assertCast("interval year to month", "INTERVAL YEAR TO MONTH");
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
    public void testCoalesce()
    {
        assertInvalidExpression("coalesce()", "The 'coalesce' function must have at least two arguments");
        assertInvalidExpression("coalesce(5)", "The 'coalesce' function must have at least two arguments");
        assertInvalidExpression("coalesce(1, 2) filter (where true)", "FILTER not valid for 'coalesce' function");
        assertInvalidExpression("coalesce(1, 2) OVER ()", "OVER clause not valid for 'coalesce' function");
        assertExpression("coalesce(13, 42)", new CoalesceExpression(new LongLiteral("13"), new LongLiteral("42")));
        assertExpression("coalesce(6, 7, 8)", new CoalesceExpression(new LongLiteral("6"), new LongLiteral("7"), new LongLiteral("8")));
        assertExpression("coalesce(13, null)", new CoalesceExpression(new LongLiteral("13"), new NullLiteral()));
        assertExpression("coalesce(null, 13)", new CoalesceExpression(new NullLiteral(), new LongLiteral("13")));
        assertExpression("coalesce(null, null)", new CoalesceExpression(new NullLiteral(), new NullLiteral()));
    }

    @Test
    public void testIf()
    {
        assertExpression("if(true, 1, 0)", new IfExpression(new BooleanLiteral("true"), new LongLiteral("1"), new LongLiteral("0")));
        assertExpression("if(true, 3, null)", new IfExpression(new BooleanLiteral("true"), new LongLiteral("3"), new NullLiteral()));
        assertExpression("if(false, null, 4)", new IfExpression(new BooleanLiteral("false"), new NullLiteral(), new LongLiteral("4")));
        assertExpression("if(false, null, null)", new IfExpression(new BooleanLiteral("false"), new NullLiteral(), new NullLiteral()));
        assertExpression("if(true, 3)", new IfExpression(new BooleanLiteral("true"), new LongLiteral("3"), null));
        assertInvalidExpression("IF(true)", "Invalid number of arguments for 'if' function");
        assertInvalidExpression("IF(true, 1, 0) FILTER (WHERE true)", "FILTER not valid for 'if' function");
        assertInvalidExpression("IF(true, 1, 0) OVER()", "OVER clause not valid for 'if' function");
    }

    @Test
    public void testNullIf()
    {
        assertExpression("nullif(42, 87)", new NullIfExpression(new LongLiteral("42"), new LongLiteral("87")));
        assertExpression("nullif(42, null)", new NullIfExpression(new LongLiteral("42"), new NullLiteral()));
        assertExpression("nullif(null, null)", new NullIfExpression(new NullLiteral(), new NullLiteral()));
        assertInvalidExpression("nullif(1)", "Invalid number of arguments for 'nullif' function");
        assertInvalidExpression("nullif(1, 2, 3)", "Invalid number of arguments for 'nullif' function");
        assertInvalidExpression("nullif(42, 87) filter (where true)", "FILTER not valid for 'nullif' function");
        assertInvalidExpression("nullif(42, 87) OVER ()", "OVER clause not valid for 'nullif' function");
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
                                new Intersect(ImmutableList.of(createSelect123(), createSelect123()), Optional.of(true)),
                                createSelect123()
                        ), Optional.of(false)),
                        Optional.empty(),
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
                                new Union(ImmutableList.of(createSelect123(), createSelect123()), Optional.of(true)),
                                createSelect123()
                        ), Optional.of(false)),
                        Optional.empty(),
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
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    @Test
    public void testBetween()
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
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of("ALL")));
    }

    @Test
    public void testValues()
    {
        Query valuesQuery = query(values(
                row(new StringLiteral("a"), new LongLiteral("1"), new DoubleLiteral("2.2")),
                row(new StringLiteral("b"), new LongLiteral("2"), new DoubleLiteral("3.3"))));

        assertStatement("VALUES ('a', 1, 2.2e0), ('b', 2, 3.3e0)", valuesQuery);

        assertStatement("SELECT * FROM (VALUES ('a', 1, 2.2e0), ('b', 2, 3.3e0))",
                simpleQuery(
                        selectList(new AllColumns()),
                        subquery(valuesQuery)));
    }

    @Test
    public void testPrecedenceAndAssociativity()
    {
        assertExpression("1 AND 2 OR 3", new LogicalBinaryExpression(LogicalBinaryExpression.Operator.OR,
                new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND,
                        new LongLiteral("1"),
                        new LongLiteral("2")),
                new LongLiteral("3")));

        assertExpression("1 OR 2 AND 3", new LogicalBinaryExpression(LogicalBinaryExpression.Operator.OR,
                new LongLiteral("1"),
                new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND,
                        new LongLiteral("2"),
                        new LongLiteral("3"))));

        assertExpression("NOT 1 AND 2", new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND,
                new NotExpression(new LongLiteral("1")),
                new LongLiteral("2")));

        assertExpression("NOT 1 OR 2", new LogicalBinaryExpression(LogicalBinaryExpression.Operator.OR,
                new NotExpression(new LongLiteral("1")),
                new LongLiteral("2")));

        assertExpression("-1 + 2", new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.ADD,
                negative(new LongLiteral("1")),
                new LongLiteral("2")));

        assertExpression("1 - 2 - 3", new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.SUBTRACT,
                new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.SUBTRACT,
                        new LongLiteral("1"),
                        new LongLiteral("2")),
                new LongLiteral("3")));

        assertExpression("1 / 2 / 3", new ArithmeticBinaryExpression(DIVIDE,
                new ArithmeticBinaryExpression(DIVIDE,
                        new LongLiteral("1"),
                        new LongLiteral("2")),
                new LongLiteral("3")));

        assertExpression("1 + 2 * 3", new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.ADD,
                new LongLiteral("1"),
                new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.MULTIPLY,
                        new LongLiteral("2"),
                        new LongLiteral("3"))));
    }

    @Test
    public void testAllowIdentifierColon()
    {
        SqlParser sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(COLON));
        sqlParser.createStatement("select * from foo:bar");
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

        assertInvalidExpression("123.", "Unexpected decimal literal: 123.");
        assertInvalidExpression("123.0", "Unexpected decimal literal: 123.0");
        assertInvalidExpression(".5", "Unexpected decimal literal: .5");
        assertInvalidExpression("123.5", "Unexpected decimal literal: 123.5");
    }

    @Test
    public void testTime()
    {
        assertExpression("TIME '03:04:05'", new TimeLiteral("03:04:05"));
    }

    @Test
    public void testCurrentTimestamp()
    {
        assertExpression("CURRENT_TIMESTAMP", new CurrentTime(CurrentTime.Function.TIMESTAMP));
    }

    @Test
    public void testSetSession()
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
    {
        assertStatement("RESET SESSION foo.bar", new ResetSession(QualifiedName.of("foo", "bar")));
        assertStatement("RESET SESSION foo", new ResetSession(QualifiedName.of("foo")));
    }

    @Test
    public void testShowSession()
    {
        assertStatement("SHOW SESSION", new ShowSession(Optional.empty(), Optional.empty()));
        assertStatement("SHOW SESSION LIKE '%'", new ShowSession(Optional.of("%"), Optional.empty()));
        assertStatement("SHOW SESSION LIKE '%' ESCAPE '$'", new ShowSession(Optional.of("%"), Optional.of("$")));
    }

    @Test
    public void testShowCatalogs()
    {
        assertStatement("SHOW CATALOGS", new ShowCatalogs(Optional.empty(), Optional.empty()));
        assertStatement("SHOW CATALOGS LIKE '%'", new ShowCatalogs(Optional.of("%"), Optional.empty()));
        assertStatement("SHOW CATALOGS LIKE '%$_%' ESCAPE '$'", new ShowCatalogs(Optional.of("%$_%"), Optional.of("$")));
    }

    @Test
    public void testShowSchemas()
    {
        assertStatement("SHOW SCHEMAS", new ShowSchemas(Optional.empty(), Optional.empty(), Optional.empty()));
        assertStatement("SHOW SCHEMAS FROM foo", new ShowSchemas(Optional.of(identifier("foo")), Optional.empty(), Optional.empty()));
        assertStatement("SHOW SCHEMAS IN foo LIKE '%'", new ShowSchemas(Optional.of(identifier("foo")), Optional.of("%"), Optional.empty()));
        assertStatement("SHOW SCHEMAS IN foo LIKE '%$_%' ESCAPE '$'", new ShowSchemas(Optional.of(identifier("foo")), Optional.of("%$_%"), Optional.of("$")));
    }

    @Test
    public void testShowTables()
    {
        assertStatement("SHOW TABLES", new ShowTables(Optional.empty(), Optional.empty(), Optional.empty()));
        assertStatement("SHOW TABLES FROM a", new ShowTables(Optional.of(QualifiedName.of("a")), Optional.empty(), Optional.empty()));
        assertStatement("SHOW TABLES FROM \"awesome schema\"", new ShowTables(Optional.of(QualifiedName.of("awesome schema")), Optional.empty(), Optional.empty()));
        assertStatement("SHOW TABLES IN a LIKE '%$_%' ESCAPE '$'", new ShowTables(Optional.of(QualifiedName.of("a")), Optional.of("%$_%"), Optional.of("$")));
    }

    @Test
    public void testShowColumns()
    {
        assertStatement("SHOW COLUMNS FROM a", new ShowColumns(QualifiedName.of("a")));
        assertStatement("SHOW COLUMNS FROM a.b", new ShowColumns(QualifiedName.of("a", "b")));
        assertStatement("SHOW COLUMNS FROM \"awesome table\"", new ShowColumns(QualifiedName.of("awesome table")));
        assertStatement("SHOW COLUMNS FROM \"awesome schema\".\"awesome table\"", new ShowColumns(QualifiedName.of("awesome schema", "awesome table")));
    }

    @Test
    public void testShowFunctions()
    {
        assertStatement("SHOW FUNCTIONS", new ShowFunctions(Optional.empty(), Optional.empty()));
        assertStatement("SHOW FUNCTIONS LIKE '%'", new ShowFunctions(Optional.of("%"), Optional.empty()));
        assertStatement("SHOW FUNCTIONS LIKE '%$_%' ESCAPE '$'", new ShowFunctions(Optional.of("%$_%"), Optional.of("$")));
    }

    @Test
    public void testShowCreateFunction()
    {
        assertStatement("SHOW CREATE FUNCTION x.y.z", new ShowCreateFunction(QualifiedName.of("x", "y", "z"), Optional.empty()));
        assertStatement("SHOW CREATE FUNCTION x.y.z()", new ShowCreateFunction(QualifiedName.of("x", "y", "z"), Optional.of(ImmutableList.of())));
        assertStatement(
                "SHOW CREATE FUNCTION x.y.z(int, double)",
                new ShowCreateFunction(QualifiedName.of("x", "y", "z"), Optional.of(ImmutableList.of("int", "double"))));
    }

    @Test
    public void testShowCreateSchema()
    {
        assertStatement("SHOW CREATE SCHEMA x.y", new ShowCreate(SCHEMA, QualifiedName.of("x", "y")));
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
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
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
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
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
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
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
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Test
    public void testSelectWithRowType()
    {
        assertStatement("SELECT col1.f1, col2, col3.f1.f2.f3 FROM table1",
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(
                                        new DereferenceExpression(new Identifier("col1"), identifier("f1")),
                                        new Identifier("col2"),
                                        new DereferenceExpression(
                                                new DereferenceExpression(new DereferenceExpression(new Identifier("col3"), identifier("f1")), identifier("f2")), identifier("f3"))),
                                Optional.of(new Table(QualifiedName.of("table1"))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT col1.f1[0], col2, col3[2].f2.f3, col4[4] FROM table1",
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(
                                        new SubscriptExpression(new DereferenceExpression(new Identifier("col1"), identifier("f1")), new LongLiteral("0")),
                                        new Identifier("col2"),
                                        new DereferenceExpression(new DereferenceExpression(new SubscriptExpression(new Identifier("col3"), new LongLiteral("2")), identifier("f2")), identifier("f3")),
                                        new SubscriptExpression(new Identifier("col4"), new LongLiteral("4"))),
                                Optional.of(new Table(QualifiedName.of("table1"))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT CAST(ROW(11, 12) AS ROW(COL0 INTEGER, COL1 INTEGER)).col0",
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(
                                        new DereferenceExpression(new Cast(new Row(Lists.newArrayList(new LongLiteral("11"), new LongLiteral("12"))), "ROW(COL0 INTEGER,COL1 INTEGER)"), identifier("col0"))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Test
    public void testSelectWithOrderBy()
    {
        assertStatement("SELECT * FROM table1 ORDER BY a",
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(new AllColumns()),
                                Optional.of(new Table(QualifiedName.of("table1"))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(new OrderBy(ImmutableList.of(new SortItem(
                                        new Identifier("a"),
                                        ASCENDING,
                                        UNDEFINED)))),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Test
    public void testSelectWithOffset()
    {
        assertStatement("SELECT * FROM table1 OFFSET 2 ROWS",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(QualifiedName.of("table1")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new Offset("2")),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 OFFSET 2",
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(new AllColumns()),
                                Optional.of(new Table(QualifiedName.of("table1"))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(new Offset("2")),
                                Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 order by x OFFSET 2 limit 10",
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(new AllColumns()),
                                Optional.of(new Table(QualifiedName.of("table1"))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(new OrderBy(ImmutableList.of(new SortItem(new Identifier("x"), ASCENDING, UNDEFINED)))),
                                Optional.of(new Offset("2")),
                                Optional.of("10")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 order by x FETCH FIRST 10 ROWS ONLY",
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(new AllColumns()),
                                Optional.of(new Table(QualifiedName.of("table1"))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(new OrderBy(ImmutableList.of(new SortItem(new Identifier("x"), ASCENDING, UNDEFINED)))),
                                Optional.empty(),
                                Optional.of("10")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        Query valuesQuery = query(values(
                row(new LongLiteral("1"), new StringLiteral("1")),
                row(new LongLiteral("2"), new StringLiteral("2"))));

        assertStatement("SELECT * FROM (VALUES (1, '1'), (2, '2')) OFFSET 2 ROWS",
                simpleQuery(selectList(new AllColumns()),
                        subquery(valuesQuery),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new Offset("2")),
                        Optional.empty()));

        assertStatement("SELECT * FROM (VALUES (1, '1'), (2, '2')) OFFSET 2",
                simpleQuery(selectList(new AllColumns()),
                        subquery(valuesQuery),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new Offset("2")),
                        Optional.empty()));
    }

    @Test
    public void testSelectWithGroupBy()
    {
        assertStatement("SELECT * FROM table1 GROUP BY a",
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(new AllColumns()),
                                Optional.of(new Table(QualifiedName.of("table1"))),
                                Optional.empty(),
                                Optional.of(new GroupBy(false, ImmutableList.of(new SimpleGroupBy(ImmutableList.of(new Identifier("a")))))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
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
                                        new SimpleGroupBy(ImmutableList.of(new Identifier("a"))),
                                        new SimpleGroupBy(ImmutableList.of(new Identifier("b")))))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
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
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 GROUP BY GROUPING SETS (a)",
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(new AllColumns()),
                                Optional.of(new Table(QualifiedName.of("table1"))),
                                Optional.empty(),
                                Optional.of(new GroupBy(false, ImmutableList.of(new GroupingSets(ImmutableList.of(ImmutableList.of(new Identifier("a"))))))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT a, b, GROUPING(a, b) FROM table1 GROUP BY GROUPING SETS ((a), (b))",
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(
                                        DereferenceExpression.from(QualifiedName.of("a")),
                                        DereferenceExpression.from(QualifiedName.of("b")),
                                        new GroupingOperation(
                                                Optional.empty(),
                                                ImmutableList.of(QualifiedName.of("a"), QualifiedName.of("b")))),
                                Optional.of(new Table(QualifiedName.of("table1"))),
                                Optional.empty(),
                                Optional.of(new GroupBy(false, ImmutableList.of(new GroupingSets(ImmutableList.of(ImmutableList.of(new Identifier("a")), ImmutableList.of(new Identifier("b"))))))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
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
                                                ImmutableList.of(ImmutableList.of(new Identifier("a"), new Identifier("b")),
                                                        ImmutableList.of(new Identifier("a")),
                                                        ImmutableList.of())),
                                        new Cube(ImmutableList.of(new Identifier("c"))),
                                        new Rollup(ImmutableList.of(new Identifier("d")))))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
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
                                                ImmutableList.of(ImmutableList.of(new Identifier("a"), new Identifier("b")),
                                                        ImmutableList.of(new Identifier("a")),
                                                        ImmutableList.of())),
                                        new Cube(ImmutableList.of(new Identifier("c"))),
                                        new Rollup(ImmutableList.of(new Identifier("d")))))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Test
    public void testCreateSchema()
    {
        assertStatement("CREATE SCHEMA test",
                new CreateSchema(QualifiedName.of("test"), false, ImmutableList.of()));

        assertStatement("CREATE SCHEMA IF NOT EXISTS test",
                new CreateSchema(QualifiedName.of("test"), true, ImmutableList.of()));

        assertStatement("CREATE SCHEMA test WITH (a = 'apple', b = 123)",
                new CreateSchema(
                        QualifiedName.of("test"),
                        false,
                        ImmutableList.of(
                                new Property(new Identifier("a"), new StringLiteral("apple")),
                                new Property(new Identifier("b"), new LongLiteral("123")))));

        assertStatement("CREATE SCHEMA \"some name that contains space\"",
                new CreateSchema(QualifiedName.of("some name that contains space"), false, ImmutableList.of()));
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

        assertStatement("DROP SCHEMA \"some schema that contains space\"",
                new DropSchema(QualifiedName.of("some schema that contains space"), false, false));
    }

    @Test
    public void testRenameSchema()
    {
        assertStatement("ALTER SCHEMA foo RENAME TO bar",
                new RenameSchema(QualifiedName.of("foo"), identifier("bar")));

        assertStatement("ALTER SCHEMA foo.bar RENAME TO baz",
                new RenameSchema(QualifiedName.of("foo", "bar"), identifier("baz")));

        assertStatement("ALTER SCHEMA \"awesome schema\".\"awesome table\" RENAME TO \"even more awesome table\"",
                new RenameSchema(QualifiedName.of("awesome schema", "awesome table"), quotedIdentifier("even more awesome table")));
    }

    @Test
    public void testUnicodeString()
    {
        assertExpression("U&''", new StringLiteral(""));
        assertExpression("U&'' UESCAPE ')'", new StringLiteral(""));
        assertExpression("U&'hello\\6d4B\\8Bd5\\+10FFFFworld\\7F16\\7801'", new StringLiteral("hello\u6d4B\u8Bd5\uDBFF\uDFFFworld\u7F16\u7801"));
        assertExpression("U&'\u6d4B\u8Bd5ABC\\6d4B\\8Bd5'", new StringLiteral("\u6d4B\u8Bd5ABC\u6d4B\u8Bd5"));
        assertExpression("u&'\u6d4B\u8Bd5ABC\\6d4B\\8Bd5'", new StringLiteral("\u6d4B\u8Bd5ABC\u6d4B\u8Bd5"));
        assertExpression("u&'\u6d4B\u8Bd5ABC\\\\'", new StringLiteral("\u6d4B\u8Bd5ABC\\"));
        assertExpression("u&'\u6d4B\u8Bd5ABC###8Bd5' UESCAPE '#'", new StringLiteral("\u6d4B\u8Bd5ABC#\u8Bd5"));
        assertExpression("u&'\u6d4B\u8Bd5''A''B''C##''''#8Bd5' UESCAPE '#'", new StringLiteral("\u6d4B\u8Bd5\'A\'B\'C#\'\'\u8Bd5"));
        assertInvalidExpression("U&  '\u6d4B\u8Bd5ABC\\\\'", ".*mismatched input.*");
        assertInvalidExpression("u&'\u6d4B\u8Bd5ABC\\'", "Incomplete escape sequence: ");
        assertInvalidExpression("u&'\u6d4B\u8Bd5ABC\\+'", "Incomplete escape sequence: ");
        assertInvalidExpression("U&'hello\\6dB\\8Bd5'", "Incomplete escape sequence: 6dB.*");
        assertInvalidExpression("U&'hello\\6D4B\\8Bd'", "Incomplete escape sequence: 8Bd");
        assertInvalidExpression("U&'hello\\K6B\\8Bd5'", "Invalid hexadecimal digit: K");
        assertInvalidExpression("U&'hello\\+FFFFFD\\8Bd5'", "Invalid escaped character: FFFFFD");
        assertInvalidExpression("U&'hello\\DBFF'", "Invalid escaped character: DBFF\\. Escaped character is a surrogate\\. Use \'\\\\\\+123456\' instead\\.");
        assertInvalidExpression("U&'hello\\+00DBFF'", "Invalid escaped character: 00DBFF\\. Escaped character is a surrogate\\. Use \'\\\\\\+123456\' instead\\.");
        assertInvalidExpression("U&'hello\\8Bd5' UESCAPE '%%'", "Invalid Unicode escape character: %%");
        assertInvalidExpression("U&'hello\\8Bd5' UESCAPE '\uDBFF'", "Invalid Unicode escape character: \uDBFF");
        assertInvalidExpression("U&'hello\\8Bd5' UESCAPE '\n'", "Invalid Unicode escape character: \n");
        assertInvalidExpression("U&'hello\\8Bd5' UESCAPE ''''", "Invalid Unicode escape character: \'");
        assertInvalidExpression("U&'hello\\8Bd5' UESCAPE ' '", "Invalid Unicode escape character:  ");
        assertInvalidExpression("U&'hello\\8Bd5' UESCAPE ''", "Empty Unicode escape character");
        assertInvalidExpression("U&'hello\\8Bd5' UESCAPE '1'", "Invalid Unicode escape character: 1");
        assertInvalidExpression("U&'hello\\8Bd5' UESCAPE '+'", "Invalid Unicode escape character: \\+");
        assertExpression("U&'hello!6d4B!8Bd5!+10FFFFworld!7F16!7801' UESCAPE '!'", new StringLiteral("hello\u6d4B\u8Bd5\uDBFF\uDFFFworld\u7F16\u7801"));
        assertExpression("U&'\u6d4B\u8Bd5ABC!6d4B!8Bd5' UESCAPE '!'", new StringLiteral("\u6d4B\u8Bd5ABC\u6d4B\u8Bd5"));
        assertExpression("U&'hello\\6d4B\\8Bd5\\+10FFFFworld\\7F16\\7801' UESCAPE '!'",
                new StringLiteral("hello\\6d4B\\8Bd5\\+10FFFFworld\\7F16\\7801"));
    }

    @Test
    public void testCreateTable()
    {
        assertStatement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c IPADDRESS)",
                new CreateTable(QualifiedName.of("foo"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("a"), "VARCHAR", true, emptyList(), Optional.empty()),
                                new ColumnDefinition(identifier("b"), "BIGINT", true, emptyList(), Optional.of("hello world")),
                                new ColumnDefinition(identifier("c"), "IPADDRESS", true, emptyList(), Optional.empty())),
                        false,
                        ImmutableList.of(),
                        Optional.empty()));
        assertStatement("CREATE TABLE IF NOT EXISTS bar (c TIMESTAMP)",
                new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(new ColumnDefinition(identifier("c"), "TIMESTAMP", true, emptyList(), Optional.empty())),
                        true,
                        ImmutableList.of(),
                        Optional.empty()));

        // with properties
        assertStatement("CREATE TABLE IF NOT EXISTS bar (c TIMESTAMP WITH (nullable = true, compression = 'LZ4'))",
                new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(new ColumnDefinition(identifier("c"), "TIMESTAMP", true, ImmutableList.of(
                                new Property(new Identifier("nullable"), BooleanLiteral.TRUE_LITERAL),
                                new Property(new Identifier("compression"), new StringLiteral("LZ4"))
                        ), Optional.empty())),
                        true,
                        ImmutableList.of(),
                        Optional.empty()));

        // with LIKE
        assertStatement("CREATE TABLE IF NOT EXISTS bar (LIKE like_table)",
                new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(
                                new LikeClause(QualifiedName.of("like_table"),
                                        Optional.empty())),
                        true,
                        ImmutableList.of(),
                        Optional.empty()));
        assertStatement("CREATE TABLE IF NOT EXISTS bar (c TIMESTAMP, LIKE like_table)",
                new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("c"), "TIMESTAMP", true, emptyList(), Optional.empty()),
                                new LikeClause(QualifiedName.of("like_table"),
                                        Optional.empty())),
                        true,
                        ImmutableList.of(),
                        Optional.empty()));
        assertStatement("CREATE TABLE IF NOT EXISTS bar (c TIMESTAMP, LIKE like_table, d DATE)",
                new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("c"), "TIMESTAMP", true, emptyList(), Optional.empty()),
                                new LikeClause(QualifiedName.of("like_table"),
                                        Optional.empty()),
                                new ColumnDefinition(identifier("d"), "DATE", true, emptyList(), Optional.empty())),
                        true,
                        ImmutableList.of(),
                        Optional.empty()));
        assertStatement("CREATE TABLE IF NOT EXISTS bar (LIKE like_table INCLUDING PROPERTIES)",
                new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(
                                new LikeClause(QualifiedName.of("like_table"),
                                        Optional.of(LikeClause.PropertiesOption.INCLUDING))),
                        true,
                        ImmutableList.of(),
                        Optional.empty()));
        assertStatement("CREATE TABLE IF NOT EXISTS bar (c TIMESTAMP, LIKE like_table EXCLUDING PROPERTIES)",
                new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("c"), "TIMESTAMP", true, emptyList(), Optional.empty()),
                                new LikeClause(QualifiedName.of("like_table"),
                                        Optional.of(LikeClause.PropertiesOption.EXCLUDING))),
                        true,
                        ImmutableList.of(),
                        Optional.empty()));
        assertStatement("CREATE TABLE IF NOT EXISTS bar (c TIMESTAMP, LIKE like_table EXCLUDING PROPERTIES) COMMENT 'test'",
                new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("c"), "TIMESTAMP", true, emptyList(), Optional.empty()),
                                new LikeClause(QualifiedName.of("like_table"),
                                        Optional.of(LikeClause.PropertiesOption.EXCLUDING))),
                        true,
                        ImmutableList.of(),
                        Optional.of("test")));
    }

    @Test
    public void testCreateTableWithNotNull()
    {
        assertStatement(
                "CREATE TABLE foo (" +
                        "a VARCHAR NOT NULL COMMENT 'column a', " +
                        "b BIGINT COMMENT 'hello world', " +
                        "c IPADDRESS, " +
                        "d DATE NOT NULL)",
                new CreateTable(
                        QualifiedName.of("foo"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("a"), "VARCHAR", false, emptyList(), Optional.of("column a")),
                                new ColumnDefinition(identifier("b"), "BIGINT", true, emptyList(), Optional.of("hello world")),
                                new ColumnDefinition(identifier("c"), "IPADDRESS", true, emptyList(), Optional.empty()),
                                new ColumnDefinition(identifier("d"), "DATE", false, emptyList(), Optional.empty())),
                        false,
                        ImmutableList.of(),
                        Optional.empty()));
    }

    @Test
    public void testCreateTableAsSelect()
    {
        Query query = simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t")));
        Query querySelectColumn = simpleQuery(selectList(new Identifier("a")), table(QualifiedName.of("t")));
        Query querySelectColumns = simpleQuery(selectList(new Identifier("a"), new Identifier("b")), table(QualifiedName.of("t")));
        QualifiedName table = QualifiedName.of("foo");

        assertStatement("CREATE TABLE foo AS SELECT * FROM t",
                new CreateTableAsSelect(table, query, false, ImmutableList.of(), true, Optional.empty(), Optional.empty()));
        assertStatement("CREATE TABLE foo(x) AS SELECT a FROM t",
                new CreateTableAsSelect(table, querySelectColumn, false, ImmutableList.of(), true, Optional.of(ImmutableList.of(new Identifier("x"))), Optional.empty()));
        assertStatement("CREATE TABLE foo(x,y) AS SELECT a,b FROM t",
                new CreateTableAsSelect(table, querySelectColumns, false, ImmutableList.of(), true, Optional.of(ImmutableList.of(new Identifier("x"), new Identifier("y"))), Optional.empty()));

        assertStatement("CREATE TABLE IF NOT EXISTS foo AS SELECT * FROM t",
                new CreateTableAsSelect(table, query, true, ImmutableList.of(), true, Optional.empty(), Optional.empty()));
        assertStatement("CREATE TABLE IF NOT EXISTS foo(x) AS SELECT a FROM t",
                new CreateTableAsSelect(table, querySelectColumn, true, ImmutableList.of(), true, Optional.of(ImmutableList.of(new Identifier("x"))), Optional.empty()));
        assertStatement("CREATE TABLE IF NOT EXISTS foo(x,y) AS SELECT a,b FROM t",
                new CreateTableAsSelect(table, querySelectColumns, true, ImmutableList.of(), true, Optional.of(ImmutableList.of(new Identifier("x"), new Identifier("y"))), Optional.empty()));

        assertStatement("CREATE TABLE foo AS SELECT * FROM t WITH NO DATA",
                new CreateTableAsSelect(table, query, false, ImmutableList.of(), false, Optional.empty(), Optional.empty()));
        assertStatement("CREATE TABLE foo(x) AS SELECT a FROM t WITH NO DATA",
                new CreateTableAsSelect(table, querySelectColumn, false, ImmutableList.of(), false, Optional.of(ImmutableList.of(new Identifier("x"))), Optional.empty()));
        assertStatement("CREATE TABLE foo(x,y) AS SELECT a,b FROM t WITH NO DATA",
                new CreateTableAsSelect(table, querySelectColumns, false, ImmutableList.of(), false, Optional.of(ImmutableList.of(new Identifier("x"), new Identifier("y"))), Optional.empty()));

        List<Property> properties = ImmutableList.of(
                new Property(new Identifier("string"), new StringLiteral("bar")),
                new Property(new Identifier("long"), new LongLiteral("42")),
                new Property(
                        new Identifier("computed"),
                        new FunctionCall(QualifiedName.of("concat"), ImmutableList.of(new StringLiteral("ban"), new StringLiteral("ana")))),
                new Property(new Identifier("a"), new ArrayConstructor(ImmutableList.of(new StringLiteral("v1"), new StringLiteral("v2")))));

        assertStatement("CREATE TABLE foo " +
                        "WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] ) " +
                        "AS " +
                        "SELECT * FROM t",
                new CreateTableAsSelect(table, query, false, properties, true, Optional.empty(), Optional.empty()));
        assertStatement("CREATE TABLE foo(x) " +
                        "WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] ) " +
                        "AS " +
                        "SELECT a FROM t",
                new CreateTableAsSelect(table, querySelectColumn, false, properties, true, Optional.of(ImmutableList.of(new Identifier("x"))), Optional.empty()));
        assertStatement("CREATE TABLE foo(x,y) " +
                        "WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] ) " +
                        "AS " +
                        "SELECT a,b FROM t",
                new CreateTableAsSelect(table, querySelectColumns, false, properties, true, Optional.of(ImmutableList.of(new Identifier("x"), new Identifier("y"))), Optional.empty()));

        assertStatement("CREATE TABLE foo " +
                        "WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] ) " +
                        "AS " +
                        "SELECT * FROM t " +
                        "WITH NO DATA",
                new CreateTableAsSelect(table, query, false, properties, false, Optional.empty(), Optional.empty()));
        assertStatement("CREATE TABLE foo(x) " +
                        "WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] ) " +
                        "AS " +
                        "SELECT a FROM t " +
                        "WITH NO DATA",
                new CreateTableAsSelect(table, querySelectColumn, false, properties, false, Optional.of(ImmutableList.of(new Identifier("x"))), Optional.empty()));
        assertStatement("CREATE TABLE foo(x,y) " +
                        "WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] ) " +
                        "AS " +
                        "SELECT a,b FROM t " +
                        "WITH NO DATA",
                new CreateTableAsSelect(table, querySelectColumns, false, properties, false, Optional.of(ImmutableList.of(new Identifier("x"), new Identifier("y"))), Optional.empty()));

        assertStatement("CREATE TABLE foo COMMENT 'test'" +
                        "WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] ) " +
                        "AS " +
                        "SELECT * FROM t " +
                        "WITH NO DATA",
                new CreateTableAsSelect(table, query, false, properties, false, Optional.empty(), Optional.of("test")));
        assertStatement("CREATE TABLE foo(x) COMMENT 'test'" +
                        "WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] ) " +
                        "AS " +
                        "SELECT a FROM t " +
                        "WITH NO DATA",
                new CreateTableAsSelect(table, querySelectColumn, false, properties, false, Optional.of(ImmutableList.of(new Identifier("x"))), Optional.of("test")));
        assertStatement("CREATE TABLE foo(x,y) COMMENT 'test'" +
                        "WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] ) " +
                        "AS " +
                        "SELECT a,b FROM t " +
                        "WITH NO DATA",
                new CreateTableAsSelect(table, querySelectColumns, false, properties, false, Optional.of(ImmutableList.of(new Identifier("x"), new Identifier("y"))), Optional.of("test")));
        assertStatement("CREATE TABLE foo(x,y) COMMENT 'test'" +
                        "WITH ( \"string\" = 'bar', \"long\" = 42, computed = 'ban' || 'ana', a = ARRAY[ 'v1', 'v2' ] ) " +
                        "AS " +
                        "SELECT a,b FROM t " +
                        "WITH NO DATA",
                new CreateTableAsSelect(table, querySelectColumns, false, properties, false, Optional.of(ImmutableList.of(new Identifier("x"), new Identifier("y"))), Optional.of("test")));
    }

    @Test
    public void testCreateTableAsWith()
    {
        String queryParenthesizedWith = "CREATE TABLE foo " +
                "AS " +
                "( WITH t(x) AS (VALUES 1) " +
                "TABLE t ) " +
                "WITH NO DATA";
        String queryUnparenthesizedWith = "CREATE TABLE foo " +
                "AS " +
                "WITH t(x) AS (VALUES 1) " +
                "TABLE t " +
                "WITH NO DATA";
        String queryParenthesizedWithHasAlias = "CREATE TABLE foo(a) " +
                "AS " +
                "( WITH t(x) AS (VALUES 1) " +
                "TABLE t ) " +
                "WITH NO DATA";
        String queryUnparenthesizedWithHasAlias = "CREATE TABLE foo(a) " +
                "AS " +
                "WITH t(x) AS (VALUES 1) " +
                "TABLE t " +
                "WITH NO DATA";

        QualifiedName table = QualifiedName.of("foo");

        Query query = new Query(Optional.of(new With(false, ImmutableList.of(
                new WithQuery(identifier("t"),
                        query(new Values(ImmutableList.of(new LongLiteral("1")))),
                        Optional.of(ImmutableList.of(identifier("x"))))))),
                new Table(QualifiedName.of("t")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        assertStatement(queryParenthesizedWith, new CreateTableAsSelect(table, query, false, ImmutableList.of(), false, Optional.empty(), Optional.empty()));
        assertStatement(queryUnparenthesizedWith, new CreateTableAsSelect(table, query, false, ImmutableList.of(), false, Optional.empty(), Optional.empty()));
        assertStatement(queryParenthesizedWithHasAlias, new CreateTableAsSelect(table, query, false, ImmutableList.of(), false, Optional.of(ImmutableList.of(new Identifier("a"))), Optional.empty()));
        assertStatement(queryUnparenthesizedWithHasAlias, new CreateTableAsSelect(table, query, false, ImmutableList.of(), false, Optional.of(ImmutableList.of(new Identifier("a"))), Optional.empty()));
    }

    @Test
    public void testDropTable()
    {
        assertStatement("DROP TABLE a", new DropTable(QualifiedName.of("a"), false));
        assertStatement("DROP TABLE a.b", new DropTable(QualifiedName.of("a", "b"), false));
        assertStatement("DROP TABLE a.b.c", new DropTable(QualifiedName.of("a", "b", "c"), false));

        assertStatement("DROP TABLE IF EXISTS a", new DropTable(QualifiedName.of("a"), true));
        assertStatement("DROP TABLE IF EXISTS a.b", new DropTable(QualifiedName.of("a", "b"), true));
        assertStatement("DROP TABLE IF EXISTS a.b.c", new DropTable(QualifiedName.of("a", "b", "c"), true));
    }

    @Test
    public void testTruncateTable()
            throws Exception
    {
        assertStatement("TRUNCATE TABLE a", new TruncateTable(QualifiedName.of("a")));
        assertStatement("TRUNCATE TABLE a.b", new TruncateTable(QualifiedName.of("a", "b")));
        assertStatement("TRUNCATE TABLE a.b.c", new TruncateTable(QualifiedName.of("a", "b", "c")));
    }

    @Test
    public void testRenameView()
    {
        assertStatement("ALTER VIEW a RENAME TO b", new RenameView(QualifiedName.of("a"), QualifiedName.of("b"), false));
        assertStatement("ALTER VIEW IF EXISTS a RENAME TO b", new RenameView(QualifiedName.of("a"), QualifiedName.of("b"), true));
    }

    @Test
    public void testDropView()
    {
        assertStatement("DROP VIEW a", new DropView(QualifiedName.of("a"), false));
        assertStatement("DROP VIEW a.b", new DropView(QualifiedName.of("a", "b"), false));
        assertStatement("DROP VIEW a.b.c", new DropView(QualifiedName.of("a", "b", "c"), false));

        assertStatement("DROP VIEW IF EXISTS a", new DropView(QualifiedName.of("a"), true));
        assertStatement("DROP VIEW IF EXISTS a.b", new DropView(QualifiedName.of("a", "b"), true));
        assertStatement("DROP VIEW IF EXISTS a.b.c", new DropView(QualifiedName.of("a", "b", "c"), true));
    }

    @Test
    public void testDropMaterializedView()
    {
        assertStatement("DROP MATERIALIZED VIEW a", new DropMaterializedView(Optional.empty(), QualifiedName.of("a"), false));
        assertStatement("DROP MATERIALIZED VIEW a.b", new DropMaterializedView(Optional.empty(), QualifiedName.of("a", "b"), false));
        assertStatement("DROP MATERIALIZED VIEW a.b.c", new DropMaterializedView(Optional.empty(), QualifiedName.of("a", "b", "c"), false));

        assertStatement("DROP MATERIALIZED VIEW IF EXISTS a", new DropMaterializedView(Optional.empty(), QualifiedName.of("a"), true));
        assertStatement("DROP MATERIALIZED VIEW IF EXISTS a.b", new DropMaterializedView(Optional.empty(), QualifiedName.of("a", "b"), true));
        assertStatement("DROP MATERIALIZED VIEW IF EXISTS a.b.c", new DropMaterializedView(Optional.empty(), QualifiedName.of("a", "b", "c"), true));
    }

    @Test
    public void testRefreshMaterializedView()
    {
        assertStatement(
                "REFRESH MATERIALIZED VIEW a WHERE p = 'x'",
                new RefreshMaterializedView(
                        table(QualifiedName.of("a")),
                        new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new Identifier("p"), new StringLiteral("x"))));
        assertStatement(
                "REFRESH MATERIALIZED VIEW a.b WHERE p = 'x'",
                new RefreshMaterializedView(
                        table(QualifiedName.of("a", "b")),
                        new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new Identifier("p"), new StringLiteral("x"))));
    }

    @Test
    public void testDropFunction()
    {
        assertStatement("DROP FUNCTION a", new DropFunction(QualifiedName.of("a"), Optional.empty(), false, false));
        assertStatement("DROP FUNCTION a.b", new DropFunction(QualifiedName.of("a", "b"), Optional.empty(), false, false));
        assertStatement("DROP FUNCTION a.b.c", new DropFunction(QualifiedName.of("a", "b", "c"), Optional.empty(), false, false));

        assertStatement("DROP FUNCTION a()", new DropFunction(QualifiedName.of("a"), Optional.of(ImmutableList.of()), false, false));
        assertStatement("DROP FUNCTION a.b()", new DropFunction(QualifiedName.of("a", "b"), Optional.of(ImmutableList.of()), false, false));
        assertStatement("DROP FUNCTION a.b.c()", new DropFunction(QualifiedName.of("a", "b", "c"), Optional.of(ImmutableList.of()), false, false));

        assertStatement("DROP FUNCTION IF EXISTS a.b.c(int)", new DropFunction(QualifiedName.of("a", "b", "c"), Optional.of(ImmutableList.of("int")), false, true));
        assertStatement("DROP FUNCTION IF EXISTS a.b.c(bigint, double)", new DropFunction(QualifiedName.of("a", "b", "c"), Optional.of(ImmutableList.of("bigint", "double")), false, true));
        assertStatement("DROP FUNCTION IF EXISTS a.b.c(ARRAY(string), MAP(int,double))", new DropFunction(QualifiedName.of("a", "b", "c"), Optional.of(ImmutableList.of("ARRAY(string)", "MAP(int,double)")), false, true));
    }

    @Test
    public void testInsertInto()
    {
        QualifiedName table = QualifiedName.of("a");
        Query query = simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t")));

        assertStatement("INSERT INTO a SELECT * FROM t",
                new Insert(table, Optional.empty(), query));

        assertStatement("INSERT INTO a (c1, c2) SELECT * FROM t",
                new Insert(table, Optional.of(ImmutableList.of(identifier("c1"), identifier("c2"))), query));
    }

    @Test
    public void testDelete()
    {
        assertStatement("DELETE FROM t", new Delete(table(QualifiedName.of("t")), Optional.empty()));
        assertStatement("DELETE FROM \"awesome table\"", new Delete(table(QualifiedName.of("awesome table")), Optional.empty()));

        assertStatement("DELETE FROM t WHERE a = b", new Delete(table(QualifiedName.of("t")), Optional.of(
                new ComparisonExpression(ComparisonExpression.Operator.EQUAL,
                        new Identifier("a"),
                        new Identifier("b")))));
    }

    @Test
    public void testUpdate()
    {
        assertStatement("" +
                        "UPDATE foo_table\n" +
                        "    SET bar = 23, baz = 3.1415E0, bletch = 'barf'\n" +
                        "WHERE (nothing = 'fun')",
                new Update(
                        new NodeLocation(1, 1),
                        table(QualifiedName.of("foo_table")),
                        ImmutableList.of(
                                new UpdateAssignment(new Identifier("bar"), new LongLiteral("23")),
                                new UpdateAssignment(new Identifier("baz"), new DoubleLiteral("3.1415")),
                                new UpdateAssignment(new Identifier("bletch"), new StringLiteral("barf"))),
                        Optional.of(new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new Identifier("nothing"), new StringLiteral("fun")))));
    }

    @Test
    public void testWherelessUpdate()
    {
        assertStatement("" +
                        "UPDATE foo_table\n" +
                        "    SET bar = 23",
                new Update(
                        new NodeLocation(1, 1),
                        table(QualifiedName.of("foo_table")),
                        ImmutableList.of(
                                new UpdateAssignment(new Identifier("bar"), new LongLiteral("23"))),
                        Optional.empty()));
    }

    @Test
    public void testMergeInto()
    {
        NodeLocation location = new NodeLocation(1, 1);
        assertStatement("" +
                        "MERGE INTO product_sales AS s\n" +
                        "  USING monthly_sales AS ms\n" +
                        "  ON s.product_id = ms.product_id\n" +
                        "WHEN MATCHED THEN\n" +
                        "  UPDATE SET\n" +
                        "      sales = sales + ms.sales\n" +
                        "    , last_sale = ms.sale_date\n" +
                        "    , current_price = ms.price\n" +
                        "WHEN NOT MATCHED THEN\n" +
                        "  INSERT (product_id, sales, last_sale, current_price)\n" +
                        "  VALUES (ms.product_id, ms.sales, ms.sale_date, ms.price)",
                new Merge(
                        location,
                        new AliasedRelation(location, table(QualifiedName.of("product_sales")), new Identifier("s"), null),
                        aliased(table(QualifiedName.of("monthly_sales")), "ms"),
                        equal(nameReference("s", "product_id"), nameReference("ms", "product_id")),
                        ImmutableList.of(
                                new MergeUpdate(
                                        ImmutableList.of(
                                                new MergeUpdate.Assignment(new Identifier("sales"), new ArithmeticBinaryExpression(
                                                        ArithmeticBinaryExpression.Operator.ADD, nameReference("sales"), nameReference("ms", "sales"))),
                                                new MergeUpdate.Assignment(new Identifier("last_sale"), nameReference("ms", "sale_date")),
                                                new MergeUpdate.Assignment(new Identifier("current_price"), nameReference("ms", "price")))),
                                new MergeInsert(
                                        ImmutableList.of(new Identifier("product_id"), new Identifier("sales"), new Identifier("last_sale"),
                                                new Identifier("current_price")),
                                        ImmutableList.of(nameReference("ms", "product_id"), nameReference("ms", "sales"),
                                                nameReference("ms", "sale_date"), nameReference("ms", "price"))))));
    }

    @Test
    public void testRenameTable()
    {
        assertStatement("ALTER TABLE a RENAME TO b", new RenameTable(QualifiedName.of("a"), QualifiedName.of("b"), false));
        assertStatement("ALTER TABLE IF EXISTS a RENAME TO b", new RenameTable(QualifiedName.of("a"), QualifiedName.of("b"), true));
    }

    @Test
    public void testSetProperties()
    {
        assertStatement("ALTER TABLE a SET PROPERTIES (foo='bar')", new SetProperties(SetProperties.Type.TABLE, QualifiedName.of("a"), ImmutableList.of(new Property(new Identifier("foo"), new StringLiteral("bar"))), false));
        assertStatement("ALTER TABLE a SET PROPERTIES (foo=true)", new SetProperties(SetProperties.Type.TABLE, QualifiedName.of("a"), ImmutableList.of(new Property(new Identifier("foo"), new BooleanLiteral("true"))), false));
        assertStatement("ALTER TABLE a SET PROPERTIES (foo=123)", new SetProperties(SetProperties.Type.TABLE, QualifiedName.of("a"), ImmutableList.of(new Property(new Identifier("foo"), new LongLiteral("123"))), false));
        assertStatement("ALTER TABLE a SET PROPERTIES (foo=123, bar=456)", new SetProperties(SetProperties.Type.TABLE, QualifiedName.of("a"), ImmutableList.of(new Property(new Identifier("foo"), new LongLiteral("123")), new Property(new Identifier("bar"), new LongLiteral("456"))), false));
        assertStatement("ALTER TABLE a SET PROPERTIES (\" s p a c e \"='bar')", new SetProperties(SetProperties.Type.TABLE, QualifiedName.of("a"), ImmutableList.of(new Property(new Identifier(" s p a c e "), new StringLiteral("bar"))), false));

        assertInvalidStatement("ALTER TABLE a SET PROPERTIES ()", "mismatched input '\\)'. Expecting: <identifier>");
        assertStatement("ALTER TABLE IF EXISTS b SET PROPERTIES (foo=12345)", new SetProperties(SetProperties.Type.TABLE, QualifiedName.of("b"), ImmutableList.of(new Property(new Identifier("foo"), new LongLiteral("12345"))), true));
        assertInvalidStatement("ALTER TABLE IF EXISTS b SET PROPERTIES ()", "mismatched input '\\)'. Expecting: <identifier>");
    }

    @Test
    public void testRenameColumn()
    {
        assertStatement("ALTER TABLE foo.t RENAME COLUMN a TO b", new RenameColumn(QualifiedName.of("foo", "t"), identifier("a"), identifier("b"), false, false));
        assertStatement("ALTER TABLE IF EXISTS foo.t RENAME COLUMN a TO b", new RenameColumn(QualifiedName.of("foo", "t"), identifier("a"), identifier("b"), true, false));
        assertStatement("ALTER TABLE foo.t RENAME COLUMN IF EXISTS a TO b", new RenameColumn(QualifiedName.of("foo", "t"), identifier("a"), identifier("b"), false, true));
        assertStatement("ALTER TABLE IF EXISTS foo.t RENAME COLUMN IF EXISTS a TO b", new RenameColumn(QualifiedName.of("foo", "t"), identifier("a"), identifier("b"), true, true));
    }

    @Test
    public void testAnalyze()
    {
        QualifiedName table = QualifiedName.of("foo");
        assertStatement("ANALYZE foo", new Analyze(table, ImmutableList.of()));

        assertStatement("ANALYZE foo WITH ( \"string\" = 'bar', \"long\" = 42, computed = concat('ban', 'ana'), a = ARRAY[ 'v1', 'v2' ] )",
                new Analyze(table, ImmutableList.of(
                        new Property(new Identifier("string"), new StringLiteral("bar")),
                        new Property(new Identifier("long"), new LongLiteral("42")),
                        new Property(
                                new Identifier("computed"),
                                new FunctionCall(QualifiedName.of("concat"), ImmutableList.of(new StringLiteral("ban"), new StringLiteral("ana")))),
                        new Property(new Identifier("a"), new ArrayConstructor(ImmutableList.of(new StringLiteral("v1"), new StringLiteral("v2")))))));

        assertStatement("EXPLAIN ANALYZE foo", new Explain(new Analyze(table, ImmutableList.of()), false, false, ImmutableList.of()));
        assertStatement("EXPLAIN ANALYZE ANALYZE foo", new Explain(new Analyze(table, ImmutableList.of()), true, false, ImmutableList.of()));
    }

    @Test
    public void testAddColumn()
    {
        assertStatement("ALTER TABLE foo.t ADD COLUMN c bigint", new AddColumn(QualifiedName.of("foo", "t"),
                new ColumnDefinition(identifier("c"), "bigint", true, emptyList(), Optional.empty()), false, false));
        assertStatement("ALTER TABLE foo.t ADD COLUMN d double NOT NULL", new AddColumn(QualifiedName.of("foo", "t"),
                new ColumnDefinition(identifier("d"), "double", false, emptyList(), Optional.empty()), false, false));

        assertStatement("ALTER TABLE IF EXISTS foo.t ADD COLUMN d double NOT NULL",
                new AddColumn(QualifiedName.of("foo", "t"),
                        new ColumnDefinition(identifier("d"), "double", false, emptyList(), Optional.empty()), true, false));

        assertStatement("ALTER TABLE foo.t ADD COLUMN IF NOT EXISTS d double NOT NULL",
                new AddColumn(QualifiedName.of("foo", "t"),
                        new ColumnDefinition(identifier("d"), "double", false, emptyList(), Optional.empty()), false, true));

        assertStatement("ALTER TABLE IF EXISTS foo.t ADD COLUMN IF NOT EXISTS d double NOT NULL",
                new AddColumn(QualifiedName.of("foo", "t"),
                        new ColumnDefinition(identifier("d"), "double", false, emptyList(), Optional.empty()), true, true));
    }

    @Test
    public void testDropColumn()
    {
        assertStatement("ALTER TABLE foo.t DROP COLUMN c", new DropColumn(QualifiedName.of("foo", "t"), identifier("c"), false, false));
        assertStatement("ALTER TABLE \"t x\" DROP COLUMN \"c d\"", new DropColumn(QualifiedName.of("t x"), quotedIdentifier("c d"), false, false));
        assertStatement("ALTER TABLE IF EXISTS foo.t DROP COLUMN c", new DropColumn(QualifiedName.of("foo", "t"), identifier("c"), true, false));
        assertStatement("ALTER TABLE foo.t DROP COLUMN IF EXISTS c", new DropColumn(QualifiedName.of("foo", "t"), identifier("c"), false, true));
        assertStatement("ALTER TABLE IF EXISTS foo.t DROP COLUMN IF EXISTS c", new DropColumn(QualifiedName.of("foo", "t"), identifier("c"), true, true));
    }

    @Test
    public void testCreateView()
    {
        Query query = simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t")));

        assertStatement("CREATE VIEW a AS SELECT * FROM t", new CreateView(QualifiedName.of("a"), query, false, Optional.empty()));
        assertStatement("CREATE OR REPLACE VIEW a AS SELECT * FROM t", new CreateView(QualifiedName.of("a"), query, true, Optional.empty()));

        assertStatement("CREATE VIEW a SECURITY DEFINER AS SELECT * FROM t", new CreateView(QualifiedName.of("a"), query, false, Optional.of(CreateView.Security.DEFINER)));
        assertStatement("CREATE VIEW a SECURITY INVOKER AS SELECT * FROM t", new CreateView(QualifiedName.of("a"), query, false, Optional.of(CreateView.Security.INVOKER)));

        assertStatement("CREATE VIEW bar.foo AS SELECT * FROM t", new CreateView(QualifiedName.of("bar", "foo"), query, false, Optional.empty()));
        assertStatement("CREATE VIEW \"awesome view\" AS SELECT * FROM t", new CreateView(QualifiedName.of("awesome view"), query, false, Optional.empty()));
        assertStatement("CREATE VIEW \"awesome schema\".\"awesome view\" AS SELECT * FROM t", new CreateView(QualifiedName.of("awesome schema", "awesome view"), query, false, Optional.empty()));
    }

    @Test
    public void testCreateMaterializedView()
    {
        Query query = simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t")));
        assertStatement(
                "CREATE MATERIALIZED VIEW mv" +
                        " AS SELECT * FROM t",
                new CreateMaterializedView(Optional.empty(), QualifiedName.of("mv"), query, false, ImmutableList.of(), Optional.empty()));

        assertStatement(
                "CREATE MATERIALIZED VIEW mv COMMENT 'A simple materialized view'" +
                        " AS SELECT * FROM t",
                new CreateMaterializedView(Optional.empty(), QualifiedName.of("mv"), query, false, ImmutableList.of(), Optional.of("A simple materialized view")));

        assertStatement(
                "CREATE MATERIALIZED VIEW IF NOT EXISTS mv COMMENT 'A simple materialized view'" +
                        " AS SELECT * FROM t",
                new CreateMaterializedView(Optional.empty(), QualifiedName.of("mv"), query, true, ImmutableList.of(), Optional.of("A simple materialized view")));

        List<Property> properties = ImmutableList.of(
                new Property(new Identifier("partitioned_by"), new ArrayConstructor(ImmutableList.of(new StringLiteral("ds")))));
        assertStatement(
                "CREATE MATERIALIZED VIEW IF NOT EXISTS mv COMMENT 'A simple materialized view'" +
                        " WITH (partitioned_by = ARRAY ['ds'])" +
                        " AS SELECT * FROM t",
                new CreateMaterializedView(Optional.empty(), QualifiedName.of("mv"), query, true, properties, Optional.of("A simple materialized view")));

        List<Property> properties1 = ImmutableList.of(
                new Property(new Identifier("partitioned_by"), new ArrayConstructor(ImmutableList.of(new StringLiteral("ds")))),
                new Property(new Identifier("retention_days"), new LongLiteral("90")));
        assertStatement(
                "CREATE MATERIALIZED VIEW IF NOT EXISTS mv COMMENT 'A simple materialized view'" +
                        " WITH (partitioned_by = ARRAY ['ds'], retention_days = 90)" +
                        " AS SELECT * FROM t",
                new CreateMaterializedView(Optional.empty(), QualifiedName.of("mv"), query, true, properties1, Optional.of("A simple materialized view")));
    }

    @Test
    public void testCreateFunction()
    {
        assertStatement(
                "CREATE FUNCTION tan (x double)\n" +
                        "RETURNS double\n" +
                        "COMMENT 'tangent trigonometric function'\n" +
                        "LANGUAGE SQL\n" +
                        "DETERMINISTIC\n" +
                        "RETURNS NULL ON NULL INPUT\n" +
                        "RETURN sin(x) / cos(x)",
                new CreateFunction(
                        QualifiedName.of("tan"),
                        false,
                        false,
                        ImmutableList.of(new SqlParameterDeclaration(identifier("x"), "double")),
                        "double",
                        Optional.of("tangent trigonometric function"),
                        new RoutineCharacteristics(SQL, DETERMINISTIC, RETURNS_NULL_ON_NULL_INPUT),
                        new Return(new ArithmeticBinaryExpression(
                                DIVIDE,
                                new FunctionCall(QualifiedName.of("sin"), ImmutableList.of(identifier("x"))),
                                new FunctionCall(QualifiedName.of("cos"), ImmutableList.of(identifier("x")))))));

        CreateFunction createFunctionRand = new CreateFunction(
                QualifiedName.of("dev", "testing", "rand"),
                true,
                false,
                ImmutableList.of(),
                "double",
                Optional.empty(),
                new RoutineCharacteristics(SQL, NOT_DETERMINISTIC, CALLED_ON_NULL_INPUT),
                new Return(new FunctionCall(QualifiedName.of("rand"), ImmutableList.of())));
        assertStatement(
                "CREATE OR REPLACE FUNCTION dev.testing.rand ()\n" +
                        "RETURNS double\n" +
                        "LANGUAGE SQL\n" +
                        "NOT DETERMINISTIC\n" +
                        "CALLED ON NULL INPUT\n" +
                        "RETURN rand()",
                createFunctionRand);
        assertStatement(
                "CREATE OR REPLACE FUNCTION dev.testing.rand ()\n" +
                        "RETURNS double\n" +
                        "RETURN rand()",
                createFunctionRand);

        CreateFunction createTemporaryFunctionFoo = new CreateFunction(
                QualifiedName.of("foo"),
                false,
                true,
                ImmutableList.of(),
                "boolean",
                Optional.empty(),
                new RoutineCharacteristics(SQL, NOT_DETERMINISTIC, CALLED_ON_NULL_INPUT),
                new Return(new BooleanLiteral("true")));
        assertStatement(
                "CREATE TEMPORARY FUNCTION foo() \n" +
                        "RETURNS boolean \n" +
                        "RETURN true",
                createTemporaryFunctionFoo);

        assertInvalidStatement(
                "CREATE FUNCTION dev.testing.rand () RETURNS double LANGUAGE SQL LANGUAGE SQL RETURN rand()",
                "Duplicate language clause: SQL");
        assertInvalidStatement(
                "CREATE FUNCTION dev.testing.rand () RETURNS double DETERMINISTIC DETERMINISTIC RETURN rand()",
                "Duplicate determinism characteristics: DETERMINISTIC");
        assertInvalidStatement(
                "CREATE FUNCTION dev.testing.rand () RETURNS double CALLED ON NULL INPUT CALLED ON NULL INPUT RETURN rand()",
                "Duplicate null-call clause: CALLEDONNULLINPUT");
    }

    @Test
    public void testAlterFunction()
    {
        QualifiedName functionName = QualifiedName.of("testing", "default", "tan");
        assertStatement(
                "ALTER FUNCTION testing.default.tan\n" +
                        "CALLED ON NULL INPUT",
                new AlterFunction(functionName, Optional.empty(), new AlterRoutineCharacteristics(Optional.of(CALLED_ON_NULL_INPUT))));
        assertStatement(
                "ALTER FUNCTION testing.default.tan(double)\n" +
                        "RETURNS NULL ON NULL INPUT",
                new AlterFunction(functionName, Optional.of(ImmutableList.of("double")), new AlterRoutineCharacteristics(Optional.of(RETURNS_NULL_ON_NULL_INPUT))));

        assertInvalidStatement(
                "ALTER FUNCTION testing.default.tan",
                "No alter routine characteristics specified");
        assertInvalidStatement(
                "ALTER FUNCTION testing.default.tan\n" +
                        "RETURNS NULL ON NULL INPUT\n" +
                        "RETURNS NULL ON NULL INPUT",
                "Duplicate null-call clause: RETURNSNULLONNULLINPUT");
    }

    @Test
    public void testGrant()
    {
        assertStatement("GRANT INSERT, DELETE ON t TO u",
                new Grant(
                        Optional.of(ImmutableList.of("INSERT", "DELETE")),
                        false,
                        QualifiedName.of("t"),
                        new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("u")),
                        false));
        assertStatement("GRANT SELECT ON t TO ROLE PUBLIC WITH GRANT OPTION",
                new Grant(
                        Optional.of(ImmutableList.of("SELECT")),
                        false, QualifiedName.of("t"),
                        new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("PUBLIC")),
                        true));
        assertStatement("GRANT ALL PRIVILEGES ON t TO USER u",
                new Grant(
                        Optional.empty(),
                        false,
                        QualifiedName.of("t"),
                        new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("u")),
                        false));
        assertStatement("GRANT taco ON \"t\" TO ROLE \"public\" WITH GRANT OPTION",
                new Grant(
                        Optional.of(ImmutableList.of("taco")),
                        false,
                        QualifiedName.of("t"),
                        new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("public")),
                        true));
    }

    @Test
    public void testRevoke()
    {
        assertStatement("REVOKE INSERT, DELETE ON t FROM u",
                new Revoke(
                        false,
                        Optional.of(ImmutableList.of("INSERT", "DELETE")),
                        false,
                        QualifiedName.of("t"),
                        new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("u"))));
        assertStatement("REVOKE GRANT OPTION FOR SELECT ON t FROM ROLE PUBLIC",
                new Revoke(
                        true,
                        Optional.of(ImmutableList.of("SELECT")),
                        false,
                        QualifiedName.of("t"),
                        new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("PUBLIC"))));
        assertStatement("REVOKE ALL PRIVILEGES ON TABLE t FROM USER u",
                new Revoke(
                        false,
                        Optional.empty(),
                        true,
                        QualifiedName.of("t"),
                        new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("u"))));
        assertStatement("REVOKE taco ON TABLE \"t\" FROM \"u\"",
                new Revoke(
                        false,
                        Optional.of(ImmutableList.of("taco")),
                        true,
                        QualifiedName.of("t"),
                        new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("u"))));
    }

    @Test
    public void testShowGrants()
    {
        assertStatement("SHOW GRANTS ON TABLE t",
                new ShowGrants(true, Optional.of(QualifiedName.of("t"))));
        assertStatement("SHOW GRANTS ON t",
                new ShowGrants(false, Optional.of(QualifiedName.of("t"))));
        assertStatement("SHOW GRANTS",
                new ShowGrants(false, Optional.empty()));
    }

    @Test
    public void testShowRoles()
            throws Exception
    {
        assertStatement("SHOW ROLES",
                new ShowRoles(Optional.empty(), false));
        assertStatement("SHOW ROLES FROM foo",
                new ShowRoles(Optional.of(new Identifier("foo")), false));
        assertStatement("SHOW ROLES IN foo",
                new ShowRoles(Optional.of(new Identifier("foo")), false));

        assertStatement("SHOW CURRENT ROLES",
                new ShowRoles(Optional.empty(), true));
        assertStatement("SHOW CURRENT ROLES FROM foo",
                new ShowRoles(Optional.of(new Identifier("foo")), true));
        assertStatement("SHOW CURRENT ROLES IN foo",
                new ShowRoles(Optional.of(new Identifier("foo")), true));
    }

    @Test
    public void testShowRoleGrants()
    {
        assertStatement("SHOW ROLE GRANTS",
                new ShowRoleGrants(Optional.empty(), Optional.empty()));
        assertStatement("SHOW ROLE GRANTS FROM catalog",
                new ShowRoleGrants(Optional.of(new Identifier("catalog"))));
    }

    @Test
    public void testWith()
    {
        assertStatement("WITH a (t, u) AS (SELECT * FROM x), b AS (SELECT * FROM y) TABLE z",
                new Query(Optional.of(new With(false, ImmutableList.of(
                        new WithQuery(identifier("a"), simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("x"))), Optional.of(ImmutableList.of(identifier("t"), identifier("u")))),
                        new WithQuery(identifier("b"), simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("y"))), Optional.empty())))),
                        new Table(QualifiedName.of("z")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("WITH RECURSIVE a AS (SELECT * FROM x) TABLE y",
                new Query(Optional.of(new With(true, ImmutableList.of(
                        new WithQuery(identifier("a"), simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("x"))), Optional.empty())))),
                        new Table(QualifiedName.of("y")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Test
    public void testNoWith()
    {
        assertStatement("SELECT * FROM t",
                simpleQuery(
                        selectList(new AllColumns()),
                        table(QualifiedName.of("t"))));

        assertInvalidStatement("SELECT * FROM my_table ORDER BY column1, column2 OFFSET 5 ROWS LIMIT \"\"", ".*mismatched input.*");
        assertInvalidStatement("\"\"", ".*mismatched input.*");
    }

    @Test
    public void testImplicitJoin()
    {
        assertStatement("SELECT * FROM a, b",
                simpleQuery(selectList(new AllColumns()),
                        new Join(Join.Type.IMPLICIT,
                                new Table(QualifiedName.of("a")),
                                new Table(QualifiedName.of("b")),
                                Optional.empty())));
    }

    @Test
    public void testExplain()
    {
        assertStatement("EXPLAIN SELECT * FROM t",
                new Explain(simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))), false, false, ImmutableList.of()));
        assertStatement("EXPLAIN (TYPE LOGICAL) SELECT * FROM t",
                new Explain(
                        simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))),
                        false,
                        false,
                        ImmutableList.of(new ExplainType(ExplainType.Type.LOGICAL))));
        assertStatement("EXPLAIN (TYPE LOGICAL, FORMAT TEXT) SELECT * FROM t",
                new Explain(
                        simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))),
                        false,
                        false,
                        ImmutableList.of(
                                new ExplainType(ExplainType.Type.LOGICAL),
                                new ExplainFormat(ExplainFormat.Type.TEXT))));
    }

    @Test
    public void testExplainVerbose()
    {
        assertStatement("EXPLAIN VERBOSE SELECT * FROM t",
                new Explain(simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))), false, true, ImmutableList.of()));
    }

    @Test
    public void testExplainVerboseTypeLogical()
    {
        assertStatement("EXPLAIN VERBOSE (type LOGICAL) SELECT * FROM t",
                new Explain(simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))), false, true, ImmutableList.of(new ExplainType(ExplainType.Type.LOGICAL))));
    }

    @Test
    public void testExplainAnalyze()
    {
        assertStatement("EXPLAIN ANALYZE SELECT * FROM t",
                new Explain(simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))), true, false, ImmutableList.of()));
    }

    @Test
    public void testExplainAnalyzeTypeDistributed()
    {
        assertStatement("EXPLAIN ANALYZE (type DISTRIBUTED) SELECT * FROM t",
                new Explain(simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))), true, false, ImmutableList.of(new ExplainType(ExplainType.Type.DISTRIBUTED))));
    }

    @Test
    public void testExplainAnalyzeFormatJson()
    {
        assertStatement("EXPLAIN ANALYZE (format JSON) SELECT * FROM t",
                new Explain(simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))), true, false, ImmutableList.of(new ExplainFormat(ExplainFormat.Type.JSON))));
    }

    @Test
    public void testExplainAnalyzeFormatJsonTypeDistributed()
    {
        assertStatement("EXPLAIN ANALYZE (format JSON, type DISTRIBUTED) SELECT * FROM t",
                new Explain(simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))), true, false, ImmutableList.of(new ExplainFormat(ExplainFormat.Type.JSON), new ExplainType(ExplainType.Type.DISTRIBUTED))));
    }

    @Test
    public void testExplainAnalyzeVerbose()
    {
        assertStatement("EXPLAIN ANALYZE VERBOSE SELECT * FROM t",
                new Explain(simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))), true, true, ImmutableList.of()));
    }

    @Test
    public void testExplainAnalyzeVerboseTypeDistributed()
    {
        assertStatement("EXPLAIN ANALYZE VERBOSE (type DISTRIBUTED) SELECT * FROM t",
                new Explain(simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))), true, true, ImmutableList.of(new ExplainType(ExplainType.Type.DISTRIBUTED))));
    }

    @Test
    public void testExplainAnalyzeVerboseFormatJson()
    {
        assertStatement("EXPLAIN ANALYZE VERBOSE (format JSON) SELECT * FROM t",
                new Explain(simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))), true, true, ImmutableList.of(new ExplainFormat(ExplainFormat.Type.JSON))));
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
                                        Optional.empty()),
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
                                                        Optional.empty()),
                                                new Table(QualifiedName.of("c")),
                                                Optional.of(new NaturalJoin())),
                                        new Table(QualifiedName.of("d")),
                                        Optional.empty()),
                                new Table(QualifiedName.of("e")),
                                Optional.of(new NaturalJoin()))));
    }

    @Test
    public void testUnnest()
    {
        assertStatement("SELECT * FROM t CROSS JOIN UNNEST(a)",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Join(
                                Join.Type.CROSS,
                                new Table(QualifiedName.of("t")),
                                new Unnest(ImmutableList.of(new Identifier("a")), false),
                                Optional.empty())));
        assertStatement("SELECT * FROM t CROSS JOIN UNNEST(a, b) WITH ORDINALITY",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Join(
                                Join.Type.CROSS,
                                new Table(QualifiedName.of("t")),
                                new Unnest(ImmutableList.of(new Identifier("a"), new Identifier("b")), true),
                                Optional.empty())));
        assertStatement("SELECT * FROM t FULL JOIN UNNEST(a) AS tmp (c) ON true",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Join(
                                Join.Type.FULL,
                                new Table(QualifiedName.of("t")),
                                new AliasedRelation(new Unnest(ImmutableList.of(new Identifier("a")), false), new Identifier("tmp"), ImmutableList.of(new Identifier("c"))),
                                Optional.of(new JoinOn(BooleanLiteral.TRUE_LITERAL)))));
    }

    @Test
    public void testLateral()
    {
        Lateral lateralRelation = new Lateral(new Query(
                Optional.empty(),
                new Values(ImmutableList.of(new LongLiteral("1"))),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));

        assertStatement("SELECT * FROM t, LATERAL (VALUES 1) a(x)",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Join(
                                Join.Type.IMPLICIT,
                                new Table(QualifiedName.of("t")),
                                new AliasedRelation(lateralRelation, identifier("a"), ImmutableList.of(identifier("x"))),
                                Optional.empty())));

        assertStatement("SELECT * FROM t CROSS JOIN LATERAL (VALUES 1) ",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Join(
                                Join.Type.CROSS,
                                new Table(QualifiedName.of("t")),
                                lateralRelation,
                                Optional.empty())));

        assertStatement("SELECT * FROM t FULL JOIN LATERAL (VALUES 1) ON true",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Join(
                                Join.Type.FULL,
                                new Table(QualifiedName.of("t")),
                                lateralRelation,
                                Optional.of(new JoinOn(BooleanLiteral.TRUE_LITERAL)))));
    }

    @Test
    public void testStartTransaction()
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
    {
        assertStatement("COMMIT", new Commit());
        assertStatement("COMMIT WORK", new Commit());
    }

    @Test
    public void testRollback()
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
                                        new AtTimeZone(new TimestampLiteral("2012-10-31 01:00 UTC"), new StringLiteral("America/Los_Angeles"))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Test
    public void testLambda()
    {
        assertExpression("() -> x",
                new LambdaExpression(
                        ImmutableList.of(),
                        new Identifier("x")));
        assertExpression("x -> sin(x)",
                new LambdaExpression(
                        ImmutableList.of(new LambdaArgumentDeclaration(identifier("x"))),
                        new FunctionCall(QualifiedName.of("sin"), ImmutableList.of(new Identifier("x")))));
        assertExpression("(x, y) -> mod(x, y)",
                new LambdaExpression(
                        ImmutableList.of(new LambdaArgumentDeclaration(identifier("x")), new LambdaArgumentDeclaration(identifier("y"))),
                        new FunctionCall(
                                QualifiedName.of("mod"),
                                ImmutableList.of(new Identifier("x"), new Identifier("y")))));
    }

    @Test
    public void testNonReserved()
    {
        assertStatement("SELECT zone FROM t",
                simpleQuery(
                        selectList(new Identifier("zone")),
                        table(QualifiedName.of("t"))));
        assertStatement("SELECT INCLUDING, EXCLUDING, PROPERTIES FROM t",
                simpleQuery(
                        selectList(
                                new Identifier("INCLUDING"),
                                new Identifier("EXCLUDING"),
                                new Identifier("PROPERTIES")),
                        table(QualifiedName.of("t"))));
        assertStatement("SELECT ALL, SOME, ANY FROM t",
                simpleQuery(
                        selectList(
                                new Identifier("ALL"),
                                new Identifier("SOME"),
                                new Identifier("ANY")),
                        table(QualifiedName.of("t"))));
        assertStatement("SELECT CURRENT_ROLE, t.current_role FROM t",
                simpleQuery(
                        selectList(
                                new Identifier("CURRENT_ROLE"),
                                new DereferenceExpression(new Identifier("t"), new Identifier("current_role"))),
                        table(QualifiedName.of("t"))));

        assertExpression("stats", new Identifier("stats"));
        assertExpression("nfd", new Identifier("nfd"));
        assertExpression("nfc", new Identifier("nfc"));
        assertExpression("nfkd", new Identifier("nfkd"));
        assertExpression("nfkc", new Identifier("nfkc"));
        assertExpression("current_role", new Identifier("current_role"));
    }

    @Test
    public void testBinaryLiteralToHex()
    {
        // note that toHexString() always outputs in upper case
        assertEquals(new BinaryLiteral("ab 01").toHexString(), "AB01");
    }

    @Test
    public void testCall()
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
                new Prepare(identifier("myquery"), simpleQuery(
                        selectList(new AllColumns()),
                        table(QualifiedName.of("foo")))));
    }

    @Test
    public void testPrepareWithParameters()
    {
        assertStatement("PREPARE myquery FROM SELECT ?, ? FROM foo",
                new Prepare(identifier("myquery"), simpleQuery(
                        selectList(new Parameter(0), new Parameter(1)),
                        table(QualifiedName.of("foo")))));
    }

    @Test
    public void testDeallocatePrepare()
    {
        assertStatement("DEALLOCATE PREPARE myquery", new Deallocate(identifier("myquery")));
    }

    @Test
    public void testExecute()
    {
        assertStatement("EXECUTE myquery", new Execute(identifier("myquery"), emptyList()));
    }

    @Test
    public void testExecuteWithUsing()
    {
        assertStatement("EXECUTE myquery USING 1, 'abc', ARRAY ['hello']",
                new Execute(identifier("myquery"), ImmutableList.of(new LongLiteral("1"), new StringLiteral("abc"), new ArrayConstructor(ImmutableList.of(new StringLiteral("hello"))))));
    }

    @Test
    public void testExists()
    {
        assertStatement("SELECT EXISTS(SELECT 1)", simpleQuery(selectList(exists(simpleQuery(selectList(new LongLiteral("1")))))));

        assertStatement(
                "SELECT EXISTS(SELECT 1) = EXISTS(SELECT 2)",
                simpleQuery(
                        selectList(new ComparisonExpression(
                                ComparisonExpression.Operator.EQUAL,
                                exists(simpleQuery(selectList(new LongLiteral("1")))),
                                exists(simpleQuery(selectList(new LongLiteral("2"))))))));

        assertStatement(
                "SELECT NOT EXISTS(SELECT 1) = EXISTS(SELECT 2)",
                simpleQuery(
                        selectList(
                                new NotExpression(
                                        new ComparisonExpression(
                                                ComparisonExpression.Operator.EQUAL,
                                                exists(simpleQuery(selectList(new LongLiteral("1")))),
                                                exists(simpleQuery(selectList(new LongLiteral("2")))))))));

        assertStatement(
                "SELECT (NOT EXISTS(SELECT 1)) = EXISTS(SELECT 2)",
                simpleQuery(
                        selectList(
                                new ComparisonExpression(
                                        ComparisonExpression.Operator.EQUAL,
                                        new NotExpression(exists(simpleQuery(selectList(new LongLiteral("1"))))),
                                        exists(simpleQuery(selectList(new LongLiteral("2"))))))));
    }

    private static ExistsPredicate exists(Query query)
    {
        return new ExistsPredicate(new SubqueryExpression(query));
    }

    @Test
    public void testShowStats()
    {
        final String[] tableNames = {"t", "s.t", "c.s.t"};

        for (String fullName : tableNames) {
            QualifiedName qualifiedName = QualifiedName.of(Arrays.asList(fullName.split("\\.")));
            assertStatement(format("SHOW STATS FOR %s", qualifiedName), new ShowStats(new Table(qualifiedName)));
        }
    }

    @Test
    public void testShowStatsForQuery()
    {
        final String[] tableNames = {"t", "s.t", "c.s.t"};

        for (String fullName : tableNames) {
            QualifiedName qualifiedName = QualifiedName.of(Arrays.asList(fullName.split("\\.")));
            assertStatement(format("SHOW STATS FOR (SELECT * FROM %s)", qualifiedName),
                    createShowStats(qualifiedName, ImmutableList.of(new AllColumns()), Optional.empty()));
            assertStatement(format("SHOW STATS FOR (SELECT * FROM %s WHERE field > 0)", qualifiedName),
                    createShowStats(qualifiedName,
                            ImmutableList.of(new AllColumns()),
                            Optional.of(
                                    new ComparisonExpression(GREATER_THAN,
                                            new Identifier("field"),
                                            new LongLiteral("0")))));
            assertStatement(format("SHOW STATS FOR (SELECT * FROM %s WHERE field > 0 or field < 0)", qualifiedName),
                    createShowStats(qualifiedName,
                            ImmutableList.of(new AllColumns()),
                            Optional.of(
                                    new LogicalBinaryExpression(LogicalBinaryExpression.Operator.OR,
                                            new ComparisonExpression(GREATER_THAN,
                                                    new Identifier("field"),
                                                    new LongLiteral("0")),
                                            new ComparisonExpression(LESS_THAN,
                                                    new Identifier("field"),
                                                    new LongLiteral("0"))))));
        }
    }

    private ShowStats createShowStats(QualifiedName name, List<SelectItem> selects, Optional<Expression> where)
    {
        return new ShowStats(
                new TableSubquery(simpleQuery(new Select(false, selects),
                        new Table(name),
                        where,
                        Optional.empty())));
    }

    @Test
    public void testDescribeOutput()
    {
        assertStatement("DESCRIBE OUTPUT myquery", new DescribeOutput(identifier("myquery")));
    }

    @Test
    public void testDescribeInput()
    {
        assertStatement("DESCRIBE INPUT myquery", new DescribeInput(identifier("myquery")));
    }

    @Test
    public void testAggregationFilter()
    {
        assertStatement("SELECT SUM(x) FILTER (WHERE x > 4)",
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(
                                        new FunctionCall(
                                                QualifiedName.of("SUM"),
                                                Optional.empty(),
                                                Optional.of(new ComparisonExpression(
                                                        ComparisonExpression.Operator.GREATER_THAN,
                                                        new Identifier("x"),
                                                        new LongLiteral("4"))),
                                                Optional.empty(),
                                                false,
                                                false,
                                                ImmutableList.of(new Identifier("x")))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Test
    public void testQuantifiedComparison()
    {
        assertExpression("col1 < ANY (SELECT col2 FROM table1)",
                new QuantifiedComparisonExpression(
                        LESS_THAN,
                        QuantifiedComparisonExpression.Quantifier.ANY,
                        identifier("col1"),
                        new SubqueryExpression(simpleQuery(selectList(new SingleColumn(identifier("col2"))), table(QualifiedName.of("table1"))))));
        assertExpression("col1 = ALL (VALUES ROW(1), ROW(2))",
                new QuantifiedComparisonExpression(
                        ComparisonExpression.Operator.EQUAL,
                        QuantifiedComparisonExpression.Quantifier.ALL,
                        identifier("col1"),
                        new SubqueryExpression(query(values(row(new LongLiteral("1")), row(new LongLiteral("2")))))));
        assertExpression("col1 >= SOME (SELECT 10)",
                new QuantifiedComparisonExpression(
                        ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL,
                        QuantifiedComparisonExpression.Quantifier.SOME,
                        identifier("col1"),
                        new SubqueryExpression(simpleQuery(selectList(new LongLiteral("10"))))));
    }

    @Test
    public void testAggregationWithOrderBy()
    {
        assertExpression("array_agg(x ORDER BY x DESC)",
                new FunctionCall(
                        QualifiedName.of("array_agg"),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new OrderBy(ImmutableList.of(new SortItem(identifier("x"), DESCENDING, UNDEFINED)))),
                        false,
                        ImmutableList.of(identifier("x"))));
        assertStatement("SELECT array_agg(x ORDER BY t.y) FROM t",
                new Query(
                        Optional.empty(),
                        new QuerySpecification(
                                selectList(
                                        new FunctionCall(
                                                QualifiedName.of("array_agg"),
                                                Optional.empty(),
                                                Optional.empty(),
                                                Optional.of(new OrderBy(ImmutableList.of(new SortItem(new DereferenceExpression(new Identifier("t"), identifier("y")), ASCENDING, UNDEFINED)))),
                                                false,
                                                ImmutableList.of(new Identifier("x")))),
                                Optional.of(table(QualifiedName.of("t"))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Test
    public void testCreateRole()
            throws Exception
    {
        assertStatement("CREATE ROLE role", new CreateRole(new Identifier("role"), Optional.empty()));
        assertStatement("CREATE ROLE role1 WITH ADMIN admin",
                new CreateRole(
                        new Identifier("role1"),
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("admin")))))));
        assertStatement("CREATE ROLE \"role\" WITH ADMIN \"admin\"",
                new CreateRole(
                        new Identifier("role"),
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("admin")))))));
        assertStatement("CREATE ROLE \"ro le\" WITH ADMIN \"ad min\"",
                new CreateRole(
                        new Identifier("ro le"),
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("ad min")))))));
        assertStatement("CREATE ROLE \"!@#$%^&*'\" WITH ADMIN \"ад\"\"мін\"",
                new CreateRole(
                        new Identifier("!@#$%^&*'"),
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("ад\"мін")))))));
        assertStatement("CREATE ROLE role2 WITH ADMIN USER admin1",
                new CreateRole(
                        new Identifier("role2"),
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("admin1")))))));
        assertStatement("CREATE ROLE role2 WITH ADMIN ROLE role1",
                new CreateRole(
                        new Identifier("role2"),
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("role1")))))));
        assertStatement("CREATE ROLE role2 WITH ADMIN CURRENT_USER",
                new CreateRole(
                        new Identifier("role2"),
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.CURRENT_USER,
                                Optional.empty()))));
        assertStatement("CREATE ROLE role2 WITH ADMIN CURRENT_ROLE",
                new CreateRole(
                        new Identifier("role2"),
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.CURRENT_ROLE,
                                Optional.empty()))));
    }

    @Test
    public void testDropRole()
            throws Exception
    {
        assertStatement("DROP ROLE role", new DropRole(new Identifier("role")));
        assertStatement("DROP ROLE \"role\"", new DropRole(new Identifier("role")));
        assertStatement("DROP ROLE \"ro le\"", new DropRole(new Identifier("ro le")));
        assertStatement("DROP ROLE \"!@#$%^&*'ад\"\"мін\"", new DropRole(new Identifier("!@#$%^&*'ад\"мін")));
    }

    @Test
    public void testGrantRoles()
            throws Exception
    {
        assertStatement("GRANT role1 TO user1",
                new GrantRoles(
                        ImmutableSet.of(new Identifier("role1")),
                        ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("user1"))),
                        false,
                        Optional.empty()));
        assertStatement("GRANT role1, role2, role3 TO user1, USER user2, ROLE role4 WITH ADMIN OPTION",
                new GrantRoles(
                        ImmutableSet.of(new Identifier("role1"), new Identifier("role2"), new Identifier("role3")),
                        ImmutableSet.of(
                                new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("user1")),
                                new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("user2")),
                                new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("role4"))),
                        true,
                        Optional.empty()));
        assertStatement("GRANT role1 TO user1 WITH ADMIN OPTION GRANTED BY admin",
                new GrantRoles(
                        ImmutableSet.of(new Identifier("role1")),
                        ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("user1"))),
                        true,
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("admin")))))));
        assertStatement("GRANT role1 TO USER user1 WITH ADMIN OPTION GRANTED BY USER admin",
                new GrantRoles(
                        ImmutableSet.of(new Identifier("role1")),
                        ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("user1"))),
                        true,
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("admin")))))));
        assertStatement("GRANT role1 TO ROLE role2 WITH ADMIN OPTION GRANTED BY ROLE admin",
                new GrantRoles(
                        ImmutableSet.of(new Identifier("role1")),
                        ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("role2"))),
                        true,
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("admin")))))));
        assertStatement("GRANT role1 TO ROLE role2 GRANTED BY ROLE admin",
                new GrantRoles(
                        ImmutableSet.of(new Identifier("role1")),
                        ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("role2"))),
                        false,
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("admin")))))));
        assertStatement("GRANT \"role1\" TO ROLE \"role2\" GRANTED BY ROLE \"admin\"",
                new GrantRoles(
                        ImmutableSet.of(new Identifier("role1")),
                        ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("role2"))),
                        false,
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("admin")))))));
    }

    @Test
    public void testRevokeRoles()
            throws Exception
    {
        assertStatement("REVOKE role1 FROM user1",
                new RevokeRoles(
                        ImmutableSet.of(new Identifier("role1")),
                        ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("user1"))),
                        false,
                        Optional.empty()));
        assertStatement("REVOKE ADMIN OPTION FOR role1, role2, role3 FROM user1, USER user2, ROLE role4",
                new RevokeRoles(
                        ImmutableSet.of(new Identifier("role1"), new Identifier("role2"), new Identifier("role3")),
                        ImmutableSet.of(
                                new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("user1")),
                                new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("user2")),
                                new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("role4"))),
                        true,
                        Optional.empty()));
        assertStatement("REVOKE ADMIN OPTION FOR role1 FROM user1 GRANTED BY admin",
                new RevokeRoles(
                        ImmutableSet.of(new Identifier("role1")),
                        ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("user1"))),
                        true,
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("admin")))))));
        assertStatement("REVOKE ADMIN OPTION FOR role1 FROM USER user1 GRANTED BY USER admin",
                new RevokeRoles(
                        ImmutableSet.of(new Identifier("role1")),
                        ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("user1"))),
                        true,
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("admin")))))));
        assertStatement("REVOKE role1 FROM ROLE role2 GRANTED BY ROLE admin",
                new RevokeRoles(
                        ImmutableSet.of(new Identifier("role1")),
                        ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("role2"))),
                        false,
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("admin")))))));
        assertStatement("REVOKE \"role1\" FROM ROLE \"role2\" GRANTED BY ROLE \"admin\"",
                new RevokeRoles(
                        ImmutableSet.of(new Identifier("role1")),
                        ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("role2"))),
                        false,
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("admin")))))));
    }

    @Test
    public void testSetRole()
            throws Exception
    {
        assertStatement("SET ROLE ALL", new SetRole(SetRole.Type.ALL, Optional.empty()));
        assertStatement("SET ROLE NONE", new SetRole(SetRole.Type.NONE, Optional.empty()));
        assertStatement("SET ROLE role", new SetRole(SetRole.Type.ROLE, Optional.of(new Identifier("role"))));
        assertStatement("SET ROLE \"role\"", new SetRole(SetRole.Type.ROLE, Optional.of(new Identifier("role"))));
    }

    @Test
    public void testNullTreatment()
    {
        assertExpression("lead(x, 1) ignore nulls over()",
                new FunctionCall(
                        QualifiedName.of("lead"),
                        Optional.of(new Window(ImmutableList.of(), Optional.empty(), Optional.empty())),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        true,
                        ImmutableList.of(new Identifier("x"), new LongLiteral("1"))));
        assertExpression("lead(x, 1) respect nulls over()",
                new FunctionCall(
                        QualifiedName.of("lead"),
                        Optional.of(new Window(ImmutableList.of(), Optional.empty(), Optional.empty())),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        false,
                        ImmutableList.of(new Identifier("x"), new LongLiteral("1"))));
    }

    @Test
    public void testUse()
    {
        assertStatement("Use test_schema", new Use(Optional.empty(), identifier("test_schema")));
        assertStatement("Use test_catalog.test_schema", new Use(Optional.of(identifier("test_catalog")), new Identifier("test_schema")));

        assertStatement("Use \"test_schema\"", new Use(Optional.empty(), quotedIdentifier("test_schema")));
        assertStatement("Use \"test_catalog\".\"test_schema\"", new Use(Optional.of(quotedIdentifier("test_catalog")), quotedIdentifier("test_schema")));
    }

    @Test
    public void testDropConstraint()
    {
        assertStatement("ALTER TABLE foo.t DROP CONSTRAINT cons", new DropConstraint(QualifiedName.of("foo", "t"), "cons", false, false));
        assertStatement("ALTER TABLE \"t x\" DROP CONSTRAINT \"c d\"", new DropConstraint(QualifiedName.of("t x"), quotedIdentifier("c d").toString(), false, false));
        assertStatement("ALTER TABLE IF EXISTS foo.t DROP CONSTRAINT cons", new DropConstraint(QualifiedName.of("foo", "t"), "cons", true, false));
        assertStatement("ALTER TABLE foo.t DROP CONSTRAINT IF EXISTS cons", new DropConstraint(QualifiedName.of("foo", "t"), "cons", false, true));
        assertStatement("ALTER TABLE IF EXISTS foo.t DROP CONSTRAINT IF EXISTS cons", new DropConstraint(QualifiedName.of("foo", "t"), "cons", true, true));
    }

    @Test
    public void testAlterTableAddConstraint()
    {
        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2)", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, true, true, true)));
        assertStatement("ALTER TABLE IF EXISTS foo.t ADD CONSTRAINT cons primary key (c1, c2)", new AddConstraint(QualifiedName.of("foo", "t"), true, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, true, true, true)));
        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons UNIQUE (c1, c2)", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), UNIQUE, true, true, true)));
        assertStatement("ALTER TABLE IF EXISTS foo.t ADD CONSTRAINT cons UNIQUE (c1, c2)", new AddConstraint(QualifiedName.of("foo", "t"), true, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), UNIQUE, true, true, true)));

        // Test all combinations of enabled/rely/enforced
        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) ENABLED", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, true, true, true)));
        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) RELY", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, true, true, true)));
        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) NOT ENFORCED", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, true, true, false)));

        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) DISABLED", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, false, true, true)));
        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) NOT RELY", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, true, false, true)));
        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) ENFORCED", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, true, true, true)));

        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) DISABLED ENFORCED", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, false, true, true)));
        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) DISABLED NOT ENFORCED", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, false, true, false)));
        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) ENABLED ENFORCED", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, true, true, true)));
        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) NOT ENFORCED ENABLED", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, true, true, false)));

        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) RELY ENFORCED", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, true, true, true)));
        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) RELY NOT ENFORCED", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, true, true, false)));
        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) NOT RELY ENFORCED", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, true, false, true)));
        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) NOT RELY NOT ENFORCED", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, true, false, false)));

        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) ENABLED RELY", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, true, true, true)));
        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) ENABLED NOT RELY", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, true, false, true)));
        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) DISABLED RELY", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, false, true, true)));
        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) DISABLED NOT RELY", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, false, false, true)));

        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) ENABLED RELY ENFORCED", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, true, true, true)));
        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) ENABLED NOT RELY ENFORCED", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, true, false, true)));
        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) DISABLED RELY ENFORCED", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, false, true, true)));
        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) DISABLED NOT RELY ENFORCED", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, false, false, true)));

        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) ENABLED RELY NOT ENFORCED", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, true, true, false)));
        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) ENABLED NOT ENFORCED NOT RELY", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, true, false, false)));
        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) DISABLED RELY NOT ENFORCED", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, false, true, false)));
        assertStatement("ALTER TABLE foo.t ADD CONSTRAINT cons PRIMARY KEY (c1, c2) DISABLED NOT RELY NOT ENFORCED", new AddConstraint(QualifiedName.of("foo", "t"), false, new ConstraintSpecification(Optional.of("cons"), ImmutableList.of("c1", "c2"), PRIMARY_KEY, false, false, false)));

        // Negative tests
        assertInvalidStatement("ALTER TABLE foo.t ADD PRIMARY KEY", ".*mismatched input.*");
        assertInvalidStatement("ALTER TABLE foo.t ADD CONSTRAINT uq UNIQUE", ".*mismatched input.*");
        assertInvalidStatement("ALTER TABLE foo.t ADD CONSTRAINT pk PRIMARY KEY (c1, c2), ADD CONSTRAINT uq UNIQUE (c3)", ".*mismatched input.*");
        assertInvalidStatement("ALTER TABLE foo.t ADD PRIMARY KEY (c1) NOT ENFORCED ENFORCED RELY", ".*Invalid.*constraint specification.*");
        assertInvalidStatement("ALTER TABLE foo.t ADD PRIMARY KEY (c1) RELY ENABLED NOT ENFORCED NOT RELY", ".*Invalid.*constraint specification.*");
    }

    @Test
    public void testCreateTableWithConstraints()
    {
        assertStatement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c DOUBLE, CONSTRAINT pk PRIMARY KEY (c,b))",
                new CreateTable(QualifiedName.of("foo"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("a"), "VARCHAR", true, emptyList(), Optional.empty()),
                                new ColumnDefinition(identifier("b"), "BIGINT", true, emptyList(), Optional.of("hello world")),
                                new ColumnDefinition(identifier("c"), "DOUBLE", true, emptyList(), Optional.empty()),
                                new ConstraintSpecification(Optional.of("pk"), ImmutableList.of("c", "b"), PRIMARY_KEY, true, true, true)),
                        false,
                        ImmutableList.of(),
                        Optional.empty()));

        assertStatement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c DOUBLE, PRIMARY KEY (c,b))",
                new CreateTable(QualifiedName.of("foo"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("a"), "VARCHAR", true, emptyList(), Optional.empty()),
                                new ColumnDefinition(identifier("b"), "BIGINT", true, emptyList(), Optional.of("hello world")),
                                new ColumnDefinition(identifier("c"), "DOUBLE", true, emptyList(), Optional.empty()),
                                new ConstraintSpecification(Optional.empty(), ImmutableList.of("c", "b"), PRIMARY_KEY, true, true, true)),
                        false,
                        ImmutableList.of(),
                        Optional.empty()));

        assertStatement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c DOUBLE, CONSTRAINT uq UNIQUE (c,b))",
                new CreateTable(QualifiedName.of("foo"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("a"), "VARCHAR", true, emptyList(), Optional.empty()),
                                new ColumnDefinition(identifier("b"), "BIGINT", true, emptyList(), Optional.of("hello world")),
                                new ColumnDefinition(identifier("c"), "DOUBLE", true, emptyList(), Optional.empty()),
                                new ConstraintSpecification(Optional.of("uq"), ImmutableList.of("c", "b"), UNIQUE, true, true, true)),
                        false,
                        ImmutableList.of(),
                        Optional.empty()));

        assertStatement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c DOUBLE, UNIQUE (c,b))",
                new CreateTable(QualifiedName.of("foo"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("a"), "VARCHAR", true, emptyList(), Optional.empty()),
                                new ColumnDefinition(identifier("b"), "BIGINT", true, emptyList(), Optional.of("hello world")),
                                new ColumnDefinition(identifier("c"), "DOUBLE", true, emptyList(), Optional.empty()),
                                new ConstraintSpecification(Optional.empty(), ImmutableList.of("c", "b"), UNIQUE, true, true, true)),
                        false,
                        ImmutableList.of(),
                        Optional.empty()));

        // test constrainnt qualifiers
        assertStatement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c DOUBLE, CONSTRAINT uq UNIQUE (c,b) ENABLED RELY ENFORCED)",
                new CreateTable(QualifiedName.of("foo"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("a"), "VARCHAR", true, emptyList(), Optional.empty()),
                                new ColumnDefinition(identifier("b"), "BIGINT", true, emptyList(), Optional.of("hello world")),
                                new ColumnDefinition(identifier("c"), "DOUBLE", true, emptyList(), Optional.empty()),
                                new ConstraintSpecification(Optional.of("uq"), ImmutableList.of("c", "b"), UNIQUE, true, true, true)),
                        false,
                        ImmutableList.of(),
                        Optional.empty()));

        assertStatement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c DOUBLE, CONSTRAINT uq UNIQUE (c,b) ENABLED NOT RELY ENFORCED)",
                new CreateTable(QualifiedName.of("foo"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("a"), "VARCHAR", true, emptyList(), Optional.empty()),
                                new ColumnDefinition(identifier("b"), "BIGINT", true, emptyList(), Optional.of("hello world")),
                                new ColumnDefinition(identifier("c"), "DOUBLE", true, emptyList(), Optional.empty()),
                                new ConstraintSpecification(Optional.of("uq"), ImmutableList.of("c", "b"), UNIQUE, true, false, true)),
                        false,
                        ImmutableList.of(),
                        Optional.empty()));

        assertStatement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c DOUBLE, CONSTRAINT uq UNIQUE (c,b) RELY ENFORCED DISABLED)",
                new CreateTable(QualifiedName.of("foo"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("a"), "VARCHAR", true, emptyList(), Optional.empty()),
                                new ColumnDefinition(identifier("b"), "BIGINT", true, emptyList(), Optional.of("hello world")),
                                new ColumnDefinition(identifier("c"), "DOUBLE", true, emptyList(), Optional.empty()),
                                new ConstraintSpecification(Optional.of("uq"), ImmutableList.of("c", "b"), UNIQUE, false, true, true)),
                        false,
                        ImmutableList.of(),
                        Optional.empty()));

        assertStatement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c DOUBLE, CONSTRAINT uq UNIQUE (c,b) DISABLED NOT RELY ENFORCED)",
                new CreateTable(QualifiedName.of("foo"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("a"), "VARCHAR", true, emptyList(), Optional.empty()),
                                new ColumnDefinition(identifier("b"), "BIGINT", true, emptyList(), Optional.of("hello world")),
                                new ColumnDefinition(identifier("c"), "DOUBLE", true, emptyList(), Optional.empty()),
                                new ConstraintSpecification(Optional.of("uq"), ImmutableList.of("c", "b"), UNIQUE, false, false, true)),
                        false,
                        ImmutableList.of(),
                        Optional.empty()));

        assertStatement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c DOUBLE, CONSTRAINT uq UNIQUE (c,b) ENABLED RELY NOT ENFORCED)",
                new CreateTable(QualifiedName.of("foo"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("a"), "VARCHAR", true, emptyList(), Optional.empty()),
                                new ColumnDefinition(identifier("b"), "BIGINT", true, emptyList(), Optional.of("hello world")),
                                new ColumnDefinition(identifier("c"), "DOUBLE", true, emptyList(), Optional.empty()),
                                new ConstraintSpecification(Optional.of("uq"), ImmutableList.of("c", "b"), UNIQUE, true, true, false)),
                        false,
                        ImmutableList.of(),
                        Optional.empty()));

        assertStatement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c DOUBLE, CONSTRAINT pk PRIMARY KEY (c,b) ENABLED NOT RELY ENFORCED)",
                new CreateTable(QualifiedName.of("foo"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("a"), "VARCHAR", true, emptyList(), Optional.empty()),
                                new ColumnDefinition(identifier("b"), "BIGINT", true, emptyList(), Optional.of("hello world")),
                                new ColumnDefinition(identifier("c"), "DOUBLE", true, emptyList(), Optional.empty()),
                                new ConstraintSpecification(Optional.of("pk"), ImmutableList.of("c", "b"), PRIMARY_KEY, true, false, true)),
                        false,
                        ImmutableList.of(),
                        Optional.empty()));

        assertStatement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c DOUBLE, CONSTRAINT pk PRIMARY KEY (c,b) DISABLED RELY NOT ENFORCED)",
                new CreateTable(QualifiedName.of("foo"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("a"), "VARCHAR", true, emptyList(), Optional.empty()),
                                new ColumnDefinition(identifier("b"), "BIGINT", true, emptyList(), Optional.of("hello world")),
                                new ColumnDefinition(identifier("c"), "DOUBLE", true, emptyList(), Optional.empty()),
                                new ConstraintSpecification(Optional.of("pk"), ImmutableList.of("c", "b"), PRIMARY_KEY, false, true, false)),
                        false,
                        ImmutableList.of(),
                        Optional.empty()));

        assertStatement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c DOUBLE, CONSTRAINT pk PRIMARY KEY (c,b) DISABLED NOT RELY NOT ENFORCED)",
                new CreateTable(QualifiedName.of("foo"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("a"), "VARCHAR", true, emptyList(), Optional.empty()),
                                new ColumnDefinition(identifier("b"), "BIGINT", true, emptyList(), Optional.of("hello world")),
                                new ColumnDefinition(identifier("c"), "DOUBLE", true, emptyList(), Optional.empty()),
                                new ConstraintSpecification(Optional.of("pk"), ImmutableList.of("c", "b"), PRIMARY_KEY, false, false, false)),
                        false,
                        ImmutableList.of(),
                        Optional.empty()));

        assertStatement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c DOUBLE, UNIQUE (c,b), PRIMARY KEY (a))",
                new CreateTable(QualifiedName.of("foo"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("a"), "VARCHAR", true, emptyList(), Optional.empty()),
                                new ColumnDefinition(identifier("b"), "BIGINT", true, emptyList(), Optional.of("hello world")),
                                new ColumnDefinition(identifier("c"), "DOUBLE", true, emptyList(), Optional.empty()),
                                new ConstraintSpecification(Optional.empty(), ImmutableList.of("c", "b"), UNIQUE, true, true, true),
                                new ConstraintSpecification(Optional.empty(), ImmutableList.of("a"), PRIMARY_KEY, true, true, true)),
                        false,
                        ImmutableList.of(),
                        Optional.empty()));

        assertStatement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c DOUBLE, CONSTRAINT uq UNIQUE (c,b), CONSTRAINT pk PRIMARY KEY (a))",
                new CreateTable(QualifiedName.of("foo"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("a"), "VARCHAR", true, emptyList(), Optional.empty()),
                                new ColumnDefinition(identifier("b"), "BIGINT", true, emptyList(), Optional.of("hello world")),
                                new ColumnDefinition(identifier("c"), "DOUBLE", true, emptyList(), Optional.empty()),
                                new ConstraintSpecification(Optional.of("uq"), ImmutableList.of("c", "b"), UNIQUE, true, true, true),
                                new ConstraintSpecification(Optional.of("pk"), ImmutableList.of("a"), PRIMARY_KEY, true, true, true)),
                        false,
                        ImmutableList.of(),
                        Optional.empty()));

        assertStatement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c DOUBLE, CONSTRAINT uq UNIQUE (c,b), PRIMARY KEY (a))",
                new CreateTable(QualifiedName.of("foo"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("a"), "VARCHAR", true, emptyList(), Optional.empty()),
                                new ColumnDefinition(identifier("b"), "BIGINT", true, emptyList(), Optional.of("hello world")),
                                new ColumnDefinition(identifier("c"), "DOUBLE", true, emptyList(), Optional.empty()),
                                new ConstraintSpecification(Optional.of("uq"), ImmutableList.of("c", "b"), UNIQUE, true, true, true),
                                new ConstraintSpecification(Optional.empty(), ImmutableList.of("a"), PRIMARY_KEY, true, true, true)),
                        false,
                        ImmutableList.of(),
                        Optional.empty()));

        assertStatement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c DOUBLE, UNIQUE (c), PRIMARY KEY (a), CONSTRAINT uq UNIQUE (b))",
                new CreateTable(QualifiedName.of("foo"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("a"), "VARCHAR", true, emptyList(), Optional.empty()),
                                new ColumnDefinition(identifier("b"), "BIGINT", true, emptyList(), Optional.of("hello world")),
                                new ColumnDefinition(identifier("c"), "DOUBLE", true, emptyList(), Optional.empty()),
                                new ConstraintSpecification(Optional.empty(), ImmutableList.of("c"), UNIQUE, true, true, true),
                                new ConstraintSpecification(Optional.empty(), ImmutableList.of("a"), PRIMARY_KEY, true, true, true),
                                new ConstraintSpecification(Optional.of("uq"), ImmutableList.of("b"), UNIQUE, true, true, true)),
                        false,
                        ImmutableList.of(),
                        Optional.empty()));

        // The following two cases are illegal since they have duplicate constraints. But they pass the parser and error handling happens later
        assertStatement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c DOUBLE, CONSTRAINT uq UNIQUE (c), PRIMARY KEY (a), CONSTRAINT uq UNIQUE (b))",
                new CreateTable(QualifiedName.of("foo"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("a"), "VARCHAR", true, emptyList(), Optional.empty()),
                                new ColumnDefinition(identifier("b"), "BIGINT", true, emptyList(), Optional.of("hello world")),
                                new ColumnDefinition(identifier("c"), "DOUBLE", true, emptyList(), Optional.empty()),
                                new ConstraintSpecification(Optional.of("uq"), ImmutableList.of("c"), UNIQUE, true, true, true),
                                new ConstraintSpecification(Optional.empty(), ImmutableList.of("a"), PRIMARY_KEY, true, true, true),
                                new ConstraintSpecification(Optional.of("uq"), ImmutableList.of("b"), UNIQUE, true, true, true)),
                        false,
                        ImmutableList.of(),
                        Optional.empty()));

        assertStatement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c DOUBLE, CONSTRAINT uq UNIQUE (c), PRIMARY KEY (a), CONSTRAINT uq UNIQUE (b), CONSTRAINT pk PRIMARY KEY (b), CONSTRAINT pk PRIMARY KEY (c))",
                new CreateTable(QualifiedName.of("foo"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("a"), "VARCHAR", true, emptyList(), Optional.empty()),
                                new ColumnDefinition(identifier("b"), "BIGINT", true, emptyList(), Optional.of("hello world")),
                                new ColumnDefinition(identifier("c"), "DOUBLE", true, emptyList(), Optional.empty()),
                                new ConstraintSpecification(Optional.of("uq"), ImmutableList.of("c"), UNIQUE, true, true, true),
                                new ConstraintSpecification(Optional.empty(), ImmutableList.of("a"), PRIMARY_KEY, true, true, true),
                                new ConstraintSpecification(Optional.of("uq"), ImmutableList.of("b"), UNIQUE, true, true, true),
                                new ConstraintSpecification(Optional.of("pk"), ImmutableList.of("b"), PRIMARY_KEY, true, true, true),
                                new ConstraintSpecification(Optional.of("pk"), ImmutableList.of("c"), PRIMARY_KEY, true, true, true)),
                        false,
                        ImmutableList.of(),
                        Optional.empty()));

        // Since PRIMARY is not a reserved word, this is parsed as a column of type KEY
        assertStatement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c DOUBLE, PRIMARY KEY)",
                new CreateTable(QualifiedName.of("foo"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("a"), "VARCHAR", true, emptyList(), Optional.empty()),
                                new ColumnDefinition(identifier("b"), "BIGINT", true, emptyList(), Optional.of("hello world")),
                                new ColumnDefinition(identifier("c"), "DOUBLE", true, emptyList(), Optional.empty()),
                                new ColumnDefinition(identifier("PRIMARY"), "KEY", true, emptyList(), Optional.empty())),
                        false,
                        ImmutableList.of(),
                        Optional.empty()));

        //Negative tests
        assertInvalidStatement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c DOUBLE, UNIQUE (c), UNIQUE)", ".*mismatched input.*");
        assertInvalidStatement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c DOUBLE, UNIQUE (c), UNIQUE (a) ENFORCED DISABLED ENFORCED)", ".*Invalid.*constraint specification.*");
        assertInvalidStatement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c DOUBLE, UNIQUE (c), UNIQUE (a) RELY DISABLED NOT RELY)", ".*Invalid.*constraint specification.*");
    }

    @Test
    public void testAlterColumnAddDropNotNullConstraints()
    {
        assertStatement("ALTER TABLE foo.t ALTER COLUMN b SET NOT NULL", new AlterColumnNotNull(QualifiedName.of("foo", "t"),
                identifier("b"), false, false));
        assertStatement("ALTER TABLE foo.t ALTER COLUMN b DROP NOT NULL", new AlterColumnNotNull(QualifiedName.of("foo", "t"),
                identifier("b"), false, true));
        assertStatement("ALTER TABLE IF EXISTS foo.t ALTER COLUMN b SET NOT NULL", new AlterColumnNotNull(QualifiedName.of("foo", "t"),
                identifier("b"), true, false));
        assertStatement("ALTER TABLE foo.t ALTER b DROP NOT NULL", new AlterColumnNotNull(QualifiedName.of("foo", "t"),
                identifier("b"), false, true));
        assertStatement("ALTER TABLE IF EXISTS foo.t ALTER b DROP NOT NULL", new AlterColumnNotNull(QualifiedName.of("foo", "t"),
                identifier("b"), true, true));
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
            fail(format("expected%n%n%s%n%nto parse as%n%n%s%n%nbut was%n%n%s%n",
                    indent(input),
                    indent(formatSql(expected, Optional.empty())),
                    indent(formatSql(parsed, Optional.empty()))));
        }
    }

    private static void assertInvalidStatement(String expression, String expectedErrorMessageRegex)
    {
        try {
            Statement result = SQL_PARSER.createStatement(expression, ParsingOptions.builder().build());
            fail("Expected to throw ParsingException for input:[" + expression + "], but got: " + result);
        }
        catch (ParsingException e) {
            if (!e.getErrorMessage().matches(expectedErrorMessageRegex)) {
                fail(format("Expected error message to match '%s', but was: '%s'", expectedErrorMessageRegex, e.getErrorMessage()));
            }
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

    @Test
    public void testSelectWithAsOfVersion()
    {
        assertStatement("SELECT * FROM table1 FOR VERSION AS OF 8772871542276440693",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(new NodeLocation(1, 15), QualifiedName.of("table1"),
                                new TableVersionExpression(VERSION, TableVersionOperator.EQUAL, new LongLiteral("8772871542276440693"))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 FOR VERSION AS OF 'branch-name'",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(new NodeLocation(1, 15), QualifiedName.of("table1"),
                                new TableVersionExpression(VERSION, TableVersionOperator.EQUAL, new StringLiteral("branch-name"))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 FOR SYSTEM_VERSION AS OF 8772871542276440693",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(new NodeLocation(1, 15), QualifiedName.of("table1"),
                                new TableVersionExpression(VERSION, TableVersionOperator.EQUAL, new LongLiteral("8772871542276440693"))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 FOR VERSION AS OF 8772871542276440693 WHERE (c1 = 100)",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(new NodeLocation(1, 15), QualifiedName.of("table1"),
                                new TableVersionExpression(VERSION, TableVersionOperator.EQUAL, new LongLiteral("8772871542276440693"))),
                        Optional.of(new ComparisonExpression(EQUAL, new Identifier("c1"), new LongLiteral("100"))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 FOR VERSION AS OF 'branch-name' WHERE (c1 = 100)",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(new NodeLocation(1, 15), QualifiedName.of("table1"),
                                new TableVersionExpression(VERSION, TableVersionOperator.EQUAL, new StringLiteral("branch-name"))),
                        Optional.of(new ComparisonExpression(EQUAL, new Identifier("c1"), new LongLiteral("100"))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 FOR VERSION AS OF 8772871542276440693, table2 FOR VERSION AS OF 123456789012345",
                simpleQuery(selectList(new AllColumns()),
                        new Join(Join.Type.IMPLICIT,
                                new Table(new NodeLocation(1, 15), QualifiedName.of("table1"),
                                        new TableVersionExpression(VERSION, TableVersionOperator.EQUAL, new LongLiteral("8772871542276440693"))),
                                new Table(new NodeLocation(1, 60), QualifiedName.of("table2"),
                                        new TableVersionExpression(VERSION, TableVersionOperator.EQUAL, new LongLiteral("123456789012345"))),
                                Optional.empty())));

        assertStatement("SELECT * FROM table1 FOR VERSION AS OF 8772871542276440693, table2 FOR TIMESTAMP AS OF TIMESTAMP '2023-08-17 13:29:46.822 America/Los_Angeles' ",
                simpleQuery(selectList(new AllColumns()),
                        new Join(Join.Type.IMPLICIT,
                                new Table(new NodeLocation(1, 15), QualifiedName.of("table1"),
                                        new TableVersionExpression(VERSION, TableVersionOperator.EQUAL, new LongLiteral("8772871542276440693"))),
                                new Table(new NodeLocation(1, 60), QualifiedName.of("table2"),
                                        new TableVersionExpression(TIMESTAMP, TableVersionOperator.EQUAL, new TimestampLiteral("2023-08-17 13:29:46.822 America/Los_Angeles"))),
                                Optional.empty())));

        Query query = simpleQuery(selectList(new AllColumns()), new Table(new NodeLocation(1, 35), QualifiedName.of("table1"),
                        new TableVersionExpression(VERSION, TableVersionOperator.EQUAL, new LongLiteral("8772871542276440693"))));

        assertStatement("CREATE VIEW view1 AS SELECT * FROM table1 FOR VERSION AS OF 8772871542276440693",
                new CreateView(QualifiedName.of("view1"), query, false, Optional.empty()));
    }

    @Test
    public void testSelectWithAsOfTimestamp()
    {
        assertStatement("SELECT * FROM table1 FOR TIMESTAMP AS OF TIMESTAMP '2023-08-17 13:29:46.822 America/Los_Angeles' ",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(new NodeLocation(1, 15), QualifiedName.of("table1"),
                                new TableVersionExpression(TIMESTAMP, TableVersionOperator.EQUAL, new TimestampLiteral("2023-08-17 13:29:46.822 America/Los_Angeles"))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 FOR SYSTEM_TIME AS OF TIMESTAMP '2023-08-17 13:29:46.822 America/Los_Angeles' ",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(new NodeLocation(1, 15), QualifiedName.of("table1"),
                                new TableVersionExpression(TIMESTAMP, TableVersionOperator.EQUAL, new TimestampLiteral("2023-08-17 13:29:46.822 America/Los_Angeles"))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 FOR TIMESTAMP AS OF TIMESTAMP '2023-08-17 13:29:46.822 America/Los_Angeles' WHERE (c1 > 100)",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(new NodeLocation(1, 15), QualifiedName.of("table1"),
                                new TableVersionExpression(TIMESTAMP, TableVersionOperator.EQUAL, new TimestampLiteral("2023-08-17 13:29:46.822 America/Los_Angeles"))),
                        Optional.of(new ComparisonExpression(GREATER_THAN, new Identifier("c1"), new LongLiteral("100"))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 FOR TIMESTAMP AS OF CURRENT_TIMESTAMP",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(new NodeLocation(1, 15), QualifiedName.of("table1"),
                                new TableVersionExpression(TIMESTAMP, TableVersionOperator.EQUAL, new CurrentTime(CurrentTime.Function.TIMESTAMP))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 FOR TIMESTAMP AS OF TIMESTAMP '2023-08-17 13:29:46.822 America/Los_Angeles', table2 FOR TIMESTAMP AS OF TIMESTAMP '2023-11-01 05:45:25.123 America/Los_Angeles'",
                simpleQuery(selectList(new AllColumns()),
                        new Join(Join.Type.IMPLICIT,
                                new Table(new NodeLocation(1, 15), QualifiedName.of("table1"),
                                        new TableVersionExpression(TIMESTAMP, TableVersionOperator.EQUAL, new TimestampLiteral("2023-08-17 13:29:46.822 America/Los_Angeles"))),
                                new Table(new NodeLocation(1, 98), QualifiedName.of("table2"),
                                        new TableVersionExpression(TIMESTAMP, TableVersionOperator.EQUAL, new TimestampLiteral("2023-11-01 05:45:25.123 America/Los_Angeles"))),
                                Optional.empty())));

        assertStatement("SELECT * FROM table1 FOR TIMESTAMP AS OF TIMESTAMP '2023-08-17 13:29:46.822 America/Los_Angeles', table2 FOR VERSION AS OF 8772871542276440693",
                simpleQuery(selectList(new AllColumns()),
                        new Join(Join.Type.IMPLICIT,
                                new Table(new NodeLocation(1, 15), QualifiedName.of("table1"),
                                        new TableVersionExpression(TIMESTAMP, TableVersionOperator.EQUAL, new TimestampLiteral("2023-08-17 13:29:46.822 America/Los_Angeles"))),
                                new Table(new NodeLocation(1, 98), QualifiedName.of("table2"),
                                        new TableVersionExpression(VERSION, TableVersionOperator.EQUAL, new LongLiteral("8772871542276440693"))),
                                Optional.empty())));

        Query query = simpleQuery(selectList(new AllColumns()), new Table(new NodeLocation(1, 35), QualifiedName.of("table1"),
                        new TableVersionExpression(TIMESTAMP, TableVersionOperator.EQUAL, new TimestampLiteral("2023-08-17 13:29:46.822 America/Los_Angeles"))));

        assertStatement("CREATE VIEW view1 AS SELECT * FROM table1 FOR TIMESTAMP AS OF TIMESTAMP '2023-08-17 13:29:46.822 America/Los_Angeles'",
                new CreateView(QualifiedName.of("view1"), query, false, Optional.empty()));
    }

    @Test
    public void testSelectWithBeforeVersion()
    {
        assertStatement("SELECT * FROM table1 FOR VERSION BEFORE 8772871542276440693",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(new NodeLocation(1, 15), QualifiedName.of("table1"),
                                new TableVersionExpression(VERSION, TableVersionOperator.LESS_THAN, new LongLiteral("8772871542276440693"))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 FOR SYSTEM_VERSION BEFORE 8772871542276440693",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(new NodeLocation(1, 15), QualifiedName.of("table1"),
                                new TableVersionExpression(VERSION, TableVersionOperator.LESS_THAN, new LongLiteral("8772871542276440693"))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 FOR VERSION BEFORE 8772871542276440693 WHERE (c1 = 100)",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(new NodeLocation(1, 15), QualifiedName.of("table1"),
                                new TableVersionExpression(VERSION, TableVersionOperator.LESS_THAN, new LongLiteral("8772871542276440693"))),
                        Optional.of(new ComparisonExpression(EQUAL, new Identifier("c1"), new LongLiteral("100"))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 FOR VERSION BEFORE 8772871542276440693, table2 FOR VERSION BEFORE 123456789012345",
                simpleQuery(selectList(new AllColumns()),
                        new Join(Join.Type.IMPLICIT,
                                new Table(new NodeLocation(1, 15), QualifiedName.of("table1"),
                                        new TableVersionExpression(VERSION, TableVersionOperator.LESS_THAN, new LongLiteral("8772871542276440693"))),
                                new Table(new NodeLocation(1, 60), QualifiedName.of("table2"),
                                        new TableVersionExpression(VERSION, TableVersionOperator.LESS_THAN, new LongLiteral("123456789012345"))),
                                Optional.empty())));

        assertStatement("SELECT * FROM table1 FOR VERSION BEFORE 8772871542276440693, table2 FOR TIMESTAMP BEFORE TIMESTAMP '2023-08-17 13:29:46.822 America/Los_Angeles' ",
                simpleQuery(selectList(new AllColumns()),
                        new Join(Join.Type.IMPLICIT,
                                new Table(new NodeLocation(1, 15), QualifiedName.of("table1"),
                                        new TableVersionExpression(VERSION, TableVersionOperator.LESS_THAN, new LongLiteral("8772871542276440693"))),
                                new Table(new NodeLocation(1, 60), QualifiedName.of("table2"),
                                        new TableVersionExpression(TIMESTAMP, TableVersionOperator.LESS_THAN, new TimestampLiteral("2023-08-17 13:29:46.822 America/Los_Angeles"))),
                                Optional.empty())));

        Query query = simpleQuery(selectList(new AllColumns()), new Table(new NodeLocation(1, 35), QualifiedName.of("table1"),
                new TableVersionExpression(VERSION, TableVersionOperator.LESS_THAN, new LongLiteral("8772871542276440693"))));

        assertStatement("CREATE VIEW view1 AS SELECT * FROM table1 FOR VERSION BEFORE 8772871542276440693",
                new CreateView(QualifiedName.of("view1"), query, false, Optional.empty()));
    }

    @Test
    public void testSelectWithBeforeTimestamp()
    {
        assertStatement("SELECT * FROM table1 FOR TIMESTAMP BEFORE TIMESTAMP '2023-08-17 13:29:46.822 America/Los_Angeles' ",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(new NodeLocation(1, 15), QualifiedName.of("table1"),
                                new TableVersionExpression(TIMESTAMP, TableVersionOperator.LESS_THAN, new TimestampLiteral("2023-08-17 13:29:46.822 America/Los_Angeles"))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 FOR SYSTEM_TIME BEFORE TIMESTAMP '2023-08-17 13:29:46.822 America/Los_Angeles' ",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(new NodeLocation(1, 15), QualifiedName.of("table1"),
                                new TableVersionExpression(TIMESTAMP, TableVersionOperator.LESS_THAN, new TimestampLiteral("2023-08-17 13:29:46.822 America/Los_Angeles"))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 FOR TIMESTAMP BEFORE TIMESTAMP '2023-08-17 13:29:46.822 America/Los_Angeles' WHERE (c1 > 100)",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(new NodeLocation(1, 15), QualifiedName.of("table1"),
                                new TableVersionExpression(TIMESTAMP, TableVersionOperator.LESS_THAN, new TimestampLiteral("2023-08-17 13:29:46.822 America/Los_Angeles"))),
                        Optional.of(new ComparisonExpression(GREATER_THAN, new Identifier("c1"), new LongLiteral("100"))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 FOR TIMESTAMP BEFORE CURRENT_TIMESTAMP",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(new NodeLocation(1, 15), QualifiedName.of("table1"),
                                new TableVersionExpression(TIMESTAMP, TableVersionOperator.LESS_THAN, new CurrentTime(CurrentTime.Function.TIMESTAMP))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 FOR TIMESTAMP BEFORE TIMESTAMP '2023-08-17 13:29:46.822 America/Los_Angeles', table2 FOR TIMESTAMP BEFORE TIMESTAMP '2023-11-01 05:45:25.123 America/Los_Angeles'",
                simpleQuery(selectList(new AllColumns()),
                        new Join(Join.Type.IMPLICIT,
                                new Table(new NodeLocation(1, 15), QualifiedName.of("table1"),
                                        new TableVersionExpression(TIMESTAMP, TableVersionOperator.LESS_THAN, new TimestampLiteral("2023-08-17 13:29:46.822 America/Los_Angeles"))),
                                new Table(new NodeLocation(1, 98), QualifiedName.of("table2"),
                                        new TableVersionExpression(TIMESTAMP, TableVersionOperator.LESS_THAN, new TimestampLiteral("2023-11-01 05:45:25.123 America/Los_Angeles"))),
                                Optional.empty())));

        assertStatement("SELECT * FROM table1 FOR TIMESTAMP BEFORE TIMESTAMP '2023-08-17 13:29:46.822 America/Los_Angeles', table2 FOR VERSION BEFORE 8772871542276440693",
                simpleQuery(selectList(new AllColumns()),
                        new Join(Join.Type.IMPLICIT,
                                new Table(new NodeLocation(1, 15), QualifiedName.of("table1"),
                                        new TableVersionExpression(TIMESTAMP, TableVersionOperator.LESS_THAN, new TimestampLiteral("2023-08-17 13:29:46.822 America/Los_Angeles"))),
                                new Table(new NodeLocation(1, 98), QualifiedName.of("table2"),
                                        new TableVersionExpression(VERSION, TableVersionOperator.LESS_THAN, new LongLiteral("8772871542276440693"))),
                                Optional.empty())));

        Query query = simpleQuery(selectList(new AllColumns()), new Table(new NodeLocation(1, 35), QualifiedName.of("table1"),
                new TableVersionExpression(TIMESTAMP, TableVersionOperator.LESS_THAN, new TimestampLiteral("2023-08-17 13:29:46.822 America/Los_Angeles"))));

        assertStatement("CREATE VIEW view1 AS SELECT * FROM table1 FOR TIMESTAMP BEFORE TIMESTAMP '2023-08-17 13:29:46.822 America/Los_Angeles'",
                new CreateView(QualifiedName.of("view1"), query, false, Optional.empty()));
    }
}
