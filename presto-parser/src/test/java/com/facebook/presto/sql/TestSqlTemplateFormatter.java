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

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Call;
import com.facebook.presto.sql.tree.CallArgument;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.sql.SqlFormatter.formatSqlAsTemplate;
import static org.testng.Assert.assertEquals;

public class TestSqlTemplateFormatter
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testSqlTemplateFormatterBinaryLiteral()
    {
        assertTemplate("SELECT * FROM table1 WHERE binField = X''", "SELECT * FROM table1 WHERE binField = ?");
    }

    @Test
    public void testSqlTemplateFormatterBooleanLiteral()
    {
        assertTemplate("SELECT * FROM table1 WHERE a = true AND b = false", "SELECT * FROM table1 WHERE a = ? AND b = ?");
    }

    @Test
    public void testSqlTemplateFormatterCharLiteral()
    {
        assertTemplate("SELECT * WHERE c = CHAR 'abc'", "SELECT * WHERE c = ?");
    }

    @Test
    public void testSqlTemplateFormatterDecimalLiteral()
    {
        Statement statement = new Call(
                QualifiedName.of("foo"),
                ImmutableList.of(
                        new CallArgument("a", new DecimalLiteral("5")),
                        new CallArgument("b", new DecimalLiteral("6"))));
        String templateStatement = "CALL foo(a => ?, b => ?)";
        assertTemplate(statement, templateStatement);
    }

    @Test
    public void testSqlTemplateFormatterDoubleLiteral()
    {
        Statement statement = new Call(
                QualifiedName.of("foo"),
                ImmutableList.of(
                        new CallArgument("a", new DoubleLiteral("5")),
                        new CallArgument("b", new DoubleLiteral("6"))));
        String templateStatement = "CALL foo(a => ?, b => ?)";
        assertTemplate(statement, templateStatement);
    }

    @Test
    public void testSqlTemplateFormatterGenericLiteral()
    {
        Statement statement = new Call(
                QualifiedName.of("foo"),
                ImmutableList.of(new CallArgument("a", new GenericLiteral("varchar", "test"))));
        String templateStatement = "CALL foo(a => ?)";
        assertTemplate(statement, templateStatement);
    }

    @Test
    public void testSqlTemplateFormatterIntervalLiteral()
    {
        assertTemplate("SELECT * FROM table1 where Date1 < Date2 + INTERVAL '33' day to second", "SELECT * FROM table1 where Date1 < Date2 + ?");
    }

    @Test
    public void testSqlTemplateFormatterNullLiteral()
    {
        assertTemplate("SELECT * FROM table1 WHERE null", "SELECT * FROM table1 WHERE ?");
    }

    @Test
    public void testSqlTemplateFormatterStringLiteral()
    {
        Statement statement = new Call(
                QualifiedName.of("foo"),
                ImmutableList.of(new CallArgument("b", new StringLiteral("go"))));
        String templateStatement = "CALL foo(b => ?)";
        assertTemplate(statement, templateStatement);
    }

    @Test
    public void testSqlTemplateFormatterTimeLiteral()
    {
        assertTemplate("SELECT * FROM table1 WHERE Time = '01:02:03.1234'", "SELECT * FROM table1 WHERE Time = ?");
    }

    @Test
    public void testSqlTemplateFormatterTimestampLiteral()
    {
        assertTemplate("SELECT * FROM table1 WHERE DateTime > '2022-01-01 01:02:03.1234'", "SELECT * FROM table1 WHERE DateTime > ?");
    }

    @Test
    public void testSqlTemplateFormatterWithParameters()
    {
        assertTemplate("SELECT * FROM table1 WHERE i = 1 and j = ?", "SELECT * FROM table1 WHERE i = ? and j = ?");

        Statement statement = new Call(
                QualifiedName.of("foo"),
                ImmutableList.of(
                        new CallArgument("a", new Parameter(0)),
                        new CallArgument("b", new DecimalLiteral("5")),
                        new CallArgument("c", new Parameter(1))));

        Statement templateStatement = new Call(
                QualifiedName.of("foo"),
                ImmutableList.of(
                        new CallArgument("a", new Parameter(0)),
                        new CallArgument("b", new Parameter(1)),
                        new CallArgument("c", new Parameter(2))));
        assertTemplate(statement, templateStatement);
    }

    @Test
    public void testSqlFormatterGroupBy()
    {
        assertTemplate("SELECT * FROM table1 GROUP BY 1", "SELECT * FROM table1 GROUP BY 1");

        assertTemplate("SELECT * FROM table1 GROUP BY 1", "SELECT * FROM table1 GROUP BY 1");

        String statement1 = "SELECT col1 , COUNT(*) FROM table1 GROUP BY col1";
        String templateStatement1 = "SELECT col1 , COUNT(*) FROM table1 GROUP BY col1";
        assertTemplate(statement1, templateStatement1);

        String statement2 = "SELECT col1 , COUNT(*) FROM table1 GROUP BY ('SCOTT', 'BLAKE', 'TAYLOR')";
        String templateStatement2 = "SELECT col1 , COUNT(*) FROM table1 GROUP BY (?, ?, ?)";
        assertTemplate(statement2, templateStatement2);
    }

    @Test
    public void testSqlFormatterLimit()
    {
        String statement1 = "SELECT * FROM table1 LIMIT 1";
        String statement2 = "SELECT * FROM table1 LIMIT 5";
        assertEquals(formatSqlAsTemplate(SQL_PARSER.createStatement(statement1)), formatSqlAsTemplate(SQL_PARSER.createStatement(statement2)));
    }

    @Test
    public void testSqlTemplateFormatterComplexStatement()
    {
        String statement1 = "SELECT table1.id, AVG(table2.price) FROM table1 INNER JOIN table2 ON table1.id = table2.id WHERE table2.state = 'CA' " +
                "AND table2.registered = true AND null GROUP BY table1.id HAVING AVG(price) > 20 AND registrationDate <= '2022-01-01 01:02:03.1234'";
        String templateStatement1 = "SELECT table1.id, AVG(table2.price) FROM table1 INNER JOIN table2 ON table1.id = table2.id WHERE table2.state = ? " +
                "AND table2.registered = ? AND ? GROUP BY table1.id HAVING AVG(price) > ? AND registrationDate <= ?";
        assertTemplate(statement1, templateStatement1);

        String statement2 = "SELECT id FROM table1 WHERE id > 78 INTERSECT SELECT id " +
                "FROM table2 WHERE quantity <> 0 HAVING COUNT(*) > 1 ORDER BY table2.timestamp ASC";
        String templateStatement2 = "SELECT id FROM table1 WHERE id > ? INTERSECT SELECT id " +
                "FROM table2 WHERE quantity <> ? HAVING COUNT(*) > ? ORDER BY table2.timestamp ASC";
        assertTemplate(statement2, templateStatement2);

        String statement3 = "WITH a (t, u) AS (SELECT * FROM x WHERE i = 1), b AS (SELECT * FROM y WHERE j = 'test') TABLE z";
        String templateStatement3 = "WITH a (t, u) AS (SELECT * FROM x WHERE i = ?), b AS (SELECT * FROM y WHERE j = ?) TABLE z";
        assertTemplate(statement3, templateStatement3);

        String statement4 = "SELECT t1.* FROM table1 t1 JOIN (SELECT DISTINCT t2.id FROM table2 t2 WHERE t2.email = 'xyz@test.com' ) t2 ON t1.id = t2.id";
        String templateStatement4 = "SELECT t1.* FROM table1 t1 JOIN (SELECT DISTINCT t2.id FROM table2 t2 WHERE t2.email = ? ) t2 ON t1.id = t2.id";
        assertTemplate(statement4, templateStatement4);

        String statement5 = "CREATE TABLE dest_table WITH (test_property = 90) AS SELECT * FROM test_table";
        String templateStatement5 = "CREATE TABLE dest_table WITH (test_property = ?) AS SELECT * FROM test_table";
        assertTemplate(statement5, templateStatement5);

        String statement6 = "SELECT SUM(x) FILTER (WHERE x > 6) GROUP BY 1";
        String templateStatement6 = "SELECT SUM(x) FILTER (WHERE x > ?) GROUP BY 1";
        assertTemplate(statement6, templateStatement6);

        String statement7 = "CREATE TABLE foo WITH (string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY['v1', 'v2']) AS SELECT * FROM t WITH NO DATA";
        String templateStatement7 = "CREATE TABLE foo WITH (string = ?, long = ?, computed = ? || ?, a  = ARRAY[?, ?]) AS SELECT * FROM t WITH NO DATA";
        assertTemplate(statement7, templateStatement7);
    }

    private static void assertTemplate(String statement, String templateStatement)
    {
        assertTemplate(SQL_PARSER.createStatement(statement), templateStatement);
    }

    private static void assertTemplate(Statement statement, String templateStatement)
    {
        assertTemplate(statement, SQL_PARSER.createStatement(templateStatement));
    }

    private static void assertTemplate(Statement statement, Statement templateStatement)
    {
        String formattedStatement = formatSqlAsTemplate(statement);
        String formattedTemplateStatement = formatSql(templateStatement, Optional.empty());
        assertEquals(formattedStatement, formattedTemplateStatement);
    }
}
