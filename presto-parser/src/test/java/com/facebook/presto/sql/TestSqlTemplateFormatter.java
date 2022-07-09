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

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Call;
import com.facebook.presto.sql.tree.CallArgument;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static org.testng.Assert.assertEquals;

public class TestSqlTemplateFormatter
{
    private static final SqlFormatter SQL_FORMATTER = new SqlFormatter(true);
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testSqlFormatterBinaryLiteral()
    {
        String statement = "SELECT * FROM table1 WHERE binField = X''";
        String templateStatement = "SELECT * FROM table1 WHERE binField = ?";
        assertTemplate(statement, templateStatement);
    }

    @Test
    public void testSqlFormatterBooleanLiteral()
    {
        String statement = "SELECT * FROM table1 WHERE a = true AND b = false";
        String templateStatement = "SELECT * FROM table1 WHERE a = ? AND b = ?";
        assertTemplate(statement, templateStatement);
    }

    @Test
    public void testSqlFormatterCharLiteral()
    {
        assertTemplate("SELECT * WHERE c = CHAR 'abc'", "SELECT * WHERE c = ?");
    }

    @Test
    public void testSqlFormatterDecimalLiteral()
    {
        String statement = "SELECT * FROM table1 WHERE a >= ALL (VALUES 2, 3, 4)";
        String templateStatement = "SELECT * FROM table1 WHERE a >= ALL (VALUES ?, ?, ?)";
        assertTemplateWithParsingOption(statement, templateStatement, new ParsingOptions(AS_DECIMAL));
    }

    @Test
    public void testSqlFormatterDoubleLiteral()
    {
        String statement = "SELECT * FROM table1 WHERE a < ALL (VALUES 2, 3, 4)";
        String templateStatement = "SELECT * FROM table1 WHERE a < ALL (VALUES ?, ?, ?)";
        assertTemplateWithParsingOption(statement, templateStatement, new ParsingOptions(AS_DOUBLE));
    }

    @Test
    public void testRewriteGenericLiteral()
    {
        assertTemplate(new Call(QualifiedName.of("foo"), ImmutableList.of(
                new CallArgument("a", new GenericLiteral("varchar", "test")))), "CALL foo(a => ?)");
    }

    @Test
    public void testSqlFormatterIntervalLiteral()
    {
        String statement = "SELECT * FROM table1 where Date1 < Date2 + INTERVAL '33' day to second";
        String templateStatement = "SELECT * FROM table1 where Date1 < Date2 + ?";
        assertTemplate(statement, templateStatement);
    }

    @Test
    public void testSqlFormatterNullLiteral()
    {
        assertTemplate("SELECT * FROM table1 WHERE null", "SELECT * FROM table1 WHERE ?");
    }

    @Test
    public void testSqlFormatterStringLiteral()
    {
        assertTemplate(new Call(QualifiedName.of("foo"), ImmutableList.of(
                new CallArgument("b", new StringLiteral("go")))), "CALL foo(b => ?)");
    }

    @Test
    public void testSqlFormatterTimeLiteral()
    {
        assertTemplate("SELECT * FROM table1 WHERE Time = '01:02:03.1234'", "SELECT * FROM table1 WHERE Time = ?");
    }

    @Test
    public void testSqlFormatterTimestampLiteral()
    {
        assertTemplate("SELECT * FROM table1 WHERE DateTime > '2022-01-01 01:02:03.1234'", "SELECT * FROM table1 WHERE DateTime > ?");
    }

    private static void assertTemplate(String statement, String templateStatement)
    {
        Statement parsedStatement = SQL_PARSER.createStatement(statement);
        Statement parsedTemplateStatement = SQL_PARSER.createStatement(templateStatement);
        assertFormatted(parsedStatement, parsedTemplateStatement);
    }

    private static void assertTemplate(Statement statement, String templateStatement)
    {
        Statement parsedTemplateStatement = SQL_PARSER.createStatement(templateStatement);
        assertFormatted(statement, parsedTemplateStatement);
    }

    private static void assertFormatted(Statement statement, Statement expectedStatement)
    {
        String formattedStatement = SQL_FORMATTER.formatSql(statement, Optional.empty());
        String formattedTemplateStatement = SQL_FORMATTER.formatSql(expectedStatement, Optional.empty());
        assertEquals(formattedStatement, formattedTemplateStatement);
    }

    private static void assertTemplateWithParsingOption(String statement, String templateStatement, ParsingOptions options)
    {
        Statement parsedStatement = SQL_PARSER.createStatement(statement, options);
        Statement parsedTemplateStatement = SQL_PARSER.createStatement(templateStatement, options);

        assertFormatted(parsedStatement, parsedTemplateStatement);
    }
}
