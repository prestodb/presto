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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.analyzer.BuiltInQueryPreparer.BuiltInPreparedQuery;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.sql.QueryUtil.selectList;
import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.sql.QueryUtil.table;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PARAMETER_USAGE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestBuiltInQueryPreparer
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final BuiltInQueryPreparer QUERY_PREPARER = new BuiltInQueryPreparer(SQL_PARSER);
    private static final Map<String, String> emptyPreparedStatements = ImmutableMap.of();
    private static final AnalyzerOptions testAnalyzerOptions = AnalyzerOptions.builder().build();

    @Test
    public void testSelectStatement()
    {
        BuiltInPreparedQuery preparedQuery = QUERY_PREPARER.prepareQuery(testAnalyzerOptions, "SELECT * FROM foo", emptyPreparedStatements, WarningCollector.NOOP);
        assertEquals(preparedQuery.getStatement(),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo"))));
    }

    @Test
    public void testExecuteStatement()
    {
        Map<String, String> preparedStatements = ImmutableMap.of("my_query", "SELECT * FROM foo");
        BuiltInPreparedQuery preparedQuery = QUERY_PREPARER.prepareQuery(testAnalyzerOptions, "EXECUTE my_query", preparedStatements, WarningCollector.NOOP);
        assertEquals(preparedQuery.getStatement(),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo"))));
    }

    @Test
    public void testExecuteStatementDoesNotExist()
    {
        try {
            QUERY_PREPARER.prepareQuery(testAnalyzerOptions, "execute my_query", emptyPreparedStatements, WarningCollector.NOOP);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_FOUND.toErrorCode());
        }
    }

    @Test
    public void testTooManyParameters()
    {
        try {
            Map<String, String> preparedStatements = ImmutableMap.of("my_query", "SELECT * FROM foo where col1 = ?");
            QUERY_PREPARER.prepareQuery(testAnalyzerOptions, "EXECUTE my_query USING 1,2", preparedStatements, WarningCollector.NOOP);
            fail("expected exception");
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), INVALID_PARAMETER_USAGE);
        }
    }

    @Test
    public void testTooFewParameters()
    {
        try {
            Map<String, String> preparedStatements = ImmutableMap.of("my_query", "SELECT ? FROM foo where col1 = ?");
            QUERY_PREPARER.prepareQuery(testAnalyzerOptions, "EXECUTE my_query USING 1", preparedStatements, WarningCollector.NOOP);
            fail("expected exception");
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), INVALID_PARAMETER_USAGE);
        }
    }

    @Test
    public void testFormattedQuery()
    {
        AnalyzerOptions analyzerOptions = AnalyzerOptions.builder().setLogFormattedQueryEnabled(true).build();
        BuiltInPreparedQuery preparedQuery = QUERY_PREPARER.prepareQuery(
                analyzerOptions,
                "PREPARE test FROM SELECT * FROM foo where col1 = ?",
                emptyPreparedStatements,
                WarningCollector.NOOP);
        assertEquals(preparedQuery.getFormattedQuery(), Optional.of("-- Formatted Query:\n" +
                "PREPARE test FROM\n" +
                "   SELECT *\n" +
                "   FROM\n" +
                "     foo\n" +
                "   WHERE (col1 = ?)\n"));

        preparedQuery = QUERY_PREPARER.prepareQuery(
                analyzerOptions,
                "PREPARE test FROM SELECT * FROM foo",
                emptyPreparedStatements,
                WarningCollector.NOOP);
        assertEquals(preparedQuery.getFormattedQuery(), Optional.of("-- Formatted Query:\n" +
                "PREPARE test FROM\n" +
                "   SELECT *\n" +
                "   FROM\n" +
                "     foo\n"));
    }
}
