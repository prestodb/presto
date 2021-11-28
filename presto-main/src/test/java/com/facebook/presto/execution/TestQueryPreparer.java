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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryPreparer.PreparedQuery;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.SystemSessionProperties.LOG_FORMATTED_QUERY_ENABLED;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.sql.QueryUtil.selectList;
import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.sql.QueryUtil.table;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PARAMETER_USAGE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestQueryPreparer
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final QueryPreparer QUERY_PREPARER = new QueryPreparer(SQL_PARSER);

    @Test
    public void testSelectStatement()
    {
        PreparedQuery preparedQuery = QUERY_PREPARER.prepareQuery(TEST_SESSION, "SELECT * FROM foo", WarningCollector.NOOP);
        assertEquals(preparedQuery.getStatement(),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo"))));
    }

    @Test
    public void testExecuteStatement()
    {
        Session session = testSessionBuilder()
                .addPreparedStatement("my_query", "SELECT * FROM foo")
                .build();
        PreparedQuery preparedQuery = QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query", WarningCollector.NOOP);
        assertEquals(preparedQuery.getStatement(),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo"))));
    }

    @Test
    public void testExecuteStatementDoesNotExist()
    {
        try {
            QUERY_PREPARER.prepareQuery(TEST_SESSION, "execute my_query", WarningCollector.NOOP);
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
            Session session = testSessionBuilder()
                    .addPreparedStatement("my_query", "SELECT * FROM foo where col1 = ?")
                    .build();
            QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query USING 1,2", WarningCollector.NOOP);
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
            Session session = testSessionBuilder()
                    .addPreparedStatement("my_query", "SELECT ? FROM foo where col1 = ?")
                    .build();
            QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query USING 1", WarningCollector.NOOP);
            fail("expected exception");
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), INVALID_PARAMETER_USAGE);
        }
    }

    @Test
    public void testFormattedQuery()
    {
        Session prepareSession = testSessionBuilder()
                .setSystemProperty(LOG_FORMATTED_QUERY_ENABLED, "true")
                .build();

        PreparedQuery preparedQuery = QUERY_PREPARER.prepareQuery(
                prepareSession,
                "PREPARE test FROM SELECT * FROM foo where col1 = ?",
                WarningCollector.NOOP);
        assertEquals(preparedQuery.getFormattedQuery(), Optional.of("-- Formatted Query:\n" +
                "PREPARE test FROM\n" +
                "   SELECT *\n" +
                "   FROM\n" +
                "     foo\n" +
                "   WHERE (col1 = ?)\n"));

        preparedQuery = QUERY_PREPARER.prepareQuery(
                prepareSession,
                "PREPARE test FROM SELECT * FROM foo",
                WarningCollector.NOOP);
        assertEquals(preparedQuery.getFormattedQuery(), Optional.of("-- Formatted Query:\n" +
                "PREPARE test FROM\n" +
                "   SELECT *\n" +
                "   FROM\n" +
                "     foo\n"));
    }
}
