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
package io.prestosql.execution;

import io.prestosql.Session;
import io.prestosql.execution.QueryPreparer.PreparedQuery;
import io.prestosql.spi.PrestoException;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.AllColumns;
import io.prestosql.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.sql.QueryUtil.selectList;
import static io.prestosql.sql.QueryUtil.simpleQuery;
import static io.prestosql.sql.QueryUtil.table;
import static io.prestosql.sql.analyzer.SemanticErrorCode.INVALID_PARAMETER_USAGE;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestQueryPreparer
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final QueryPreparer QUERY_PREPARER = new QueryPreparer(SQL_PARSER);

    @Test
    public void testSelectStatement()
    {
        PreparedQuery preparedQuery = QUERY_PREPARER.prepareQuery(TEST_SESSION, "SELECT * FROM foo");
        assertEquals(preparedQuery.getStatement(),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo"))));
    }

    @Test
    public void testExecuteStatement()
    {
        Session session = testSessionBuilder()
                .addPreparedStatement("my_query", "SELECT * FROM foo")
                .build();
        PreparedQuery preparedQuery = QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query");
        assertEquals(preparedQuery.getStatement(),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo"))));
    }

    @Test
    public void testExecuteStatementDoesNotExist()
    {
        try {
            QUERY_PREPARER.prepareQuery(TEST_SESSION, "execute my_query");
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
            QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query USING 1,2");
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
            QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query USING 1");
            fail("expected exception");
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), INVALID_PARAMETER_USAGE);
        }
    }
}
