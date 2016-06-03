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
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.Execute;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.QueryUtil.selectList;
import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.sql.QueryUtil.table;
import static com.facebook.presto.transaction.TransactionManager.createTestTransactionManager;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestPrepareTask
{
    private final MetadataManager metadata = createTestMetadataManager();
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        executor.shutdownNow();
    }

    @Test
    public void testPrepare()
    {
        Query query = simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo")));
        String sqlString = "PREPARE my_query FROM SELECT * FROM foo";
        Map<String, String> statements = executePrepare("my_query", query, sqlString, TEST_SESSION);
        assertEquals(statements, ImmutableMap.of("my_query", "SELECT *\nFROM\n  foo\n"));
    }

    @Test
    public void testPrepareNameExists()
    {
        Session session = TEST_SESSION.withPreparedStatement("my_query", "SELECT bar, baz from foo");

        Query query = simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo")));
        String sqlString = "PREPARE my_query FROM SELECT * FROM foo";
        Map<String, String> statements = executePrepare("my_query", query, sqlString, session);
        assertEquals(statements, ImmutableMap.of("my_query", "SELECT *\nFROM\n  foo\n"));
    }

    @Test
    public void testPrepareInvalidStatement()
    {
        Statement statement = new Execute("foo");
        String sqlString = "PREPARE my_query FROM EXECUTE foo";
        try {
            executePrepare("my_query", statement, sqlString, TEST_SESSION);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            assertEquals(e.getMessage(), "Invalid statement type for prepared statement: EXECUTE");
        }
    }

    private Map<String, String> executePrepare(String statementName, Statement statement, String sqlString, Session session)
    {
        TransactionManager transactionManager = createTestTransactionManager();
        QueryStateMachine stateMachine = QueryStateMachine.begin(new QueryId("query"), sqlString, session, URI.create("fake://uri"), false, transactionManager, executor);
        Prepare prepare = new Prepare(statementName, statement);
        new PrepareTask(new SqlParser()).execute(prepare, transactionManager, metadata, new AllowAllAccessControl(), stateMachine);
        return stateMachine.getAddedPreparedStatements();
    }
}
