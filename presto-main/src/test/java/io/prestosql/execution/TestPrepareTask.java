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

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.security.AccessControlManager;
import io.prestosql.security.AllowAllAccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.AllColumns;
import io.prestosql.sql.tree.Execute;
import io.prestosql.sql.tree.Prepare;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.Statement;
import io.prestosql.transaction.TransactionManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.sql.QueryUtil.identifier;
import static io.prestosql.sql.QueryUtil.selectList;
import static io.prestosql.sql.QueryUtil.simpleQuery;
import static io.prestosql.sql.QueryUtil.table;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestPrepareTask
{
    private final MetadataManager metadata = createTestMetadataManager();
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));

    @AfterClass(alwaysRun = true)
    public void tearDown()
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
        Session session = testSessionBuilder()
                .addPreparedStatement("my_query", "SELECT bar, baz from foo")
                .build();

        Query query = simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo")));
        String sqlString = "PREPARE my_query FROM SELECT * FROM foo";
        Map<String, String> statements = executePrepare("my_query", query, sqlString, session);
        assertEquals(statements, ImmutableMap.of("my_query", "SELECT *\nFROM\n  foo\n"));
    }

    @Test
    public void testPrepareInvalidStatement()
    {
        Statement statement = new Execute(identifier("foo"), emptyList());
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
        QueryStateMachine stateMachine = QueryStateMachine.begin(
                sqlString,
                session,
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                false,
                transactionManager,
                new AccessControlManager(transactionManager),
                executor,
                metadata,
                WarningCollector.NOOP);
        Prepare prepare = new Prepare(identifier(statementName), statement);
        new PrepareTask(new SqlParser()).execute(prepare, transactionManager, metadata, new AllowAllAccessControl(), stateMachine, emptyList());
        return stateMachine.getAddedPreparedStatements();
    }
}
