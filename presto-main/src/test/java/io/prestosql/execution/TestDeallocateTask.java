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

import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.security.AccessControl;
import io.prestosql.security.AccessControlManager;
import io.prestosql.security.AllowAllAccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.sql.tree.Deallocate;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.transaction.TransactionManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestDeallocateTask
{
    private final MetadataManager metadata = createTestMetadataManager();
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testDeallocate()
    {
        Session session = testSessionBuilder()
                .addPreparedStatement("my_query", "SELECT bar, baz FROM foo")
                .build();
        Set<String> statements = executeDeallocate("my_query", "DEALLOCATE PREPARE my_query", session);
        assertEquals(statements, ImmutableSet.of("my_query"));
    }

    @Test
    public void testDeallocateNoSuchStatement()
    {
        try {
            executeDeallocate("my_query", "DEALLOCATE PREPARE my_query", TEST_SESSION);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_FOUND.toErrorCode());
            assertEquals(e.getMessage(), "Prepared statement not found: my_query");
        }
    }

    private Set<String> executeDeallocate(String statementName, String sqlString, Session session)
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControl accessControl = new AccessControlManager(transactionManager);
        QueryStateMachine stateMachine = QueryStateMachine.begin(
                sqlString,
                session,
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                false,
                transactionManager,
                accessControl,
                executor,
                metadata,
                WarningCollector.NOOP);
        Deallocate deallocate = new Deallocate(new Identifier(statementName));
        new DeallocateTask().execute(deallocate, transactionManager, metadata, new AllowAllAccessControl(), stateMachine, emptyList());
        return stateMachine.getDeallocatedPreparedStatements();
    }
}
