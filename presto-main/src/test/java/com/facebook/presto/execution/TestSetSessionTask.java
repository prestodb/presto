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

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;
import static com.facebook.presto.transaction.TransactionManager.createTestTransactionManager;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

public class TestSetSessionTask
{
    private final MetadataManager metadata = MetadataManager.createTestMetadataManager();

    public TestSetSessionTask()
    {
        metadata.getSessionPropertyManager().addSystemSessionProperty(stringSessionProperty(
                "foo",
                "test property",
                null,
                false));
        metadata.getSessionPropertyManager().addConnectorSessionProperties("foo", ImmutableList.of(stringSessionProperty(
                "bar",
                "test property",
                null,
                false)));
    }

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("stage-executor-%s"));

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        executor.shutdownNow();
    }

    @Test
    public void testSetSession()
            throws Exception
    {
        testSetSession(new StringLiteral("baz"), "baz");
        testSetSession(new FunctionCall(QualifiedName.of("concat"), ImmutableList.of(
                new StringLiteral("ban"),
                new StringLiteral("ana"))), "banana");
    }

    private void testSetSession(Expression expression, String expectedValue)
            throws Exception
    {
        TransactionManager transactionManager = createTestTransactionManager();
        QueryStateMachine stateMachine = QueryStateMachine.begin(new QueryId("query"), "set foo.bar = 'baz'", TEST_SESSION, URI.create("fake://uri"), false, transactionManager, executor);
        new SetSessionTask().execute(new SetSession(QualifiedName.of("foo", "bar"), expression), transactionManager, metadata, new AllowAllAccessControl(), stateMachine).join();

        Map<String, String> sessionProperties = stateMachine.getSetSessionProperties();
        assertEquals(sessionProperties, ImmutableMap.of("foo.bar", expectedValue));
    }
}
