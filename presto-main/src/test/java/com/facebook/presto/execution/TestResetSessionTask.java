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
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;
import static com.facebook.presto.transaction.TransactionManager.createTestTransactionManager;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

public class TestResetSessionTask
{
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("stage-executor-%s"));
    private final MetadataManager metadata = MetadataManager.createTestMetadataManager();

    public TestResetSessionTask()
    {
        metadata.getSessionPropertyManager().addSystemSessionProperty(stringSessionProperty(
                "foo",
                "test property",
                null,
                false));
        metadata.getSessionPropertyManager().addConnectorSessionProperties("catalog", ImmutableList.of(stringSessionProperty(
                "baz",
                "test property",
                null,
                false)));
    }

    @AfterClass
    public void tearDown()
            throws Exception
    {
        executor.shutdownNow();
    }

    @Test
    public void test()
            throws Exception
    {
        Session session = TEST_SESSION.withSystemProperty("foo", "bar").withCatalogProperty("catalog", "baz", "blah");

        TransactionManager transactionManager = createTestTransactionManager();
        QueryStateMachine stateMachine = QueryStateMachine.begin(new QueryId("query"), "reset foo", session, URI.create("fake://uri"), false, transactionManager, executor);
        new ResetSessionTask().execute(new ResetSession(QualifiedName.of("catalog", "baz")), transactionManager, metadata, new AllowAllAccessControl(), stateMachine, emptyList()).join();

        Set<String> sessionProperties = stateMachine.getResetSessionProperties();
        assertEquals(sessionProperties, ImmutableSet.of("catalog.baz"));
    }
}
