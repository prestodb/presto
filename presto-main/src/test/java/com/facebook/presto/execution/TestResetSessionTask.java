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
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.execution.TaskTestUtils.createQueryStateMachine;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

public class TestResetSessionTask
{
    private static final String CATALOG_NAME = "catalog";
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("stage-executor-%s"));
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final MetadataManager metadata;

    public TestResetSessionTask()
    {
        CatalogManager catalogManager = new CatalogManager();
        transactionManager = createTestTransactionManager(catalogManager);
        accessControl = new AllowAllAccessControl();

        metadata = createTestMetadataManager(transactionManager, new FeaturesConfig());

        metadata.getSessionPropertyManager().addSystemSessionProperty(stringProperty(
                "foo",
                "test property",
                null,
                false));

        Catalog bogusTestingCatalog = createBogusTestingCatalog(CATALOG_NAME);
        metadata.getSessionPropertyManager().addConnectorSessionProperties(bogusTestingCatalog.getConnectorId(), ImmutableList.of(stringProperty(
                "baz",
                "test property",
                null,
                false)));
        catalogManager.registerCatalog(bogusTestingCatalog);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void test()
    {
        Session session = testSessionBuilder(metadata.getSessionPropertyManager())
                .setSystemProperty("foo", "bar")
                .setCatalogSessionProperty(CATALOG_NAME, "baz", "blah")
                .build();
        QueryStateMachine stateMachine = createQueryStateMachine("reset foo", session, false, transactionManager, executor, metadata);

        getFutureValue(new ResetSessionTask().execute(
                new ResetSession(QualifiedName.of(CATALOG_NAME, "baz")),
                transactionManager,
                metadata,
                accessControl,
                stateMachine,
                emptyList()));

        Set<String> sessionProperties = stateMachine.getResetSessionProperties();
        assertEquals(sessionProperties, ImmutableSet.of("catalog.baz"));
    }
}
