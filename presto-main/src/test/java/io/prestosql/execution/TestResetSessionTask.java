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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.block.BlockEncodingManager;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Catalog;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.ColumnPropertyManager;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.metadata.SchemaPropertyManager;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.metadata.TablePropertyManager;
import io.prestosql.security.AccessControl;
import io.prestosql.security.AllowAllAccessControl;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.ResetSession;
import io.prestosql.transaction.TransactionManager;
import io.prestosql.type.TypeRegistry;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.spi.session.PropertyMetadata.stringProperty;
import static io.prestosql.testing.TestingSession.createBogusTestingCatalog;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.transaction.InMemoryTransactionManager.createTestTransactionManager;
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

        metadata = new MetadataManager(
                new FeaturesConfig(),
                new TypeRegistry(),
                new BlockEncodingManager(new TypeRegistry()),
                new SessionPropertyManager(),
                new SchemaPropertyManager(),
                new TablePropertyManager(),
                new ColumnPropertyManager(),
                transactionManager);

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

        QueryStateMachine stateMachine = QueryStateMachine.begin(
                "reset foo",
                session,
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                false,
                transactionManager,
                accessControl,
                executor,
                metadata,
                WarningCollector.NOOP);

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
