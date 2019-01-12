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
import io.prestosql.block.BlockEncodingManager;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.ColumnPropertyManager;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.metadata.SchemaPropertyManager;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.metadata.TablePropertyManager;
import io.prestosql.security.AccessControl;
import io.prestosql.security.AllowAllAccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.PathElement;
import io.prestosql.sql.tree.PathSpecification;
import io.prestosql.sql.tree.SetPath;
import io.prestosql.transaction.TransactionManager;
import io.prestosql.type.TypeRegistry;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

public class TestSetPathTask
{
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final MetadataManager metadata;

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("stage-executor-%s"));

    public TestSetPathTask()
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
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testSetPath()
    {
        PathSpecification pathSpecification = new PathSpecification(Optional.empty(), ImmutableList.of(
                new PathElement(Optional.empty(), new Identifier("foo"))));

        QueryStateMachine stateMachine = createQueryStateMachine("SET PATH foo");
        executeSetPathTask(pathSpecification, stateMachine);

        assertEquals(stateMachine.getSetPath(), "foo");
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Catalog does not exist: .*")
    public void testSetPathInvalidCatalog()
    {
        PathSpecification invalidPathSpecification = new PathSpecification(Optional.empty(), ImmutableList.of(
                new PathElement(Optional.of(new Identifier("invalidCatalog")), new Identifier("thisDoesNotMatter"))));

        QueryStateMachine stateMachine = createQueryStateMachine("SET PATH invalidCatalog.thisDoesNotMatter");
        executeSetPathTask(invalidPathSpecification, stateMachine);
    }

    private QueryStateMachine createQueryStateMachine(String query)
    {
        return QueryStateMachine.begin(
                query,
                TEST_SESSION,
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                false,
                transactionManager,
                accessControl,
                executor,
                metadata,
                WarningCollector.NOOP);
    }

    private void executeSetPathTask(PathSpecification pathSpecification, QueryStateMachine stateMachine)
    {
        getFutureValue(new SetPathTask().execute(
                new SetPath(pathSpecification),
                transactionManager,
                metadata,
                accessControl,
                stateMachine,
                emptyList()));
    }
}
