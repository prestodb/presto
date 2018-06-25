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

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SchemaPropertyManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.PathElement;
import com.facebook.presto.sql.tree.PathSpecification;
import com.facebook.presto.sql.tree.SetPath;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.transaction.TransactionManager.createTestTransactionManager;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
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
        PathSpecification pathSpecification = new PathSpecification(
                Optional.empty(), ImmutableList.of(
                        new PathElement(Optional.empty(), new Identifier("foo"))));

        QueryStateMachine stateMachine = createQueryStateMachine("SET PATH foo");
        executeSetPathTask(pathSpecification, stateMachine);

        assertEquals(stateMachine.getSetPath(), "foo");
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Catalog does not exist: .*")
    public void testSetPathInvalidCatalog()
    {
        PathSpecification invalidPathSpecification = new PathSpecification(
                Optional.empty(), ImmutableList.of(
                        new PathElement(Optional.of(new Identifier("invalidCatalog")), new Identifier("thisDoesNotMatter"))));

        QueryStateMachine stateMachine = createQueryStateMachine("SET PATH invalidCatalog.thisDoesNotMatter");
        executeSetPathTask(invalidPathSpecification, stateMachine);
    }

    private QueryStateMachine createQueryStateMachine(String query)
    {
        return QueryStateMachine.begin(
                new QueryId("query"),
                query,
                TEST_SESSION,
                URI.create("fake://uri"),
                false,
                transactionManager,
                accessControl,
                executor,
                metadata);
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
