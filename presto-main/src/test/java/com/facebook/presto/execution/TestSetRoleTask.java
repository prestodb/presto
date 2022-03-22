/*
 * Copyright 2017, Teradata Corp. All rights reserved.
 */

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
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.SetRole;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.execution.TaskTestUtils.createQueryStateMachine;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

public class TestSetRoleTask
{
    private static final String CATALOG_NAME = "foo";

    private TransactionManager transactionManager;
    private AccessControl accessControl;
    private MetadataManager metadata;
    private ExecutorService executor;
    private SqlParser parser;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        CatalogManager catalogManager = new CatalogManager();
        transactionManager = createTestTransactionManager(catalogManager);
        accessControl = new AllowAllAccessControl();

        metadata = MetadataManager.createTestMetadataManager(transactionManager, new FeaturesConfig());

        catalogManager.registerCatalog(createBogusTestingCatalog(CATALOG_NAME));
        executor = newCachedThreadPool(daemonThreadsNamed("test-set-role-task-executor-%s"));
        parser = new SqlParser();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        executor.shutdownNow();
        executor = null;
        metadata = null;
        accessControl = null;
        transactionManager = null;
        parser = null;
    }

    @Test
    public void testSetRole()
    {
        assertSetRole("SET ROLE ALL", ImmutableMap.of(CATALOG_NAME, new SelectedRole(SelectedRole.Type.ALL, Optional.empty())));
        assertSetRole("SET ROLE NONE", ImmutableMap.of(CATALOG_NAME, new SelectedRole(SelectedRole.Type.NONE, Optional.empty())));
        assertSetRole("SET ROLE bar", ImmutableMap.of(CATALOG_NAME, new SelectedRole(SelectedRole.Type.ROLE, Optional.of("bar"))));
    }

    private void assertSetRole(String statement, Map<String, SelectedRole> expected)
    {
        SetRole setRole = (SetRole) parser.createStatement(statement);
        Session session = testSessionBuilder()
                .setCatalog(CATALOG_NAME)
                .build();
        QueryStateMachine stateMachine = createQueryStateMachine(statement, session, false, transactionManager, executor, metadata);
        new SetRoleTask().execute(setRole, transactionManager, metadata, accessControl, stateMachine, ImmutableList.of());
        QueryInfo queryInfo = stateMachine.getQueryInfo(Optional.empty());
        assertEquals(queryInfo.getSetRoles(), expected);
    }
}
