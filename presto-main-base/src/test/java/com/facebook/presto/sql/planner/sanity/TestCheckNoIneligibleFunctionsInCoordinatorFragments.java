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
package com.facebook.presto.sql.planner.sanity;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.functionNamespace.SqlInvokedFunctionNamespaceManagerConfig;
import com.facebook.presto.functionNamespace.execution.NoopSqlFunctionExecutor;
import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutors;
import com.facebook.presto.functionNamespace.testing.InMemoryFunctionNamespaceManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TestingColumnHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.DETERMINISTIC;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Language.CPP;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Language.JAVA;
import static com.facebook.presto.spi.function.RoutineCharacteristics.NullCallClause.RETURNS_NULL_ON_NULL_INPUT;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestCheckNoIneligibleFunctionsInCoordinatorFragments
        extends BasePlanTest
{
    // CPP function for testing (similar to TestAddExchangesPlansWithFunctions)
    private static final SqlInvokedFunction CPP_FUNC = new SqlInvokedFunction(
            new QualifiedObjectName("dummy", "unittest", "cpp_func"),
            ImmutableList.of(new Parameter("x", parseTypeSignature(StandardTypes.VARCHAR))),
            parseTypeSignature(StandardTypes.VARCHAR),
            "cpp_func(x)",
            RoutineCharacteristics.builder().setLanguage(CPP).setDeterminism(DETERMINISTIC).setNullCallClause(RETURNS_NULL_ON_NULL_INPUT).build(),
            "",
            notVersioned());

    // JAVA function for testing
    private static final SqlInvokedFunction JAVA_FUNC = new SqlInvokedFunction(
            new QualifiedObjectName("dummy", "unittest", "java_func"),
            ImmutableList.of(new Parameter("x", parseTypeSignature(StandardTypes.VARCHAR))),
            parseTypeSignature(StandardTypes.VARCHAR),
            "java_func(x)",
            RoutineCharacteristics.builder().setLanguage(JAVA).setDeterminism(DETERMINISTIC).setNullCallClause(RETURNS_NULL_ON_NULL_INPUT).build(),
            "",
            notVersioned());

    public TestCheckNoIneligibleFunctionsInCoordinatorFragments()
    {
        super(TestCheckNoIneligibleFunctionsInCoordinatorFragments::createTestQueryRunner);
    }

    private static LocalQueryRunner createTestQueryRunner()
    {
        LocalQueryRunner queryRunner = new LocalQueryRunner(
                testSessionBuilder()
                        .setCatalog("local")
                        .setSchema("tiny")
                        .build(),
                new FeaturesConfig().setNativeExecutionEnabled(true),
                new FunctionsConfig().setDefaultNamespacePrefix("dummy.unittest"));

        queryRunner.createCatalog("local", new TpchConnectorFactory(), ImmutableMap.of());

        // Add function namespace with both CPP and JAVA functions
        queryRunner.getMetadata().getFunctionAndTypeManager().addFunctionNamespace(
                "dummy",
                new InMemoryFunctionNamespaceManager(
                        "dummy",
                        new SqlFunctionExecutors(
                                ImmutableMap.of(
                                        CPP, FunctionImplementationType.CPP,
                                        JAVA, FunctionImplementationType.JAVA),
                                new NoopSqlFunctionExecutor()),
                        new SqlInvokedFunctionNamespaceManagerConfig().setSupportedFunctionLanguages("CPP,JAVA")));

        // Register the functions
        queryRunner.getMetadata().getFunctionAndTypeManager().createFunction(CPP_FUNC, false);
        queryRunner.getMetadata().getFunctionAndTypeManager().createFunction(JAVA_FUNC, false);

        return queryRunner;
    }

    @Test
    public void testSystemTableScanWithJavaFunctionPasses()
    {
        // System table scan with Java function in same fragment should pass
        validatePlan(
                p -> {
                    VariableReferenceExpression col = p.variable("col", VARCHAR);
                    VariableReferenceExpression result = p.variable("result", VARCHAR);

                    // Create a system table scan - using proper system connector ID
                    TableHandle systemTableHandle = new TableHandle(
                            ConnectorId.createSystemTablesConnectorId(new ConnectorId("local")),
                            new TestingTableHandle(),
                            TestingTransactionHandle.create(),
                            Optional.empty());

                    PlanNode tableScan = p.tableScan(
                            systemTableHandle,
                            ImmutableList.of(col),
                            ImmutableMap.of(col, new TestingColumnHandle("col")));

                    // Java function (using our registered java_func)
                    return p.project(
                            assignment(result, p.rowExpression("java_func(col)")),
                            tableScan);
                });
    }

    @Test(expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = "Fragment contains both system table scan and non-Java functions.*")
    public void testSystemTableScanWithCppFunctionInProjectFails()
    {
        // System table scan with C++ function in same fragment should fail
        validatePlan(
                p -> {
                    VariableReferenceExpression col = p.variable("col", VARCHAR);
                    VariableReferenceExpression result = p.variable("result", VARCHAR);

                    // System table scan
                    TableHandle systemTableHandle = new TableHandle(
                            ConnectorId.createSystemTablesConnectorId(new ConnectorId("local")),
                            new TestingTableHandle(),
                            TestingTransactionHandle.create(),
                            Optional.empty());

                    PlanNode systemScan = p.tableScan(
                            systemTableHandle,
                            ImmutableList.of(col),
                            ImmutableMap.of(col, new TestingColumnHandle("col")));

                    // C++ function (using our registered cpp_func)
                    return p.project(
                            assignment(result, p.rowExpression("cpp_func(col)")),
                            systemScan);
                });
    }

    @Test(expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = "Fragment contains both system table scan and non-Java functions.*")
    public void testSystemTableScanWithCppFunctionInFilterFails()
    {
        // System table scan with C++ function in filter should fail
        validatePlan(
                p -> {
                    VariableReferenceExpression col = p.variable("col", VARCHAR);

                    // System table scan
                    TableHandle systemTableHandle = new TableHandle(
                            ConnectorId.createSystemTablesConnectorId(new ConnectorId("local")),
                            new TestingTableHandle(),
                            TestingTransactionHandle.create(),
                            Optional.empty());

                    PlanNode systemScan = p.tableScan(
                            systemTableHandle,
                            ImmutableList.of(col),
                            ImmutableMap.of(col, new TestingColumnHandle("col")));

                    // Filter with C++ function
                    return p.filter(
                            p.rowExpression("cpp_func(col) = 'test'"),
                            systemScan);
                });
    }

    @Test
    public void testSystemTableScanWithCppFunctionSeparatedByExchangePasses()
    {
        // System table scan and C++ function separated by exchange should pass
        validatePlan(
                p -> {
                    VariableReferenceExpression col = p.variable("col", VARCHAR);
                    VariableReferenceExpression result = p.variable("result", VARCHAR);

                    // System table scan
                    TableHandle systemTableHandle = new TableHandle(
                            ConnectorId.createSystemTablesConnectorId(new ConnectorId("local")),
                            new TestingTableHandle(),
                            TestingTransactionHandle.create(),
                            Optional.empty());

                    PlanNode systemScan = p.tableScan(
                            systemTableHandle,
                            ImmutableList.of(col),
                            ImmutableMap.of(col, new TestingColumnHandle("col")));

                    // Exchange creates fragment boundary
                    PartitioningScheme partitioningScheme = new PartitioningScheme(
                            Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()),
                            ImmutableList.of(col));

                    ExchangeNode exchange = new ExchangeNode(
                            Optional.empty(),
                            p.getIdAllocator().getNextId(),
                            ExchangeNode.Type.GATHER,
                            ExchangeNode.Scope.LOCAL,
                            partitioningScheme,
                            ImmutableList.of(systemScan),
                            ImmutableList.of(ImmutableList.of(col)),
                            false,
                            Optional.empty());

                    // C++ function in different fragment
                    return p.project(
                            assignment(result, p.rowExpression("cpp_func(col)")),
                            exchange);
                });
    }

    @Test
    public void testRegularTableScanWithCppFunctionPasses()
    {
        // Regular table scan with C++ function should pass (no system table)
        validatePlan(
                p -> {
                    VariableReferenceExpression col = p.variable("col", VARCHAR);
                    VariableReferenceExpression result = p.variable("result", VARCHAR);

                    // Regular table scan (not system)
                    TableHandle regularTableHandle = new TableHandle(
                            new ConnectorId("local"),
                            new TestingTableHandle(),
                            TestingTransactionHandle.create(),
                            Optional.empty());

                    PlanNode regularScan = p.tableScan(
                            regularTableHandle,
                            ImmutableList.of(col),
                            ImmutableMap.of(col, new TestingColumnHandle("col")));

                    // C++ function
                    return p.project(
                            assignment(result, p.rowExpression("cpp_func(col)")),
                            regularScan);
                });
    }

    @Test(expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = "Fragment contains both system table scan and non-Java functions.*")
    public void testMultipleFragmentsWithCppFunctionInSystemFragment()
    {
        // Complex plan where CPP function is in same fragment as system table scan (should fail)
        validatePlan(
                p -> {
                    VariableReferenceExpression col1 = p.variable("col1", VARCHAR);
                    VariableReferenceExpression col2 = p.variable("col2", BIGINT);
                    VariableReferenceExpression col3 = p.variable("col3", BIGINT);

                    // Fragment 1: System table scan
                    TableHandle systemTableHandle = new TableHandle(
                            ConnectorId.createSystemTablesConnectorId(new ConnectorId("local")),
                            new TestingTableHandle(),
                            TestingTransactionHandle.create(),
                            Optional.empty());

                    PlanNode systemScan = p.tableScan(
                            systemTableHandle,
                            ImmutableList.of(col1),
                            ImmutableMap.of(col1, new TestingColumnHandle("col1")));

                    // Convert to numeric for join (using CPP function - this should fail)
                    PlanNode project1 = p.project(
                            assignment(col2, p.rowExpression("cast(cpp_func(col1) as bigint)")),
                            systemScan);

                    // Fragment 2: Regular values with computation
                    PlanNode values = p.values(col3);

                    // Exchange to separate fragments
                    PartitioningScheme partitioningScheme1 = new PartitioningScheme(
                            Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()),
                            ImmutableList.of(col2));

                    ExchangeNode exchange1 = new ExchangeNode(
                            Optional.empty(),
                            p.getIdAllocator().getNextId(),
                            ExchangeNode.Type.GATHER,
                            ExchangeNode.Scope.LOCAL,
                            partitioningScheme1,
                            ImmutableList.of(project1),
                            ImmutableList.of(ImmutableList.of(col2)),
                            false,
                            Optional.empty());

                    PartitioningScheme partitioningScheme2 = new PartitioningScheme(
                            Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()),
                            ImmutableList.of(col3));

                    ExchangeNode exchange2 = new ExchangeNode(
                            Optional.empty(),
                            p.getIdAllocator().getNextId(),
                            ExchangeNode.Type.GATHER,
                            ExchangeNode.Scope.LOCAL,
                            partitioningScheme2,
                            ImmutableList.of(values),
                            ImmutableList.of(ImmutableList.of(col3)),
                            false,
                            Optional.empty());

                    // Join the results
                    return p.join(
                            JoinType.INNER,
                            exchange1,
                            exchange2,
                            p.rowExpression("col2 = col3"));
                });
    }

    @Test
    public void testMultipleFragmentsWithExchange()
    {
        // Complex plan with multiple fragments properly separated (Java function - should pass)
        validatePlan(
                p -> {
                    VariableReferenceExpression col1 = p.variable("col1", VARCHAR);
                    VariableReferenceExpression col2 = p.variable("col2", BIGINT);
                    VariableReferenceExpression col3 = p.variable("col3", BIGINT);

                    // Fragment 1: System table scan
                    TableHandle systemTableHandle = new TableHandle(
                            ConnectorId.createSystemTablesConnectorId(new ConnectorId("local")),
                            new TestingTableHandle(),
                            TestingTransactionHandle.create(),
                            Optional.empty());

                    PlanNode systemScan = p.tableScan(
                            systemTableHandle,
                            ImmutableList.of(col1),
                            ImmutableMap.of(col1, new TestingColumnHandle("col1")));

                    // Convert to numeric for join (using Java function)
                    PlanNode project1 = p.project(
                            assignment(col2, p.rowExpression("cast(java_func(col1) as bigint)")),
                            systemScan);

                    // Fragment 2: Regular values with computation
                    PlanNode values = p.values(col3);

                    // Exchange to separate fragments
                    PartitioningScheme partitioningScheme1 = new PartitioningScheme(
                            Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()),
                            ImmutableList.of(col2));

                    ExchangeNode exchange1 = new ExchangeNode(
                            Optional.empty(),
                            p.getIdAllocator().getNextId(),
                            ExchangeNode.Type.GATHER,
                            ExchangeNode.Scope.LOCAL,
                            partitioningScheme1,
                            ImmutableList.of(project1),
                            ImmutableList.of(ImmutableList.of(col2)),
                            false,
                            Optional.empty());

                    PartitioningScheme partitioningScheme2 = new PartitioningScheme(
                            Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()),
                            ImmutableList.of(col3));

                    ExchangeNode exchange2 = new ExchangeNode(
                            Optional.empty(),
                            p.getIdAllocator().getNextId(),
                            ExchangeNode.Type.GATHER,
                            ExchangeNode.Scope.LOCAL,
                            partitioningScheme2,
                            ImmutableList.of(values),
                            ImmutableList.of(ImmutableList.of(col3)),
                            false,
                            Optional.empty());

                    // Join the results
                    return p.join(
                            JoinType.INNER,
                            exchange1,
                            exchange2,
                            p.rowExpression("col2 = col3"));
                });
    }

    @Test
    public void testFilterAndProjectWithSystemTable()
    {
        // Test filter and project both with Java functions on system table
        validatePlan(
                p -> {
                    VariableReferenceExpression col = p.variable("col", VARCHAR);
                    VariableReferenceExpression len = p.variable("len", BIGINT);

                    // System table scan
                    TableHandle systemTableHandle = new TableHandle(
                            ConnectorId.createSystemTablesConnectorId(new ConnectorId("local")),
                            new TestingTableHandle(),
                            TestingTransactionHandle.create(),
                            Optional.empty());

                    PlanNode systemScan = p.tableScan(
                            systemTableHandle,
                            ImmutableList.of(col),
                            ImmutableMap.of(col, new TestingColumnHandle("col")));

                    // Filter with Java function
                    PlanNode filtered = p.filter(
                            p.rowExpression("java_func(col) = 'test'"),
                            systemScan);

                    // Project with Java function
                    return p.project(
                            assignment(len, p.rowExpression("cast(java_func(col) as bigint)")),
                            filtered);
                });
    }

    private void validatePlan(Function<PlanBuilder, PlanNode> planProvider)
    {
        Session session = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .build();

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        Metadata metadata = getQueryRunner().getMetadata();
        PlanBuilder builder = new PlanBuilder(TEST_SESSION, idAllocator, metadata);
        PlanNode planNode = planProvider.apply(builder);

        getQueryRunner().inTransaction(session, transactionSession -> {
            new CheckNoIneligibleFunctionsInCoordinatorFragments().validate(planNode, transactionSession, metadata, WarningCollector.NOOP);
            return null;
        });
    }
}
