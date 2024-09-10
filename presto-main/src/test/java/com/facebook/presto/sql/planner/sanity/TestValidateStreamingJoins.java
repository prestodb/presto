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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.cost.HistoryBasedOptimizationConfig;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.execution.warnings.WarningCollectorConfig;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import com.facebook.presto.sql.planner.CompilerConfig;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tpch.TpchTableLayoutHandle;
import com.facebook.presto.tracing.TracingConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestValidateStreamingJoins
        extends BasePlanTest
{
    private Session defaultSession;
    private Session spillSession;
    private Metadata metadata;
    private PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
    private TableHandle nationTableHandle;
    private TableHandle supplierTableHandle;
    private ColumnHandle nationColumnHandle;
    private ColumnHandle suppColumnHandle;

    @BeforeClass
    public void setup()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny");
        defaultSession = sessionBuilder.build();
        spillSession = testSessionBuilder(
                new SessionPropertyManager(new SystemSessionProperties(
                        new QueryManagerConfig(),
                        new TaskManagerConfig(),
                        new MemoryManagerConfig(),
                        new FeaturesConfig().setSpillerSpillPaths("/path/to/nowhere"),
                        new FunctionsConfig(),
                        new NodeMemoryConfig(),
                        new WarningCollectorConfig(),
                        new NodeSchedulerConfig(),
                        new NodeSpillConfig(),
                        new TracingConfig(),
                        new CompilerConfig(),
                        new HistoryBasedOptimizationConfig())))
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("spill_enabled", "true")
                .setSystemProperty("join_spill_enabled", "true")
                .build();
        metadata = getQueryRunner().getMetadata();

        TpchTableHandle nationTpchTableHandle = new TpchTableHandle("nation", 1.0);
        TpchTableHandle supplierTpchTableHandle = new TpchTableHandle("supplier", 1.0);
        ConnectorId connectorId = getCurrentConnectorId();
        nationTableHandle = new TableHandle(
                connectorId,
                nationTpchTableHandle,
                TestingTransactionHandle.create(),
                Optional.of(new TpchTableLayoutHandle(nationTpchTableHandle, TupleDomain.all())));
        supplierTableHandle = new TableHandle(
                connectorId,
                supplierTpchTableHandle,
                TestingTransactionHandle.create(),
                Optional.of(new TpchTableLayoutHandle(supplierTpchTableHandle, TupleDomain.all())));
        nationColumnHandle = new TpchColumnHandle("nationkey", BIGINT);
        suppColumnHandle = new TpchColumnHandle("suppkey", BIGINT);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        defaultSession = null;
        spillSession = null;
        metadata = null;
        idAllocator = null;
        nationTableHandle = null;
        supplierTableHandle = null;
    }

    @Test
    public void testValidateSuccessful()
    {
        validatePlan(
                p -> p.join(
                        INNER,
                        p.tableScan(nationTableHandle, ImmutableList.of(p.variable("nationkeyN", BIGINT)), ImmutableMap.of(p.variable("nationkeyN", BIGINT), nationColumnHandle)),
                        p.exchange(e -> e
                                .scope(ExchangeNode.Scope.LOCAL)
                                .type(ExchangeNode.Type.REPARTITION)
                                .addSource(p.tableScan(supplierTableHandle, ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)), ImmutableMap.of(p.variable("nationkeyS", BIGINT), nationColumnHandle, p.variable("suppkey", BIGINT), suppColumnHandle)))
                                .addInputsSet(ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)))
                                .fixedHashDistributionPartitioningScheme(ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)), ImmutableList.of(p.variable("nationkeyS", BIGINT)))),
                        ImmutableList.of(new EquiJoinClause(p.variable("nationkeyN", BIGINT), p.variable("nationkeyS", BIGINT))),
                        ImmutableList.of(p.variable("nationkeyN", BIGINT), p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)),
                        Optional.empty()));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Build side needs an additional local exchange for join: [0-9]*")
    public void testValidateFailed()
    {
        validatePlan(
                p -> p.join(
                        INNER,
                        p.tableScan(nationTableHandle, ImmutableList.of(p.variable("nationkeyN", BIGINT)), ImmutableMap.of(p.variable("nationkeyN", BIGINT), nationColumnHandle)),
                        p.tableScan(supplierTableHandle, ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)), ImmutableMap.of(p.variable("nationkeyS", BIGINT), nationColumnHandle, p.variable("suppkey", BIGINT), suppColumnHandle)),
                        ImmutableList.of(new EquiJoinClause(p.variable("nationkeyN", BIGINT), p.variable("nationkeyS", BIGINT))),
                        ImmutableList.of(p.variable("nationkeyN", BIGINT), p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)),
                        Optional.empty()));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Probe side needs an additional local exchange for join: [0-9]*")
    public void testValidateFailsForJavaSpillEnabled()
    {
        validatePlan(
                p -> p.join(
                        INNER,
                        p.tableScan(nationTableHandle, ImmutableList.of(p.variable("nationkeyN", BIGINT)), ImmutableMap.of(p.variable("nationkeyN", BIGINT), nationColumnHandle)),
                        p.exchange(e -> e
                                .scope(ExchangeNode.Scope.LOCAL)
                                .type(ExchangeNode.Type.REPARTITION)
                                .addSource(p.tableScan(supplierTableHandle, ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)), ImmutableMap.of(p.variable("nationkeyS", BIGINT), nationColumnHandle, p.variable("suppkey", BIGINT), suppColumnHandle)))
                                .addInputsSet(ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)))
                                .fixedHashDistributionPartitioningScheme(ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)), ImmutableList.of(p.variable("nationkeyS", BIGINT)))),
                        ImmutableList.of(new EquiJoinClause(p.variable("nationkeyN", BIGINT), p.variable("nationkeyS", BIGINT))),
                        ImmutableList.of(p.variable("nationkeyN", BIGINT), p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)),
                        Optional.empty()),
                false,
                spillSession);
    }

    @Test
    public void testValidateSucceedsForNativeSpillEnabled()
    {
        validatePlan(
                p -> p.join(
                        INNER,
                        p.tableScan(nationTableHandle, ImmutableList.of(p.variable("nationkeyN", BIGINT)), ImmutableMap.of(p.variable("nationkeyN", BIGINT), nationColumnHandle)),
                        p.exchange(e -> e
                                .scope(ExchangeNode.Scope.LOCAL)
                                .type(ExchangeNode.Type.REPARTITION)
                                .addSource(p.tableScan(supplierTableHandle, ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)), ImmutableMap.of(p.variable("nationkeyS", BIGINT), nationColumnHandle, p.variable("suppkey", BIGINT), suppColumnHandle)))
                                .addInputsSet(ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)))
                                .fixedHashDistributionPartitioningScheme(ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)), ImmutableList.of(p.variable("nationkeyS", BIGINT)))),
                        ImmutableList.of(new EquiJoinClause(p.variable("nationkeyN", BIGINT), p.variable("nationkeyS", BIGINT))),
                        ImmutableList.of(p.variable("nationkeyN", BIGINT), p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)),
                        Optional.empty()),
                true,
                spillSession);
    }

    private void validatePlan(Function<PlanBuilder, PlanNode> planProvider)
    {
        validatePlan(planProvider, false, defaultSession);
    }

    private void validatePlan(Function<PlanBuilder, PlanNode> planProvider, boolean nativeExecutionEnabled, Session testSession)
    {
        PlanBuilder builder = new PlanBuilder(testSession, idAllocator, metadata);
        PlanNode planNode = planProvider.apply(builder);
        getQueryRunner().inTransaction(testSession, session -> {
            session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
            new ValidateStreamingJoins(new FeaturesConfig().setNativeExecutionEnabled(nativeExecutionEnabled)).validate(planNode, session, metadata, WarningCollector.NOOP);
            return null;
        });
    }
}
