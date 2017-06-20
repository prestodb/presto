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

package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.constrainedTableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCALE_FACTOR;
import static io.airlift.slice.Slices.utf8Slice;

public class TestPushDownTableConstraints
{
    @Test
    public void test()
    {
        Session session = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("task_concurrency", "1") // these tests don't handle exchanges from local parallel
                .build();

        LocalQueryRunner queryRunner = new LocalQueryRunner(session);
        queryRunner.createCatalog(
                session.getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());

        Map<String, Domain> tableScanConstraint = ImmutableMap.<String, Domain>builder()
                .put("orderstatus", Domain.singleValue(createVarcharType(1), utf8Slice("P")))
                .build();

        new RuleTester(queryRunner).assertThat(new PushDownTableConstraints(queryRunner.getMetadata()))
                .on(p ->
                        p.filter(expression("orderstatus = 'P'"),
                                p.tableScan(
                                        new TableHandle(
                                                new ConnectorId("local"),
                                                new TpchTableHandle("local", "orders", TINY_SCALE_FACTOR)),
                                        ImmutableList.of(p.symbol("orderstatus", createVarcharType(1))),
                                        ImmutableMap.of(p.symbol("orderstatus", createVarcharType(1)), new TpchColumnHandle("orderstatus", createVarcharType(1)))
                                )))
                .matches(filter("orderstatus = 'P'",
                        constrainedTableScan("orders", tableScanConstraint, ImmutableMap.of("orderstatus", "orderstatus"))));
    }
}
