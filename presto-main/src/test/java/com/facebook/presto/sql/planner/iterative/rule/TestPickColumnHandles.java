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
import com.facebook.presto.metadata.AbstractMockMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tpch.TpchTableLayoutHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.predicate.TupleDomain.all;
import static com.facebook.presto.spi.predicate.TupleDomain.none;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.RowType.field;
import static com.facebook.presto.spi.type.RowType.from;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCALE_FACTOR;
import static com.facebook.presto.tpch.TpchTransactionHandle.INSTANCE;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class TestPickColumnHandles
        extends BaseRuleTest
{
    private static TpchTableHandle tableHandle = new TpchTableHandle("local", "orders", TINY_SCALE_FACTOR);
    private static TpchTableLayoutHandle tableLayoutHandle = new TpchTableLayoutHandle(tableHandle, all());
    private static ConnectorId local = new ConnectorId("local");
    private static TableLayoutHandle tableLayout = new TableLayoutHandle(local, INSTANCE, tableLayoutHandle);
    private static Type columnType = from(ImmutableList.of(field("x", BIGINT), field("y", BIGINT)));
    private static TableHandle table = new TableHandle(local, tableHandle);

    @Test
    public void testColumnReplacedWithNewHandle()
    {
        List<ColumnHandle> columns = ImmutableList.of(new TpchColumnHandle("totalprice_new", BIGINT));
        Metadata mockMetadata = new MockMetadata(ImmutableList.of(
                new TableLayoutResult(new TableLayout(tableLayout, getConnectorTableLayout(columns)), none())));
        tester().assertThat(new PickColumnHandles(mockMetadata))
                .on(p -> {
                    Symbol fakeTotalpriceWithFields = p.symbol("totalprice", columnType, ImmutableSet.of("y"));
                    return p.tableScan(
                            table,
                            ImmutableList.of(fakeTotalpriceWithFields),
                            ImmutableMap.of(fakeTotalpriceWithFields, new TpchColumnHandle("totalprice", columnType)));
                })
                .matches(tableScan("orders", columns)
                        .withExactAssignedOutputs(expression(new SymbolReference("totalprice", ImmutableSet.of("y")))));
    }

    @Test
    public void testMetadataNotGiveNewHandle()
    {
        tester().assertThat(new PickColumnHandles(tester().getMetadata()))
                .on(p -> {
                    Symbol fakeTotalpriceWithFields = p.symbol("totalprice", columnType, ImmutableSet.of("y"));
                    return p.tableScan(
                            table,
                            ImmutableList.of(fakeTotalpriceWithFields),
                            ImmutableMap.of(fakeTotalpriceWithFields, new TpchColumnHandle("totalprice", columnType)));
                })
                .doesNotFire();
    }

    @Test
    public void testAllNestedFieldReferenced()
    {
        tester().assertThat(new PickColumnHandles(tester().getMetadata()))
                .on(p -> {
                    Symbol fakeTotalprice = p.symbol("totalprice", columnType);
                    return p.tableScan(
                            table,
                            ImmutableList.of(fakeTotalprice),
                            ImmutableMap.of(fakeTotalprice, new TpchColumnHandle("totalprice", columnType)));
                })
                .doesNotFire();
    }

    private static class MockMetadata
            extends AbstractMockMetadata
    {
        private final List<TableLayoutResult> expectedLayouts;

        public MockMetadata(List<TableLayoutResult> expectedLayouts)
        {
            this.expectedLayouts = requireNonNull(expectedLayouts, "expectedLayouts is null");
        }

        @Override
        public List<TableLayoutResult> getLayouts(Session session, TableHandle tableHandle, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
        {
            return expectedLayouts;
        }
    }

    private static ConnectorTableLayout getConnectorTableLayout(List<ColumnHandle> columns)
    {
        return new ConnectorTableLayout(tableLayoutHandle,
                Optional.of(columns),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                emptyList());
    }
}
