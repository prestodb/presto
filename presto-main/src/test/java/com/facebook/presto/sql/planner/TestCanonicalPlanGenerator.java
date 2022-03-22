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
package com.facebook.presto.sql.planner;

import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.TestingBlockEncodingSerde;
import com.facebook.presto.common.block.TestingBlockJsonSerde;
import com.facebook.presto.common.type.TestingTypeDeserializer;
import com.facebook.presto.common.type.TestingTypeManager;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.CanonicalTableScanNode.CanonicalTableHandle;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.planner.CanonicalPlanGenerator.generateCanonicalPlan;
import static com.facebook.presto.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestCanonicalPlanGenerator
        extends BasePlanTest
{
    private final ObjectMapper objectMapper;

    public TestCanonicalPlanGenerator()
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();
        this.objectMapper = new JsonObjectMapperProvider().get()
                .registerModule(new SimpleModule()
                        .addDeserializer(Type.class, new TestingTypeDeserializer(typeManager))
                        .addSerializer(Block.class, new TestingBlockJsonSerde.Serializer(blockEncodingSerde))
                        .addDeserializer(Block.class, new TestingBlockJsonSerde.Deserializer(blockEncodingSerde)))
                .configure(ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    @Test
    public void testPartialAggregation()
            throws Exception
    {
        // Equal cases:
        assertSameCanonicalLeafSubPlan("SELECT avg(totalprice) FROM orders");
        assertSameCanonicalLeafSubPlan("SELECT avg(totalprice) FILTER (WHERE orderstatus != 'F') FROM orders");
        assertSameCanonicalLeafSubPlan("SELECT array_agg(totalprice ORDER BY totalprice) FROM orders");
        assertSameCanonicalLeafSubPlan("SELECT DISTINCT orderstatus FROM orders");
        assertSameCanonicalLeafSubPlan("SELECT count(DISTINCT orderstatus) FROM orders");

        // Test grouping sets
        assertSameCanonicalLeafSubPlan("SELECT orderstatus, sum(totalprice) FROM orders GROUP BY orderstatus");
        assertSameCanonicalLeafSubPlan("SELECT shippriority, custkey, sum(totalprice) FROM orders GROUP BY GROUPING SETS (shippriority), (shippriority, custkey)");
        assertSameCanonicalLeafSubPlan("SELECT shippriority, custkey, sum(totalprice) FROM orders GROUP BY GROUPING SETS (shippriority, custkey), (shippriority)");
        assertSameCanonicalLeafSubPlan("SELECT shippriority, custkey, sum(totalprice) FROM orders GROUP BY CUBE (shippriority, custkey)");
        assertSameCanonicalLeafSubPlan("SELECT shippriority, custkey, sum(totalprice) FROM orders GROUP BY ROLLUP (shippriority, custkey)");

        // Not equal cases:
        assertDifferentCanonicalLeafSubPlan(
                "SELECT avg(totalprice) FROM orders",
                "SELECT sum(totalprice) FROM orders");
        assertDifferentCanonicalLeafSubPlan(
                "SELECT avg(totalprice) FROM orders",
                "SELECT avg(shippriority) FROM orders");
        assertDifferentCanonicalLeafSubPlan(
                "SELECT count(orderkey) FROM orders",
                "SELECT count(orderkey) FROM lineitem");
        assertDifferentCanonicalLeafSubPlan(
                "SELECT avg(totalprice) FILTER (WHERE orderstatus != 'F') FROM orders",
                "SELECT avg(totalprice) FILTER (WHERE orderstatus != 'P') FROM orders");
        assertDifferentCanonicalLeafSubPlan(
                "SELECT array_agg(totalprice ORDER BY orderstatus) FROM orders",
                "SELECT array_agg(totalprice ORDER BY totalprice) FROM orders");
        assertDifferentCanonicalLeafSubPlan(
                "SELECT DISTINCT orderstatus FROM orders",
                "SELECT DISTINCT totalprice FROM orders");
        assertDifferentCanonicalLeafSubPlan(
                "SELECT count(DISTINCT orderstatus) FROM orders",
                "SELECT count(orderstatus) FROM orders");

        // Test grouping sets
        assertDifferentCanonicalLeafSubPlan(
                "SELECT orderstatus, sum(totalprice) FROM orders GROUP BY orderstatus",
                "SELECT shippriority, sum(totalprice) FROM orders GROUP BY shippriority");
        assertDifferentCanonicalLeafSubPlan(
                "SELECT shippriority, custkey, sum(totalprice) FROM orders GROUP BY GROUPING SETS (shippriority), (shippriority, custkey)",
                "SELECT shippriority, custkey, sum(totalprice) FROM orders GROUP BY GROUPING SETS (shippriority, custkey)");
        assertDifferentCanonicalLeafSubPlan(
                "SELECT shippriority, custkey, sum(totalprice) FROM orders GROUP BY GROUPING SETS (shippriority), (shippriority, custkey)",
                "SELECT shippriority, custkey, sum(totalprice) FROM orders GROUP BY GROUPING SETS (custkey), (shippriority, custkey)");
        assertDifferentCanonicalLeafSubPlan(
                "SELECT shippriority, custkey, sum(totalprice) FROM orders GROUP BY ROLLUP (shippriority, custkey)",
                "SELECT shippriority, custkey, sum(totalprice) FROM orders GROUP BY ROLLUP (custkey, shippriority)");
    }

    @Test
    public void testUnnest()
            throws Exception
    {
        assertSameCanonicalLeafSubPlan("" +
                "SELECT a.custkey, t.e " +
                "FROM (SELECT custkey, ARRAY[1, 2, 3] AS my_array FROM orders) a " +
                "CROSS JOIN UNNEST(my_array) AS t(e)");
        assertSameCanonicalLeafSubPlan("" +
                "SELECT * " +
                "FROM (SELECT custkey, ARRAY[1, 2, 3] AS my_array FROM orders) a " +
                "CROSS JOIN UNNEST(my_array) WITH ORDINALITY AS t(e, ord)");
    }

    @Test
    public void testProject()
            throws Exception
    {
        assertSameCanonicalLeafSubPlan("SELECT 1 + 2 FROM orders");
        assertSameCanonicalLeafSubPlan("SELECT totalprice / 2 FROM orders");
        assertSameCanonicalLeafSubPlan("SELECT custkey + orderkey FROM orders");

        assertDifferentCanonicalLeafSubPlan("SELECT totalprice / 2 FROM orders", "SELECT totalprice * 2 FROM orders");
        assertDifferentCanonicalLeafSubPlan("SELECT custkey + orderkey FROM orders", "SELECT custkey + shippriority FROM orders");
    }

    @Test
    public void testFilter()
            throws Exception
    {
        assertSameCanonicalLeafSubPlan("SELECT totalprice FROM orders WHERE orderkey < 100");

        assertDifferentCanonicalLeafSubPlan("SELECT totalprice FROM orders WHERE orderkey < 100", "SELECT totalprice FROM orders WHERE orderkey < 50");
        assertDifferentCanonicalLeafSubPlan("SELECT totalprice FROM orders WHERE orderkey < 100", "SELECT totalprice FROM orders WHERE custkey < 100");
        assertDifferentCanonicalLeafSubPlan("SELECT totalprice FROM orders", "SELECT totalprice FROM orders WHERE custkey < 100");
    }

    @Test
    public void testTableScan()
            throws Exception
    {
        assertSameCanonicalLeafSubPlan("SELECT totalprice FROM orders");
        assertSameCanonicalLeafSubPlan("SELECT orderkey, totalprice FROM orders");
        assertSameCanonicalLeafSubPlan("SELECT * FROM orders");
        assertSameCanonicalLeafSubPlan(
                "SELECT * FROM orders",
                "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment FROM orders");

        assertDifferentCanonicalLeafSubPlan("SELECT totalprice FROM orders", "SELECT orderkey, totalprice FROM orders");
        assertDifferentCanonicalLeafSubPlan("SELECT * FROM orders", "SELECT orderkey, totalprice FROM orders");
    }

    private static List<SubPlan> getLeafSubPlans(SubPlan subPlan)
    {
        if (subPlan.getChildren().isEmpty()) {
            return ImmutableList.of(subPlan);
        }
        return subPlan.getChildren().stream()
                .map(TestCanonicalPlanGenerator::getLeafSubPlans)
                .flatMap(List::stream)
                .collect(toImmutableList());
    }

    private void assertSameCanonicalLeafSubPlan(String sql)
            throws Exception
    {
        assertSameCanonicalLeafSubPlan(sql, sql);
    }

    // This helper method would check if the provided sql could generate the same leaf canonical plan fragment when it appears
    // at two sides of UNION ALL. The provided sql should only contain queries that don't have subplan fanout like JOIN.
    private void assertSameCanonicalLeafSubPlan(String sql1, String sql2)
            throws Exception
    {
        SubPlan subplan = subplan(
                format("( %s ) UNION ALL ( %s )", sql1, sql2),
                OPTIMIZED_AND_VALIDATED,
                false);
        List<CanonicalPlanFragment> leafCanonicalPlans = getLeafSubPlans(subplan).stream()
                .map(SubPlan::getFragment)
                .map(fragment -> generateCanonicalPlan(fragment.getRoot(), fragment.getPartitioningScheme()))
                .map(Optional::get)
                .collect(Collectors.toList());
        assertEquals(leafCanonicalPlans.size(), 2);
        assertEquals(leafCanonicalPlans.get(0), leafCanonicalPlans.get(1));
        assertEquals(objectMapper.writeValueAsString(leafCanonicalPlans.get(0)), objectMapper.writeValueAsString(leafCanonicalPlans.get(1)));
    }

    private void assertDifferentCanonicalLeafSubPlan(String sql1, String sql2)
            throws Exception
    {
        PlanFragment fragment1 = getOnlyElement(getLeafSubPlans(subplan(sql1, OPTIMIZED_AND_VALIDATED, false))).getFragment();
        PlanFragment fragment2 = getOnlyElement(getLeafSubPlans(subplan(sql2, OPTIMIZED_AND_VALIDATED, false))).getFragment();
        Optional<CanonicalPlanFragment> canonicalPlan1 = generateCanonicalPlan(fragment1.getRoot(), fragment1.getPartitioningScheme());
        Optional<CanonicalPlanFragment> canonicalPlan2 = generateCanonicalPlan(fragment2.getRoot(), fragment2.getPartitioningScheme());
        assertTrue(canonicalPlan1.isPresent());
        assertTrue(canonicalPlan2.isPresent());
        assertNotEquals(objectMapper.writeValueAsString(canonicalPlan1), objectMapper.writeValueAsString(canonicalPlan2));
    }

    // We add the following field test to make sure corresponding canonical class is still correct.
    // If new fields are added to these classes, please verify if they should be added to canonical class.
    //   - If the newly added field could change the output of an operator, please also add these fields to canonical class
    //   - Otherwise (for example, feature flag or transaction related), we don't have to add the fields to canonical class
    // Then, change the tests accordingly
    @Test
    public void testCanonicalPartitioningScheme()
    {
        assertEquals(
                Arrays.stream(PartitioningScheme.class.getDeclaredFields())
                        .map(Field::getName)
                        .collect(toImmutableSet()),
                ImmutableSet.of("partitioning", "outputLayout", "hashColumn", "replicateNullsAndAny", "bucketToPartition"));
        assertEquals(
                Arrays.stream(Partitioning.class.getDeclaredFields())
                        .map(Field::getName)
                        .collect(toImmutableSet()),
                ImmutableSet.of("handle", "arguments"));
        assertEquals(
                Arrays.stream(PartitioningHandle.class.getDeclaredFields())
                        .map(Field::getName)
                        .collect(toImmutableSet()),
                ImmutableSet.of("connectorId", "transactionHandle", "connectorHandle"));
        assertEquals(
                Arrays.stream(CanonicalPartitioningScheme.class.getDeclaredFields())
                        .map(Field::getName)
                        .collect(toImmutableSet()),
                ImmutableSet.of("connectorId", "connectorHandle", "arguments", "outputLayout"));
    }

    @Test
    public void testCanonicalTableScanNodeField()
    {
        assertEquals(
                Arrays.stream(TableScanNode.class.getDeclaredFields())
                        .map(Field::getName)
                        .collect(toImmutableSet()),
                ImmutableSet.of("table", "assignments", "outputVariables", "currentConstraint", "enforcedConstraint"));
        assertEquals(
                Arrays.stream(CanonicalTableScanNode.class.getDeclaredFields())
                        .map(Field::getName)
                        .collect(toImmutableSet()),
                ImmutableSet.of("table", "assignments", "outputVariables"));

        assertEquals(
                Arrays.stream(TableHandle.class.getDeclaredFields())
                        .map(Field::getName)
                        .collect(toImmutableSet()),
                ImmutableSet.of("connectorId", "connectorHandle", "transaction", "layout", "dynamicFilter"));
        assertEquals(
                Arrays.stream(CanonicalTableHandle.class.getDeclaredFields())
                        .map(Field::getName)
                        .collect(toImmutableSet()),
                ImmutableSet.of("connectorId", "tableHandle", "layoutIdentifier", "layoutHandle"));
    }
}
