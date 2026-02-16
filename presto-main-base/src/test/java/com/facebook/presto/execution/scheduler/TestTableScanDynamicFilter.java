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
package com.facebook.presto.execution.scheduler;

import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.connector.DynamicFilter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTableScanDynamicFilter
{
    private static final Duration DEFAULT_TIMEOUT = new Duration(2, TimeUnit.SECONDS);

    @Test
    public void testSingleFilterComposite()
    {
        JoinDynamicFilter filter = createFilter("549", "customer_id");
        filter.setExpectedPartitions(1);

        TestColumnHandle customerIdHandle = new TestColumnHandle("customer_id");
        Map<String, ColumnHandle> columnNameToHandle = ImmutableMap.of("customer_id", customerIdHandle);
        TableScanDynamicFilter composite = new TableScanDynamicFilter(ImmutableList.of(filter), columnNameToHandle);

        assertFalse(composite.isComplete());

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 1L))));

        assertTrue(composite.isComplete());
        assertEquals(
                composite.getCurrentPredicate(),
                TupleDomain.withColumnDomains(
                        ImmutableMap.of((ColumnHandle) customerIdHandle, Domain.singleValue(INTEGER, 1L))));
    }

    @Test
    public void testTwoFiltersIntersection()
    {
        JoinDynamicFilter filter1 = createFilter("549", "customer_id");
        JoinDynamicFilter filter2 = createFilter("550", "product_id");
        filter1.setExpectedPartitions(1);
        filter2.setExpectedPartitions(1);

        TestColumnHandle customerIdHandle = new TestColumnHandle("customer_id");
        TestColumnHandle productIdHandle = new TestColumnHandle("product_id");
        Map<String, ColumnHandle> columnNameToHandle = ImmutableMap.of(
                "customer_id", customerIdHandle,
                "product_id", productIdHandle);
        TableScanDynamicFilter composite = new TableScanDynamicFilter(ImmutableList.of(filter1, filter2), columnNameToHandle);

        assertFalse(composite.isComplete());

        filter1.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)))));

        assertFalse(composite.isComplete());
        assertEquals(
                composite.getCurrentPredicate(),
                TupleDomain.withColumnDomains(
                        ImmutableMap.of((ColumnHandle) customerIdHandle, Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)))));

        filter2.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("550", Domain.multipleValues(INTEGER, ImmutableList.of(10L, 11L)))));

        assertTrue(composite.isComplete());
        assertEquals(
                composite.getCurrentPredicate(),
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                (ColumnHandle) customerIdHandle, Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)),
                                (ColumnHandle) productIdHandle, Domain.multipleValues(INTEGER, ImmutableList.of(10L, 11L)))));
    }

    @Test
    public void testProgressiveResolution()
    {
        JoinDynamicFilter filter1 = createFilter("797", "customer_id");
        JoinDynamicFilter filter2 = createFilter("798", "product_id");
        JoinDynamicFilter filter3 = createFilter("799", "region_id");
        filter1.setExpectedPartitions(1);
        filter2.setExpectedPartitions(1);
        filter3.setExpectedPartitions(1);

        TestColumnHandle customerIdHandle = new TestColumnHandle("customer_id");
        TestColumnHandle productIdHandle = new TestColumnHandle("product_id");
        TestColumnHandle regionIdHandle = new TestColumnHandle("region_id");
        Map<String, ColumnHandle> columnNameToHandle = ImmutableMap.of(
                "customer_id", customerIdHandle,
                "product_id", productIdHandle,
                "region_id", regionIdHandle);
        TableScanDynamicFilter composite = new TableScanDynamicFilter(ImmutableList.of(filter1, filter2, filter3), columnNameToHandle);

        assertEquals(composite.getCurrentPredicate(), TupleDomain.all());
        assertFalse(composite.isComplete());

        filter1.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("797", Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)))));

        assertEquals(
                composite.getCurrentPredicate(),
                TupleDomain.withColumnDomains(
                        ImmutableMap.of((ColumnHandle) customerIdHandle, Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)))));
        assertFalse(composite.isComplete());

        filter2.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("798", Domain.multipleValues(INTEGER, ImmutableList.of(10L, 11L)))));

        assertEquals(
                composite.getCurrentPredicate(),
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                (ColumnHandle) customerIdHandle, Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)),
                                (ColumnHandle) productIdHandle, Domain.multipleValues(INTEGER, ImmutableList.of(10L, 11L)))));
        assertFalse(composite.isComplete());

        filter3.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("799", Domain.singleValue(INTEGER, 100L))));

        assertEquals(
                composite.getCurrentPredicate(),
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                (ColumnHandle) customerIdHandle, Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)),
                                (ColumnHandle) productIdHandle, Domain.multipleValues(INTEGER, ImmutableList.of(10L, 11L)),
                                (ColumnHandle) regionIdHandle, Domain.singleValue(INTEGER, 100L))));
        assertTrue(composite.isComplete());
    }

    @Test
    public void testIsCompleteRequiresAllFilters()
    {
        JoinDynamicFilter filter1 = createFilter("549", "customer_id");
        JoinDynamicFilter filter2 = createFilter("550", "product_id");
        filter1.setExpectedPartitions(1);
        filter2.setExpectedPartitions(2); // Needs 2 partitions

        Map<String, ColumnHandle> columnNameToHandle = ImmutableMap.of(
                "customer_id", new TestColumnHandle("customer_id"),
                "product_id", new TestColumnHandle("product_id"));
        TableScanDynamicFilter composite = new TableScanDynamicFilter(ImmutableList.of(filter1, filter2), columnNameToHandle);

        assertFalse(composite.isComplete());

        filter1.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 1L))));
        assertFalse(composite.isComplete());

        filter2.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("550", Domain.singleValue(INTEGER, 10L))));
        assertFalse(composite.isComplete());

        filter2.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("550", Domain.singleValue(INTEGER, 20L))));
        assertTrue(composite.isComplete());
    }

    @Test
    public void testOneFilterTimesOut()
            throws Exception
    {
        JoinDynamicFilter filter1 = createFilter("549", "customer_id", new Duration(100, TimeUnit.MILLISECONDS));
        JoinDynamicFilter filter2 = createFilter("550", "product_id", DEFAULT_TIMEOUT);
        filter1.setExpectedPartitions(2); // Won't get all partitions
        filter2.setExpectedPartitions(1);

        TestColumnHandle productIdHandle = new TestColumnHandle("product_id");
        Map<String, ColumnHandle> columnNameToHandle = ImmutableMap.of(
                "customer_id", new TestColumnHandle("customer_id"),
                "product_id", productIdHandle);
        TableScanDynamicFilter composite = new TableScanDynamicFilter(ImmutableList.of(filter1, filter2), columnNameToHandle);

        filter1.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 1L))));

        filter2.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("550", Domain.singleValue(INTEGER, 10L))));

        composite.isBlocked();
        Thread.sleep(300);

        assertFalse(filter1.isComplete());
        assertTrue(filter2.isComplete());
        assertFalse(composite.isComplete());

        assertEquals(
                composite.getCurrentPredicate(),
                TupleDomain.withColumnDomains(
                        ImmutableMap.of((ColumnHandle) productIdHandle, Domain.singleValue(INTEGER, 10L))));
    }

    @Test
    public void testMinimumTimeout()
    {
        JoinDynamicFilter filter1 = createFilter("549", "customer_id", new Duration(1000, TimeUnit.MILLISECONDS));
        JoinDynamicFilter filter2 = createFilter("550", "product_id", new Duration(500, TimeUnit.MILLISECONDS));
        JoinDynamicFilter filter3 = createFilter("551", "region_id", new Duration(2000, TimeUnit.MILLISECONDS));
        filter1.setExpectedPartitions(1);
        filter2.setExpectedPartitions(1);
        filter3.setExpectedPartitions(1);

        Map<String, ColumnHandle> columnNameToHandle = ImmutableMap.of(
                "customer_id", new TestColumnHandle("customer_id"),
                "product_id", new TestColumnHandle("product_id"),
                "region_id", new TestColumnHandle("region_id"));
        TableScanDynamicFilter composite = new TableScanDynamicFilter(ImmutableList.of(filter1, filter2, filter3), columnNameToHandle);

        assertEquals(composite.getWaitTimeout(), new Duration(500, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testGetFilterIdConcatenated()
    {
        JoinDynamicFilter filter1 = createFilter("549", "customer_id");
        JoinDynamicFilter filter2 = createFilter("550", "product_id");
        JoinDynamicFilter filter3 = createFilter("551", "region_id");
        filter1.setExpectedPartitions(1);
        filter2.setExpectedPartitions(1);
        filter3.setExpectedPartitions(1);

        Map<String, ColumnHandle> columnNameToHandle = ImmutableMap.of(
                "customer_id", new TestColumnHandle("customer_id"),
                "product_id", new TestColumnHandle("product_id"),
                "region_id", new TestColumnHandle("region_id"));
        TableScanDynamicFilter composite = new TableScanDynamicFilter(ImmutableList.of(filter1, filter2, filter3), columnNameToHandle);

        assertEquals(composite.getFilterId(), "549,550,551");
    }

    @Test
    public void testIsBlockedFuture()
    {
        JoinDynamicFilter filter1 = createFilter("549", "customer_id");
        JoinDynamicFilter filter2 = createFilter("550", "product_id");
        filter1.setExpectedPartitions(1);
        filter2.setExpectedPartitions(1);

        TestColumnHandle customerIdHandle = new TestColumnHandle("customer_id");
        TestColumnHandle productIdHandle = new TestColumnHandle("product_id");
        Map<String, ColumnHandle> columnNameToHandle = ImmutableMap.of(
                "customer_id", customerIdHandle,
                "product_id", productIdHandle);
        TableScanDynamicFilter composite = new TableScanDynamicFilter(ImmutableList.of(filter1, filter2), columnNameToHandle);

        CompletableFuture<?> blocked = composite.isBlocked();
        assertFalse(blocked.isDone());

        filter1.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 1L))));
        filter2.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("550", Domain.singleValue(INTEGER, 10L))));

        assertTrue(blocked.isDone());
        assertTrue(composite.isComplete());
        assertEquals(composite.isBlocked(), DynamicFilter.NOT_BLOCKED);
        assertEquals(
                composite.getCurrentPredicate(),
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                (ColumnHandle) customerIdHandle, Domain.singleValue(INTEGER, 1L),
                                (ColumnHandle) productIdHandle, Domain.singleValue(INTEGER, 10L))));
    }

    @Test
    public void testEmptyResultAfterIntersection()
    {
        TestColumnHandle colHandle = new TestColumnHandle("col");
        JoinDynamicFilter filter1 = createFilter("549", "col");
        JoinDynamicFilter filter2 = createFilter("550", "col");
        filter1.setExpectedPartitions(1);
        filter2.setExpectedPartitions(1);

        Map<String, ColumnHandle> columnNameToHandle = ImmutableMap.of("col", colHandle);
        TableScanDynamicFilter composite = new TableScanDynamicFilter(ImmutableList.of(filter1, filter2), columnNameToHandle);

        filter1.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)))));

        filter2.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("550", Domain.multipleValues(INTEGER, ImmutableList.of(4L, 5L, 6L)))));

        assertTrue(composite.isComplete());

        TupleDomain<ColumnHandle> result = composite.getCurrentPredicate();
        assertTrue(result.isNone(), "Mutually exclusive constraints should produce none()");
    }

    @Test
    public void testGetFiltersReturnsUnderlyingFilters()
    {
        JoinDynamicFilter filter1 = createFilter("549", "customer_id");
        JoinDynamicFilter filter2 = createFilter("550", "product_id");
        filter1.setExpectedPartitions(1);
        filter2.setExpectedPartitions(1);

        Map<String, ColumnHandle> columnNameToHandle = ImmutableMap.of(
                "customer_id", new TestColumnHandle("customer_id"),
                "product_id", new TestColumnHandle("product_id"));
        TableScanDynamicFilter composite = new TableScanDynamicFilter(ImmutableList.of(filter1, filter2), columnNameToHandle);

        assertEquals(composite.getFilters(), ImmutableList.of(filter1, filter2));
    }

    @Test
    public void testIsBlockedStartsTimeoutForAllFilters()
    {
        JoinDynamicFilter filter1 = createFilter("549", "customer_id", new Duration(100, TimeUnit.MILLISECONDS));
        JoinDynamicFilter filter2 = createFilter("550", "product_id", new Duration(100, TimeUnit.MILLISECONDS));
        filter1.setExpectedPartitions(2);
        filter2.setExpectedPartitions(2);

        Map<String, ColumnHandle> columnNameToHandle = ImmutableMap.of(
                "customer_id", new TestColumnHandle("customer_id"),
                "product_id", new TestColumnHandle("product_id"));
        TableScanDynamicFilter composite = new TableScanDynamicFilter(ImmutableList.of(filter1, filter2), columnNameToHandle);

        CompletableFuture<?> blocked = composite.isBlocked();
        assertFalse(blocked.isDone());
    }

    @Test
    public void testIsBlockedProgressiveWakeup()
    {
        JoinDynamicFilter filter1 = createFilter("549", "customer_id");
        JoinDynamicFilter filter2 = createFilter("550", "product_id");
        filter1.setExpectedPartitions(1);
        filter2.setExpectedPartitions(1);

        TestColumnHandle customerIdHandle = new TestColumnHandle("customer_id");
        TestColumnHandle productIdHandle = new TestColumnHandle("product_id");
        Map<String, ColumnHandle> columnNameToHandle = ImmutableMap.of(
                "customer_id", customerIdHandle,
                "product_id", productIdHandle);
        TableScanDynamicFilter composite = new TableScanDynamicFilter(ImmutableList.of(filter1, filter2), columnNameToHandle);

        CompletableFuture<?> blocked1 = composite.isBlocked();
        assertFalse(blocked1.isDone());
        assertFalse(composite.isComplete());

        filter1.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 1L))));
        assertTrue(blocked1.isDone());
        assertFalse(composite.isComplete());

        assertEquals(
                composite.getCurrentPredicate(),
                TupleDomain.withColumnDomains(
                        ImmutableMap.of((ColumnHandle) customerIdHandle, Domain.singleValue(INTEGER, 1L))));

        CompletableFuture<?> blocked2 = composite.isBlocked();
        assertFalse(blocked2.isDone());

        filter2.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("550", Domain.singleValue(INTEGER, 10L))));
        assertTrue(blocked2.isDone());
        assertTrue(composite.isComplete());

        assertEquals(composite.isBlocked(), DynamicFilter.NOT_BLOCKED);
        assertEquals(
                composite.getCurrentPredicate(),
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                (ColumnHandle) customerIdHandle, Domain.singleValue(INTEGER, 1L),
                                (ColumnHandle) productIdHandle, Domain.singleValue(INTEGER, 10L))));
    }

    private JoinDynamicFilter createFilter(String filterId, String columnName)
    {
        return createFilter(filterId, columnName, DEFAULT_TIMEOUT);
    }

    private JoinDynamicFilter createFilter(String filterId, String columnName, Duration timeout)
    {
        return new JoinDynamicFilter(
                filterId,
                columnName,
                timeout,
                new DynamicFilterStats(),
                Optional.of(new RuntimeStats()));
    }

    private static class TestColumnHandle
            implements ColumnHandle
    {
        private final String name;

        public TestColumnHandle(String name)
        {
            this.name = name;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestColumnHandle that = (TestColumnHandle) o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name);
        }

        @Override
        public String toString()
        {
            return "TestColumnHandle{name='" + name + "'}";
        }
    }
}
