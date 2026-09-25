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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.hive.HiveColumnConverterProvider;
import com.facebook.presto.spi.WarningCollector;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.hive.metastore.InMemoryCachingHiveMetastore.memoizeMetastore;
import static org.testng.Assert.assertEquals;

public class TestPerTransactionHiveMetastoreCaching
{
    @Test
    public void testRepeatedLookupsWithinTransactionHitMetastoreOnce()
    {
        CountingHiveMetastore counting = new CountingHiveMetastore();
        ExtendedHiveMetastore transactionMetastore = memoizeMetastore(counting, false, 1000, 0);
        MetastoreContext context = createMetastoreContext();

        transactionMetastore.getTable(context, "test_schema", "test_table");
        transactionMetastore.getTable(context, "test_schema", "test_table");
        transactionMetastore.getTable(context, "test_schema", "test_table");

        assertEquals(counting.getTableCallCount(), 1);
    }

    @Test
    public void testSeparateTransactionsDoNotShareCache()
    {
        CountingHiveMetastore counting = new CountingHiveMetastore();
        ExtendedHiveMetastore txn1 = memoizeMetastore(counting, false, 1000, 0);
        ExtendedHiveMetastore txn2 = memoizeMetastore(counting, false, 1000, 0);
        MetastoreContext context = createMetastoreContext();

        txn1.getTable(context, "test_schema", "test_table");
        assertEquals(counting.getTableCallCount(), 1);

        // Second transaction has its own fresh cache; must go to the underlying metastore
        txn2.getTable(context, "test_schema", "test_table");
        assertEquals(counting.getTableCallCount(), 2);
    }

    @Test
    public void testWriteOperationsInvalidateCache()
    {
        CountingHiveMetastore counting = new CountingHiveMetastore();
        ExtendedHiveMetastore transactionMetastore = memoizeMetastore(counting, false, 1000, 0);
        MetastoreContext context = createMetastoreContext();

        // First lookup populates the cache
        transactionMetastore.getTable(context, "test_schema", "test_table");
        assertEquals(counting.getTableCallCount(), 1);

        // Repeated lookup is served from cache
        transactionMetastore.getTable(context, "test_schema", "test_table");
        assertEquals(counting.getTableCallCount(), 1);

        // Write path (dropTable) invalidates the cached entry via AbstractCachingHiveMetastore
        transactionMetastore.dropTable(context, "test_schema", "test_table", false);

        // Lookup after write must hit the underlying metastore again
        transactionMetastore.getTable(context, "test_schema", "test_table");
        assertEquals(counting.getTableCallCount(), 2);
    }

    private static MetastoreContext createMetastoreContext()
    {
        return new MetastoreContext(
                "test_user",
                "test_query_id",
                Optional.empty(),
                ImmutableSet.of(),
                Optional.empty(),
                Optional.empty(),
                false,
                HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER,
                WarningCollector.NOOP,
                new RuntimeStats());
    }

    private static final class CountingHiveMetastore
            extends UnimplementedHiveMetastore
    {
        private final AtomicInteger getTableCallCount = new AtomicInteger();

        public int getTableCallCount()
        {
            return getTableCallCount.get();
        }

        @Override
        public Optional<Table> getTable(MetastoreContext metastoreContext, String databaseName, String tableName)
        {
            getTableCallCount.incrementAndGet();
            return Optional.empty();
        }

        @Override
        public void dropTable(MetastoreContext metastoreContext, String databaseName, String tableName, boolean deleteData)
        {
            // no-op to allow cache-invalidation path through AbstractCachingHiveMetastore.dropTable
        }
    }
}
