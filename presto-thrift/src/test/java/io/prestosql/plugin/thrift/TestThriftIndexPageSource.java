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
package io.prestosql.plugin.thrift;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.prestosql.plugin.thrift.api.PrestoThriftId;
import io.prestosql.plugin.thrift.api.PrestoThriftNullableColumnSet;
import io.prestosql.plugin.thrift.api.PrestoThriftNullableSchemaName;
import io.prestosql.plugin.thrift.api.PrestoThriftNullableTableMetadata;
import io.prestosql.plugin.thrift.api.PrestoThriftNullableToken;
import io.prestosql.plugin.thrift.api.PrestoThriftPageResult;
import io.prestosql.plugin.thrift.api.PrestoThriftSchemaTableName;
import io.prestosql.plugin.thrift.api.PrestoThriftService;
import io.prestosql.plugin.thrift.api.PrestoThriftServiceException;
import io.prestosql.plugin.thrift.api.PrestoThriftSplit;
import io.prestosql.plugin.thrift.api.PrestoThriftSplitBatch;
import io.prestosql.plugin.thrift.api.PrestoThriftTupleDomain;
import io.prestosql.plugin.thrift.api.datatypes.PrestoThriftInteger;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.InMemoryRecordSet;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.plugin.thrift.api.PrestoThriftBlock.integerData;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static java.util.Collections.shuffle;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestThriftIndexPageSource
{
    private static final long MAX_BYTES_PER_RESPONSE = 16_000_000;

    @Test
    public void testGetNextPageTwoConcurrentRequests()
            throws Exception
    {
        final int splits = 3;
        final int lookupRequestsConcurrency = 2;
        final int rowsPerSplit = 1;
        List<SettableFuture<PrestoThriftPageResult>> futures = IntStream.range(0, splits)
                .mapToObj(i -> SettableFuture.<PrestoThriftPageResult>create())
                .collect(toImmutableList());
        List<CountDownLatch> signals = IntStream.range(0, splits)
                .mapToObj(i -> new CountDownLatch(1))
                .collect(toImmutableList());
        TestingThriftService client = new TestingThriftService(rowsPerSplit, false, false)
        {
            @Override
            public ListenableFuture<PrestoThriftPageResult> getRows(PrestoThriftId splitId, List<String> columns, long maxBytes, PrestoThriftNullableToken nextToken)
            {
                int key = Ints.fromByteArray(splitId.getId());
                signals.get(key).countDown();
                return futures.get(key);
            }
        };
        ThriftConnectorStats stats = new ThriftConnectorStats();
        long pageSizeReceived = 0;
        ThriftIndexPageSource pageSource = new ThriftIndexPageSource(
                (context, headers) -> client,
                ImmutableMap.of(),
                stats,
                new ThriftIndexHandle(new SchemaTableName("default", "table1"), TupleDomain.all()),
                ImmutableList.of(column("a", INTEGER)),
                ImmutableList.of(column("b", INTEGER)),
                new InMemoryRecordSet(ImmutableList.of(INTEGER), generateKeys(0, splits)),
                MAX_BYTES_PER_RESPONSE,
                lookupRequestsConcurrency);

        assertNull(pageSource.getNextPage());
        assertEquals((long) stats.getIndexPageSize().getAllTime().getTotal(), 0);
        signals.get(0).await(1, SECONDS);
        signals.get(1).await(1, SECONDS);
        signals.get(2).await(1, SECONDS);
        assertEquals(signals.get(0).getCount(), 0, "first request wasn't sent");
        assertEquals(signals.get(1).getCount(), 0, "second request wasn't sent");
        assertEquals(signals.get(2).getCount(), 1, "third request shouldn't be sent");

        // at this point first two requests were sent
        assertFalse(pageSource.isFinished());
        assertNull(pageSource.getNextPage());
        assertEquals((long) stats.getIndexPageSize().getAllTime().getTotal(), 0);

        // completing the second request
        futures.get(1).set(pageResult(20, null));
        Page page = pageSource.getNextPage();
        pageSizeReceived += page.getSizeInBytes();
        assertEquals((long) stats.getIndexPageSize().getAllTime().getTotal(), pageSizeReceived);
        assertNotNull(page);
        assertEquals(page.getPositionCount(), 1);
        assertEquals(page.getBlock(0).getInt(0, 0), 20);
        // not complete yet
        assertFalse(pageSource.isFinished());

        // once one of the requests completes the next one should be sent
        signals.get(2).await(1, SECONDS);
        assertEquals(signals.get(2).getCount(), 0, "third request wasn't sent");

        // completing the first request
        futures.get(0).set(pageResult(10, null));
        page = pageSource.getNextPage();
        assertNotNull(page);
        pageSizeReceived += page.getSizeInBytes();
        assertEquals((long) stats.getIndexPageSize().getAllTime().getTotal(), pageSizeReceived);
        assertEquals(page.getPositionCount(), 1);
        assertEquals(page.getBlock(0).getInt(0, 0), 10);
        // still not complete
        assertFalse(pageSource.isFinished());

        // completing the third request
        futures.get(2).set(pageResult(30, null));
        page = pageSource.getNextPage();
        assertNotNull(page);
        pageSizeReceived += page.getSizeInBytes();
        assertEquals((long) stats.getIndexPageSize().getAllTime().getTotal(), pageSizeReceived);
        assertEquals(page.getPositionCount(), 1);
        assertEquals(page.getBlock(0).getInt(0, 0), 30);
        // finished now
        assertTrue(pageSource.isFinished());

        // after completion
        assertNull(pageSource.getNextPage());
        pageSource.close();
    }

    @Test
    public void testGetNextPageMultipleSplitRequest()
            throws Exception
    {
        runGeneralTest(5, 2, 2, true);
    }

    @Test
    public void testGetNextPageNoSplits()
            throws Exception
    {
        runGeneralTest(0, 2, 2, false);
    }

    @Test
    public void testGetNextPageOneConcurrentRequest()
            throws Exception
    {
        runGeneralTest(3, 1, 3, false);
    }

    @Test
    public void testGetNextPageMoreConcurrencyThanRequestsNoContinuation()
            throws Exception
    {
        runGeneralTest(2, 4, 1, false);
    }

    private static void runGeneralTest(int splits, int lookupRequestsConcurrency, int rowsPerSplit, boolean twoSplitBatches)
            throws Exception
    {
        TestingThriftService client = new TestingThriftService(rowsPerSplit, true, twoSplitBatches);
        ThriftIndexPageSource pageSource = new ThriftIndexPageSource(
                (context, headers) -> client,
                ImmutableMap.of(),
                new ThriftConnectorStats(),
                new ThriftIndexHandle(new SchemaTableName("default", "table1"), TupleDomain.all()),
                ImmutableList.of(column("a", INTEGER)),
                ImmutableList.of(column("b", INTEGER)),
                new InMemoryRecordSet(ImmutableList.of(INTEGER), generateKeys(1, splits + 1)),
                MAX_BYTES_PER_RESPONSE,
                lookupRequestsConcurrency);

        List<Integer> actual = new ArrayList<>();
        while (!pageSource.isFinished()) {
            CompletableFuture<?> blocked = pageSource.isBlocked();
            blocked.get(1, SECONDS);
            Page page = pageSource.getNextPage();
            if (page != null) {
                Block block = page.getBlock(0);
                for (int position = 0; position < block.getPositionCount(); position++) {
                    actual.add(block.getInt(position, 0));
                }
            }
        }

        Collections.sort(actual);
        List<Integer> expected = new ArrayList<>(splits * rowsPerSplit);
        for (int split = 1; split <= splits; split++) {
            for (int row = 0; row < rowsPerSplit; row++) {
                expected.add(split * 10 + row);
            }
        }
        assertEquals(actual, expected);

        // must be null after finish
        assertNull(pageSource.getNextPage());

        pageSource.close();
    }

    private static class TestingThriftService
            implements PrestoThriftService
    {
        private final int rowsPerSplit;
        private final boolean shuffleSplits;
        private final boolean twoSplitBatches;

        public TestingThriftService(int rowsPerSplit, boolean shuffleSplits, boolean twoSplitBatches)
        {
            this.rowsPerSplit = rowsPerSplit;
            this.shuffleSplits = shuffleSplits;
            this.twoSplitBatches = twoSplitBatches;
        }

        @Override
        public ListenableFuture<PrestoThriftSplitBatch> getIndexSplits(PrestoThriftSchemaTableName schemaTableName, List<String> indexColumnNames, List<String> outputColumnNames, PrestoThriftPageResult keys, PrestoThriftTupleDomain outputConstraint, int maxSplitCount, PrestoThriftNullableToken nextToken)
        {
            if (keys.getRowCount() == 0) {
                return immediateFuture(new PrestoThriftSplitBatch(ImmutableList.of(), null));
            }
            PrestoThriftId newNextToken = null;
            int[] values = keys.getColumnBlocks().get(0).getIntegerData().getInts();
            int begin;
            int end;
            if (twoSplitBatches) {
                if (nextToken.getToken() == null) {
                    begin = 0;
                    end = values.length / 2;
                    newNextToken = new PrestoThriftId(Ints.toByteArray(1));
                }
                else {
                    begin = values.length / 2;
                    end = values.length;
                }
            }
            else {
                begin = 0;
                end = values.length;
            }

            List<PrestoThriftSplit> splits = new ArrayList<>(end - begin);
            for (int i = begin; i < end; i++) {
                splits.add(new PrestoThriftSplit(new PrestoThriftId(Ints.toByteArray(values[i])), ImmutableList.of()));
            }
            if (shuffleSplits) {
                shuffle(splits);
            }
            return immediateFuture(new PrestoThriftSplitBatch(splits, newNextToken));
        }

        @Override
        public ListenableFuture<PrestoThriftPageResult> getRows(PrestoThriftId splitId, List<String> columns, long maxBytes, PrestoThriftNullableToken nextToken)
        {
            if (rowsPerSplit == 0) {
                return immediateFuture(new PrestoThriftPageResult(ImmutableList.of(), 0, null));
            }
            int key = Ints.fromByteArray(splitId.getId());
            int offset = nextToken.getToken() != null ? Ints.fromByteArray(nextToken.getToken().getId()) : 0;
            PrestoThriftId newNextToken = offset + 1 < rowsPerSplit ? new PrestoThriftId(Ints.toByteArray(offset + 1)) : null;
            return immediateFuture(pageResult(key * 10 + offset, newNextToken));
        }

        // methods below are not used for the test

        @Override
        public List<String> listSchemaNames()
                throws PrestoThriftServiceException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<PrestoThriftSchemaTableName> listTables(PrestoThriftNullableSchemaName schemaNameOrNull)
                throws PrestoThriftServiceException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public PrestoThriftNullableTableMetadata getTableMetadata(PrestoThriftSchemaTableName schemaTableName)
                throws PrestoThriftServiceException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<PrestoThriftSplitBatch> getSplits(PrestoThriftSchemaTableName schemaTableName, PrestoThriftNullableColumnSet desiredColumns, PrestoThriftTupleDomain outputConstraint, int maxSplitCount, PrestoThriftNullableToken nextToken)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static ThriftColumnHandle column(String name, Type type)
    {
        return new ThriftColumnHandle(name, type, null, false);
    }

    private static List<List<Integer>> generateKeys(int beginInclusive, int endExclusive)
    {
        return IntStream.range(beginInclusive, endExclusive)
                .mapToObj(ImmutableList::of)
                .collect(toImmutableList());
    }

    private static PrestoThriftPageResult pageResult(int value, PrestoThriftId nextToken)
    {
        return new PrestoThriftPageResult(ImmutableList.of(integerData(new PrestoThriftInteger(null, new int[] {value}))), 1, nextToken);
    }
}
