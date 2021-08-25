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
package com.facebook.presto.orc;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.predicate.TupleDomain.fromFixedValues;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.NoopOrcAggregatedMemoryContext.NOOP_ORC_AGGREGATED_MEMORY_CONTEXT;
import static com.facebook.presto.orc.OrcPredicate.TRUE;
import static com.facebook.presto.orc.OrcReader.MAX_BATCH_SIZE;
import static com.facebook.presto.orc.OrcTester.Format.ORC_12;
import static com.facebook.presto.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static com.facebook.presto.orc.OrcTester.writeOrcColumnHive;
import static com.facebook.presto.orc.TupleDomainOrcPredicate.ColumnReference;
import static com.facebook.presto.orc.metadata.CompressionKind.SNAPPY;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Lists.newArrayList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestReadBloomFilter
{
    @Test
    public void test()
            throws Exception
    {
        testType(TINYINT, ImmutableList.of(1L, 50L, 100L), 50L, 77L);
        testType(SMALLINT, ImmutableList.of(1L, 5000L, 10_000L), 5000L, 7777L);
        testType(INTEGER, ImmutableList.of(1L, 500_000L, 1_000_000L), 500_000L, 777_777L);
        testType(BIGINT, ImmutableList.of(1L, 500_000L, 1_000_000L), 500_000L, 777_777L);
        testType(DOUBLE, ImmutableList.of(1.11, 500_000.55, 1_000_000.99), 500_000.55, 777_777.77);

        testType(VARCHAR, ImmutableList.of("a", "o", "z"), utf8Slice("o"), utf8Slice("w"));
        testType(VARBINARY,
                ImmutableList.of(new SqlVarbinary("a".getBytes(UTF_8)), new SqlVarbinary("o".getBytes(UTF_8)), new SqlVarbinary("z".getBytes(UTF_8))),
                utf8Slice("o"),
                utf8Slice("w"));
        // Bloom filters are not supported for DECIMAL, FLOAT, DATE, TIMESTAMP, and CHAR
    }

    private static <T> void testType(Type type, List<T> uniqueValues, T inBloomFilter, T notInBloomFilter)
            throws Exception
    {
        List<T> writeValues = newArrayList(limit(cycle(uniqueValues), 30_000));

        try (TempFile tempFile = new TempFile()) {
            writeOrcColumnHive(tempFile.getFile(), ORC_12, SNAPPY, type, writeValues);

            // no predicate
            try (OrcBatchRecordReader recordReader = createCustomOrcRecordReader(tempFile, type, Optional.empty(), true)) {
                assertEquals(recordReader.nextBatch(), MAX_BATCH_SIZE);
            }

            try (OrcBatchRecordReader recordReader = createCustomOrcRecordReader(tempFile, type, Optional.empty(), false)) {
                assertEquals(recordReader.nextBatch(), MAX_BATCH_SIZE);
            }

            // predicate for non-matching value
            try (OrcBatchRecordReader recordReader = createCustomOrcRecordReader(tempFile, type, Optional.of(notInBloomFilter), true)) {
                assertEquals(recordReader.nextBatch(), -1);
            }

            try (OrcBatchRecordReader recordReader = createCustomOrcRecordReader(tempFile, type, Optional.of(notInBloomFilter), false)) {
                assertEquals(recordReader.nextBatch(), MAX_BATCH_SIZE);
            }

            // predicate for matching value
            try (OrcBatchRecordReader recordReader = createCustomOrcRecordReader(tempFile, type, Optional.of(inBloomFilter), true)) {
                assertEquals(recordReader.nextBatch(), MAX_BATCH_SIZE);
            }

            try (OrcBatchRecordReader recordReader = createCustomOrcRecordReader(tempFile, type, Optional.of(inBloomFilter), false)) {
                assertEquals(recordReader.nextBatch(), MAX_BATCH_SIZE);
            }
        }
    }

    private static <T> OrcBatchRecordReader createCustomOrcRecordReader(TempFile tempFile, Type type, Optional<T> filterValue, boolean bloomFilterEnabled)
            throws IOException
    {
        OrcPredicate predicate = filterValue.map(value -> makeOrcPredicate(type, value, bloomFilterEnabled)).map(OrcPredicate.class::cast).orElse(TRUE);

        OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
        OrcReader orcReader = new OrcReader(
                orcDataSource,
                OrcEncoding.ORC,
                new StorageOrcFileTailSource(),
                new StorageStripeMetadataSource(),
                NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                OrcReaderTestingUtils.createDefaultTestConfig(),
                false,
                NO_ENCRYPTION,
                DwrfKeyProvider.EMPTY,
                new RuntimeStats());

        assertEquals(orcReader.getColumnNames(), ImmutableList.of("test"));
        assertEquals(orcReader.getFooter().getRowsInRowGroup(), 10_000);

        return orcReader.createBatchRecordReader(ImmutableMap.of(0, type), predicate, HIVE_STORAGE_TIME_ZONE, new TestingHiveOrcAggregatedMemoryContext(), MAX_BATCH_SIZE);
    }

    private static <T> TupleDomainOrcPredicate<String> makeOrcPredicate(Type type, T value, boolean bloomFilterEnabled)
    {
        return new TupleDomainOrcPredicate<>(
                fromFixedValues(ImmutableMap.of("test", NullableValue.of(type, value))),
                ImmutableList.of(new ColumnReference<>("test", 0, type)),
                bloomFilterEnabled,
                Optional.empty());
    }
}
