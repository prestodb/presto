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

import com.facebook.presto.orc.TupleDomainOrcPredicate.ColumnReference;
import com.facebook.presto.orc.metadata.ColumnStatistics;
import com.facebook.presto.orc.metadata.HiveBloomFilter;
import com.facebook.presto.orc.metadata.IntegerStatistics;
import com.facebook.presto.orc.metadata.OrcMetadataReader;
import com.facebook.presto.orc.proto.OrcProto;
import com.facebook.presto.orc.protobuf.CodedInputStream;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import io.airlift.slice.Slice;
import org.apache.hive.common.util.BloomFilter;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.orc.TupleDomainOrcPredicate.checkInBloomFilter;
import static com.facebook.presto.orc.TupleDomainOrcPredicate.extractDiscreteValues;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestOrcBloomFilters
{
    private static final String TEST_STRING = "ORC_STRING";
    private static final String TEST_STRING_NOT_WRITTEN = "ORC_STRING_not";
    private static final int TEST_INTEGER = 12345;
    private static final String COLUMN_0 = "bigint_0";
    private static final String COLUMN_1 = "bigint_1";

    private static final Map<Object, Type> TEST_VALUES = ImmutableMap.<Object, Type>builder()
        .put(utf8Slice(TEST_STRING), VARCHAR)
        .put(wrappedBuffer(new byte[]{12, 34, 56}), VARBINARY)
        .put(4312L, BIGINT)
        .put(123, INTEGER)
        .put(234.567, DOUBLE)
        .build();

    @Test
    public void testHiveBloomFilterSerde()
            throws Exception
    {
        BloomFilter bloomFilter = new BloomFilter(1_000_000L, 0.05);

        // String
        bloomFilter.addString(TEST_STRING);
        assertTrue(bloomFilter.testString(TEST_STRING));
        assertFalse(bloomFilter.testString(TEST_STRING_NOT_WRITTEN));

        // Integer
        bloomFilter.addLong(TEST_INTEGER);
        assertTrue(bloomFilter.testLong(TEST_INTEGER));
        assertFalse(bloomFilter.testLong(TEST_INTEGER + 1));

        // Re-construct
        HiveBloomFilter hiveBloomFilter = new HiveBloomFilter(ImmutableList.copyOf(Longs.asList(bloomFilter.getBitSet())), bloomFilter.getBitSize(), bloomFilter.getNumHashFunctions());

        // String
        assertTrue(hiveBloomFilter.testString(TEST_STRING));
        assertFalse(hiveBloomFilter.testString(TEST_STRING_NOT_WRITTEN));

        // Integer
        assertTrue(hiveBloomFilter.testLong(TEST_INTEGER));
        assertFalse(hiveBloomFilter.testLong(TEST_INTEGER + 1));
    }

    @Test
    public void testOrcHiveBloomFilterSerde()
            throws Exception
    {
        BloomFilter bloomFilterWrite = new BloomFilter(1000L, 0.05);

        bloomFilterWrite.addString(TEST_STRING);
        assertTrue(bloomFilterWrite.testString(TEST_STRING));

        OrcProto.BloomFilter.Builder bloomFilterBuilder = OrcProto.BloomFilter.newBuilder();
        bloomFilterBuilder.addAllBitset(Longs.asList(bloomFilterWrite.getBitSet()));
        bloomFilterBuilder.setNumHashFunctions(bloomFilterWrite.getNumHashFunctions());

        OrcProto.BloomFilter bloomFilter = bloomFilterBuilder.build();
        OrcProto.BloomFilterIndex bloomFilterIndex = OrcProto.BloomFilterIndex.getDefaultInstance();
        byte[] bytes = serializeBloomFilterToIndex(bloomFilter, bloomFilterIndex);

        // Read through method
        InputStream inputStream = new ByteArrayInputStream(bytes);
        OrcMetadataReader metadataReader = new OrcMetadataReader();
        List<HiveBloomFilter> bloomFilters = metadataReader.readBloomFilterIndexes(inputStream);

        assertEquals(bloomFilters.size(), 1);

        assertTrue(bloomFilters.get(0).testString(TEST_STRING));
        assertFalse(bloomFilters.get(0).testString(TEST_STRING_NOT_WRITTEN));

        assertEquals(bloomFilterWrite.getBitSize(), bloomFilters.get(0).getBitSize());
        assertEquals(bloomFilterWrite.getNumHashFunctions(), bloomFilters.get(0).getNumHashFunctions());

        // Validate bit set
        assertTrue(Arrays.equals(bloomFilters.get(0).getBitSet(), bloomFilterWrite.getBitSet()));

        // Read directly: allows better inspection of the bit sets (helped to fix a lot of bugs)
        CodedInputStream input = CodedInputStream.newInstance(bytes);
        OrcProto.BloomFilterIndex deserializedBloomFilterIndex = OrcProto.BloomFilterIndex.parseFrom(input);
        List<OrcProto.BloomFilter> bloomFilterList = deserializedBloomFilterIndex.getBloomFilterList();
        assertEquals(bloomFilterList.size(), 1);

        OrcProto.BloomFilter bloomFilterRead = bloomFilterList.get(0);

        // Validate contents of ORC bloom filter bit set
        assertTrue(Arrays.equals(Longs.toArray(bloomFilterRead.getBitsetList()), bloomFilterWrite.getBitSet()));

        // hash functions
        assertEquals(bloomFilterWrite.getNumHashFunctions(), bloomFilterRead.getNumHashFunctions());

        // bit size
        assertEquals(bloomFilterWrite.getBitSet().length, bloomFilterRead.getBitsetCount());
    }

    private static byte[] serializeBloomFilterToIndex(OrcProto.BloomFilter bloomFilter, OrcProto.BloomFilterIndex bloomFilterIndex)
            throws IOException
    {
        assertTrue(bloomFilter.isInitialized());

        OrcProto.BloomFilterIndex.Builder builder = bloomFilterIndex.toBuilder();
        builder.addBloomFilter(bloomFilter);

        OrcProto.BloomFilterIndex index = builder.build();
        assertTrue(index.isInitialized());
        assertEquals(index.getBloomFilterCount(), 1);

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        index.writeTo(os);
        os.flush();
        return os.toByteArray();
    }

    private static OrcProto.BloomFilter toOrcBloomFilter(BloomFilter bloomFilter)
    {
        OrcProto.BloomFilter.Builder builder = OrcProto.BloomFilter.newBuilder();
        builder.addAllBitset(Longs.asList(bloomFilter.getBitSet()));
        builder.setNumHashFunctions(bloomFilter.getNumHashFunctions());
        return builder.build();
    }

    @Test
    public void testBloomFilterPredicateValuesExisting()
            throws Exception
    {
        BloomFilter bloomFilter = new BloomFilter(TEST_VALUES.size() * 10, 0.01);

        for (Object o : TEST_VALUES.keySet()) {
            if (o instanceof Long) {
                bloomFilter.addLong((Long) o);
            }
            else if (o instanceof Integer) {
                bloomFilter.addLong((Integer) o);
            }
            else if (o instanceof String) {
                bloomFilter.addString((String) o);
            }
            else if (o instanceof BigDecimal) {
                bloomFilter.addString(o.toString());
            }
            else if (o instanceof Slice) {
                bloomFilter.addString(((Slice) o).toStringUtf8());
            }
            else if (o instanceof Timestamp) {
                bloomFilter.addLong(((Timestamp) o).getTime());
            }
            else if (o instanceof Double) {
                bloomFilter.addDouble((Double) o);
            }
            else {
                fail("Unsupported type " + o.getClass());
            }
        }

        for (Map.Entry<Object, Type> testValue : TEST_VALUES.entrySet()) {
            boolean matched = checkInBloomFilter(bloomFilter, testValue.getKey(), testValue.getValue());
            assertTrue(matched, "type " + testValue.getClass());
        }

        // test unsupported type: can be supported by ORC but is not implemented yet
        assertTrue(checkInBloomFilter(bloomFilter, new Date(), DATE), "unsupported type DATE should always return true");
    }

    @Test
    public void testBloomFilterPredicateValuesNonExisting()
            throws Exception
    {
        BloomFilter bloomFilter = new BloomFilter(TEST_VALUES.size() * 10, 0.01);

        for (Map.Entry<Object, Type> testValue : TEST_VALUES.entrySet()) {
            boolean matched = checkInBloomFilter(bloomFilter, testValue.getKey(), testValue.getValue());
            assertFalse(matched, "type " + testValue.getKey().getClass());
        }

        // test unsupported type: can be supported by ORC but is not implemented yet
        assertTrue(checkInBloomFilter(bloomFilter, new Date(), DATE), "unsupported type DATE should always return true");
    }

    @Test
    public void testExtractValuesFromSingleDomain()
            throws Exception
    {
        Map<Type, Object> testValues = ImmutableMap.<Type, Object>builder()
                .put(BOOLEAN, true)
                .put(INTEGER, 1234L)
                .put(BIGINT, 4321L)
                .put(DOUBLE, 0.123)
                .put(VARCHAR, utf8Slice(TEST_STRING))
                .build();

        for (Map.Entry<Type, Object> testValue : testValues.entrySet()) {
            Domain predicateDomain = Domain.singleValue(testValue.getKey(), testValue.getValue());
            Optional<Collection<Object>> discreteValues = extractDiscreteValues(predicateDomain.getValues());
            assertTrue(discreteValues.isPresent());
            Collection<Object> objects = discreteValues.get();
            assertEquals(objects.size(), 1);
            assertEquals(objects.iterator().next(), testValue.getValue());
        }
    }

    @Test
    // simulate query on a 2 columns where 1 is used as part of the where, with and without bloom filter
    public void testMatches()
            throws Exception
    {
        // stripe column
        Domain testingColumnHandleDomain = Domain.singleValue(BIGINT, 1234L);
        TupleDomain.ColumnDomain<String> column0 = new TupleDomain.ColumnDomain<>(COLUMN_0, testingColumnHandleDomain);

        // predicate consist of the bigint_0 = 1234
        TupleDomain<String> effectivePredicate = TupleDomain.fromColumnDomains(Optional.of(ImmutableList.of(column0)));
        TupleDomain<String> emptyEffectivePredicate = TupleDomain.all();

        // predicate column references
        List<ColumnReference<String>> columnReferences = ImmutableList.<ColumnReference<String>>builder()
                .add(new ColumnReference<>(COLUMN_0, 0, BIGINT))
                .add(new ColumnReference<>(COLUMN_1, 1, BIGINT))
                .build();

        TupleDomainOrcPredicate<String> predicate = new TupleDomainOrcPredicate<>(effectivePredicate, columnReferences, true);
        TupleDomainOrcPredicate<String> emptyPredicate = new TupleDomainOrcPredicate<>(emptyEffectivePredicate, columnReferences, true);

        // assemble a matching and a non-matching bloom filter
        HiveBloomFilter hiveBloomFilter = new HiveBloomFilter(new BloomFilter(1000, 0.01));
        OrcProto.BloomFilter emptyOrcBloomFilter = toOrcBloomFilter(hiveBloomFilter);
        hiveBloomFilter.addLong(1234);
        OrcProto.BloomFilter orcBloomFilter = toOrcBloomFilter(hiveBloomFilter);

        Map<Integer, ColumnStatistics> matchingStatisticsByColumnIndex = ImmutableMap.of(0, new ColumnStatistics(
                        null,
                        null,
                        new IntegerStatistics(10L, 2000L),
                        null,
                        null,
                        null,
                        null,
                        toHiveBloomFilter(orcBloomFilter)));

        Map<Integer, ColumnStatistics> nonMatchingStatisticsByColumnIndex = ImmutableMap.of(0, new ColumnStatistics(
                null,
                null,
                new IntegerStatistics(10L, 2000L),
                null,
                null,
                null,
                null,
                toHiveBloomFilter(emptyOrcBloomFilter)));

        Map<Integer, ColumnStatistics> withoutBloomFilterStatisticsByColumnIndex = ImmutableMap.of(0, new ColumnStatistics(
                null,
                null,
                new IntegerStatistics(10L, 2000L),
                null,
                null,
                null,
                null,
                null));

        assertTrue(predicate.matches(1L, matchingStatisticsByColumnIndex));
        assertTrue(predicate.matches(1L, withoutBloomFilterStatisticsByColumnIndex));
        assertFalse(predicate.matches(1L, nonMatchingStatisticsByColumnIndex));
        assertTrue(emptyPredicate.matches(1L, matchingStatisticsByColumnIndex));
    }

    private static HiveBloomFilter toHiveBloomFilter(OrcProto.BloomFilter emptyOrcBloomFilter)
    {
        return new HiveBloomFilter(emptyOrcBloomFilter.getBitsetList(), emptyOrcBloomFilter.getBitsetCount() * 64, emptyOrcBloomFilter.getNumHashFunctions());
    }
}
