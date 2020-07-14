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

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.orc.metadata.DwrfEncryption;
import com.facebook.presto.orc.metadata.DwrfMetadataReader;
import com.facebook.presto.orc.metadata.EncryptionGroup;
import com.facebook.presto.orc.metadata.Footer;
import com.facebook.presto.orc.metadata.OrcFileTail;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.facebook.presto.orc.proto.DwrfProto;
import com.facebook.presto.orc.protobuf.CodedInputStream;
import com.facebook.presto.orc.stream.OrcInputStream;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.AbstractOrcRecordReader.getDecryptionKeyMetadata;
import static com.facebook.presto.orc.AbstractTestOrcReader.intsBetween;
import static com.facebook.presto.orc.DwrfEncryptionInfo.createNodeToGroupMap;
import static com.facebook.presto.orc.NoopOrcAggregatedMemoryContext.NOOP_ORC_AGGREGATED_MEMORY_CONTEXT;
import static com.facebook.presto.orc.OrcDecompressor.createOrcDecompressor;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.OrcReader.validateEncryption;
import static com.facebook.presto.orc.OrcTester.assertFileContentsPresto;
import static com.facebook.presto.orc.OrcTester.rowType;
import static com.facebook.presto.orc.OrcTester.writeOrcColumnsPresto;
import static com.facebook.presto.orc.StripeReader.getDiskRanges;
import static com.facebook.presto.orc.metadata.ColumnEncoding.DEFAULT_SEQUENCE_ID;
import static com.facebook.presto.orc.metadata.CompressionKind.ZSTD;
import static com.facebook.presto.orc.metadata.KeyProvider.UNKNOWN;
import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.INT;
import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.LIST;
import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.MAP;
import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.STRUCT;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_INDEX;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDecryption
{
    private static final List<byte[]> A_KEYS = ImmutableList.of("key1a".getBytes(), "key2a".getBytes());
    private static final List<byte[]> B_KEYS = ImmutableList.of("key1b".getBytes(), "key2b".getBytes());
    private static final StripeInformation A_STRIPE = new StripeInformation(1, 2, 3, 4, 5, A_KEYS);
    private static final StripeInformation NO_KEYS_STRIPE = new StripeInformation(1, 2, 3, 4, 5, ImmutableList.of());
    private static final StripeInformation B_STRIPE = new StripeInformation(1, 2, 3, 4, 5, B_KEYS);

    private static final OrcType ROW_TYPE = new OrcType(STRUCT, ImmutableList.of(1, 2, 4, 7), ImmutableList.of("col_int", "col_list", "col_map", "col_row"), Optional.empty(), Optional.empty(), Optional.empty());
    private static final OrcType ROW_TYPE2 = new OrcType(STRUCT, ImmutableList.of(8), ImmutableList.of("sub_row1"), Optional.empty(), Optional.empty(), Optional.empty());
    private static final OrcType ROW_TYPE3 = new OrcType(STRUCT, ImmutableList.of(9, 10), ImmutableList.of("sub_int1", "sub_int2"), Optional.empty(), Optional.empty(), Optional.empty());
    private static final OrcType INT_TYPE = new OrcType(INT, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty());
    private static final OrcType LIST_TYPE = new OrcType(LIST, ImmutableList.of(3), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty());
    private static final OrcType MAP_TYPE = new OrcType(MAP, ImmutableList.of(5, 6), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty());

    @Test
    public void testValidateEncrypted()
    {
        List<EncryptionGroup> encryptionGroups = ImmutableList.of(
                new EncryptionGroup(
                        ImmutableList.of(1, 3),
                        Optional.empty(),
                        ImmutableList.of(Slices.EMPTY_SLICE, Slices.EMPTY_SLICE)),
                new EncryptionGroup(
                        ImmutableList.of(4),
                        Optional.empty(),
                        ImmutableList.of(Slices.EMPTY_SLICE)));

        Optional<DwrfEncryption> encryption = Optional.of(new DwrfEncryption(UNKNOWN, encryptionGroups));

        Footer footer = createFooterWithEncryption(ImmutableList.of(A_STRIPE, NO_KEYS_STRIPE), encryption);
        validateEncryption(footer, new OrcDataSourceId("1"));
    }

    @Test
    public void testValidateUnencrypted()
    {
        Footer footer = createFooterWithEncryption(ImmutableList.of(NO_KEYS_STRIPE), Optional.empty());
        validateEncryption(footer, new OrcDataSourceId("1"));
    }

    @Test(expectedExceptions = OrcCorruptionException.class)
    public void testValidateMissingStripeKeys()
    {
        List<EncryptionGroup> encryptionGroups = ImmutableList.of(
                new EncryptionGroup(
                        ImmutableList.of(1, 3),
                        Optional.empty(),
                        ImmutableList.of(Slices.EMPTY_SLICE, Slices.EMPTY_SLICE)),
                new EncryptionGroup(
                        ImmutableList.of(4),
                        Optional.empty(),
                        ImmutableList.of(Slices.EMPTY_SLICE)));

        Optional<DwrfEncryption> encryption = Optional.of(new DwrfEncryption(UNKNOWN, encryptionGroups));
        Footer footer = createFooterWithEncryption(ImmutableList.of(NO_KEYS_STRIPE), encryption);
        validateEncryption(footer, new OrcDataSourceId("1"));
    }

    @Test(expectedExceptions = OrcCorruptionException.class)
    public void testValidateMismatchedGroups()
    {
        List<EncryptionGroup> encryptionGroups = ImmutableList.of(
                new EncryptionGroup(
                        ImmutableList.of(1, 3),
                        Optional.empty(),
                        ImmutableList.of(Slices.EMPTY_SLICE, Slices.EMPTY_SLICE)));

        Optional<DwrfEncryption> encryption = Optional.of(new DwrfEncryption(UNKNOWN, encryptionGroups));
        Footer footer = createFooterWithEncryption(ImmutableList.of(A_STRIPE), encryption);
        validateEncryption(footer, new OrcDataSourceId("1"));
    }

    private Footer createFooterWithEncryption(List<StripeInformation> stripes, Optional<DwrfEncryption> encryption)
    {
        List<OrcType> types = ImmutableList.of(ROW_TYPE, INT_TYPE, LIST_TYPE, INT_TYPE, MAP_TYPE, INT_TYPE, INT_TYPE);

        return new Footer(
                1,
                2,
                stripes,
                types,
                ImmutableList.of(),
                ImmutableMap.of(),
                encryption);
    }

    @Test
    public void testCreateNodeToGroupMap()
    {
        List<List<Integer>> encryptionGroups = ImmutableList.of(
                ImmutableList.of(1, 3),
                ImmutableList.of(4),
                ImmutableList.of(7));

        List<OrcType> types = ImmutableList.of(ROW_TYPE, INT_TYPE, LIST_TYPE, INT_TYPE, MAP_TYPE, INT_TYPE, INT_TYPE, ROW_TYPE2, ROW_TYPE3, INT_TYPE, INT_TYPE);
        Map<Integer, Integer> actual = createNodeToGroupMap(encryptionGroups, types);
        Map<Integer, Integer> expected = ImmutableMap.<Integer, Integer>builder()
                .put(1, 0)
                .put(3, 0)
                .put(4, 1)
                .put(5, 1)
                .put(6, 1)
                .put(7, 2)
                .put(8, 2)
                .put(9, 2)
                .put(10, 2)
                .build();
        assertEquals(actual, expected);
    }

    @Test
    public void testGetStripeDecryptionKeys()
    {
        List<StripeInformation> encryptedStripes = ImmutableList.of(A_STRIPE, NO_KEYS_STRIPE, B_STRIPE, NO_KEYS_STRIPE);
        assertEquals(getDecryptionKeyMetadata(0, encryptedStripes), A_KEYS);
        assertEquals(getDecryptionKeyMetadata(1, encryptedStripes), A_KEYS);
        assertEquals(getDecryptionKeyMetadata(2, encryptedStripes), B_KEYS);
        assertEquals(getDecryptionKeyMetadata(3, encryptedStripes), B_KEYS);
    }

    @Test
    public void testGetStripeDecryptionKeysUnencrypted()
    {
        List<StripeInformation> unencryptedStripes = ImmutableList.of(NO_KEYS_STRIPE, NO_KEYS_STRIPE);
        assertEquals(getDecryptionKeyMetadata(0, unencryptedStripes), ImmutableList.of());
        assertEquals(getDecryptionKeyMetadata(1, unencryptedStripes), ImmutableList.of());
    }

    @Test
    public void testGetDiskRanges()
    {
        List<Stream> unencryptedStreams = ImmutableList.of(
                new Stream(3, ROW_INDEX, 5, true, DEFAULT_SEQUENCE_ID, Optional.of(15L)),
                new Stream(4, ROW_INDEX, 5, true),
                new Stream(3, DATA, 5, true, DEFAULT_SEQUENCE_ID, Optional.of(45L)),
                new Stream(4, DATA, 5, true));

        List<Stream> group1Streams = ImmutableList.of(
                new Stream(0, ROW_INDEX, 5, true),
                new Stream(5, ROW_INDEX, 5, true, DEFAULT_SEQUENCE_ID, Optional.of(25L)),
                new Stream(0, DATA, 5, true, DEFAULT_SEQUENCE_ID, Optional.of(30L)),
                new Stream(5, DATA, 5, true, DEFAULT_SEQUENCE_ID, Optional.of(55L)));

        List<Stream> group2Streams = ImmutableList.of(
                new Stream(1, ROW_INDEX, 5, true, DEFAULT_SEQUENCE_ID, Optional.of(5L)),
                new Stream(2, ROW_INDEX, 5, true),
                new Stream(1, DATA, 5, true, DEFAULT_SEQUENCE_ID, Optional.of(35L)),
                new Stream(2, DATA, 5, true));

        Map<StreamId, DiskRange> actual = getDiskRanges(ImmutableList.of(unencryptedStreams, group1Streams, group2Streams));

        Map<StreamId, DiskRange> expected = ImmutableMap.<StreamId, DiskRange>builder()
                .put(new StreamId(0, DEFAULT_SEQUENCE_ID, ROW_INDEX), new DiskRange(0, 5))
                .put(new StreamId(1, DEFAULT_SEQUENCE_ID, ROW_INDEX), new DiskRange(5, 5))
                .put(new StreamId(2, DEFAULT_SEQUENCE_ID, ROW_INDEX), new DiskRange(10, 5))
                .put(new StreamId(3, DEFAULT_SEQUENCE_ID, ROW_INDEX), new DiskRange(15, 5))
                .put(new StreamId(4, DEFAULT_SEQUENCE_ID, ROW_INDEX), new DiskRange(20, 5))
                .put(new StreamId(5, DEFAULT_SEQUENCE_ID, ROW_INDEX), new DiskRange(25, 5))
                .put(new StreamId(0, DEFAULT_SEQUENCE_ID, DATA), new DiskRange(30, 5))
                .put(new StreamId(1, DEFAULT_SEQUENCE_ID, DATA), new DiskRange(35, 5))
                .put(new StreamId(2, DEFAULT_SEQUENCE_ID, DATA), new DiskRange(40, 5))
                .put(new StreamId(3, DEFAULT_SEQUENCE_ID, DATA), new DiskRange(45, 5))
                .put(new StreamId(4, DEFAULT_SEQUENCE_ID, DATA), new DiskRange(50, 5))
                .put(new StreamId(5, DEFAULT_SEQUENCE_ID, DATA), new DiskRange(55, 5))
                .build();
        assertEquals(actual, expected);
    }

    @Test
    public void testMultipleEncryptionGroupsRowType()
            throws Exception
    {
        Type rowType = rowType(BIGINT, BIGINT, BIGINT);
        Slice iek1 = Slices.utf8Slice("iek1");
        Slice iek2 = Slices.utf8Slice("iek2");
        DwrfWriterEncryption dwrfWriterEncryption = new DwrfWriterEncryption(
                UNKNOWN,
                ImmutableList.of(
                        new WriterEncryptionGroup(ImmutableList.of(2), iek1),
                        new WriterEncryptionGroup(ImmutableList.of(3), iek2)));
        List<Type> types = ImmutableList.of(rowType);
        List<Long> columnValues = ImmutableList.copyOf(intsBetween(0, 31_234)).stream()
                .map(Number::longValue)
                .collect(toList());

        List<List<?>> values = ImmutableList.of(columnValues.stream()
                .map(OrcTester::toHiveStruct)
                .collect(toList()));
        List<Integer> outputColumns = IntStream.range(0, types.size())
                .boxed()
                .collect(toImmutableList());

        testDecryptionRoundTrip(
                types,
                values,
                values,
                Optional.of(dwrfWriterEncryption),
                ImmutableMap.of(2, iek1,
                        3, iek2),
                ImmutableMap.of(0, rowType),
                ImmutableMap.of(),
                outputColumns);
    }

    @Test
    public void testSingleEncryptionGroupRowType()
            throws Exception
    {
        Type rowType = rowType(BIGINT, BIGINT, BIGINT);
        Slice iek1 = Slices.utf8Slice("iek1");
        DwrfWriterEncryption dwrfWriterEncryption = new DwrfWriterEncryption(
                UNKNOWN,
                ImmutableList.of(
                        new WriterEncryptionGroup(ImmutableList.of(1), iek1)));
        List<Type> types = ImmutableList.of(rowType);
        List<Long> columnValues = ImmutableList.copyOf(intsBetween(0, 31_234)).stream()
                .map(Number::longValue)
                .collect(toList());

        List<List<?>> values = ImmutableList.of(columnValues.stream()
                .map(OrcTester::toHiveStruct)
                .collect(toList()));
        List<Integer> outputColumns = IntStream.range(0, types.size())
                .boxed()
                .collect(toImmutableList());

        testDecryptionRoundTrip(
                types,
                values,
                values,
                Optional.of(dwrfWriterEncryption),
                ImmutableMap.of(1, iek1),
                ImmutableMap.of(0, rowType),
                ImmutableMap.of(),
                outputColumns);
    }

    @Test
    public void testMultipleEncryptionGroupsMultipleColumns()
            throws Exception
    {
        Type rowType = rowType(BIGINT, BIGINT, BIGINT);
        Slice iek1 = Slices.utf8Slice("iek1");
        Slice iek2 = Slices.utf8Slice("iek2");
        DwrfWriterEncryption dwrfWriterEncryption = new DwrfWriterEncryption(
                UNKNOWN,
                ImmutableList.of(
                        new WriterEncryptionGroup(ImmutableList.of(1, 8), iek1),
                        new WriterEncryptionGroup(ImmutableList.of(5, 9, 10), iek2)));
        List<Type> types = ImmutableList.of(rowType, BIGINT, VARCHAR, VARCHAR, BIGINT, VARCHAR, BIGINT, BIGINT);
        List<Long> columnValues = ImmutableList.copyOf(intsBetween(0, 31_234)).stream()
                .map(Number::longValue)
                .collect(toList());
        List<String> varcharValues = ImmutableList.copyOf(intsBetween(0, 31_234)).stream()
                .map(String::valueOf)
                .collect(toList());

        List<List<?>> rowValues = columnValues.stream()
                .map(OrcTester::toHiveStruct)
                .collect(toList());
        List<List<?>> values = ImmutableList.of(
                rowValues,
                columnValues,
                varcharValues,
                varcharValues,
                columnValues,
                varcharValues,
                columnValues,
                columnValues);
        List<Integer> outputColumns = IntStream.range(0, types.size())
                .boxed()
                .collect(toImmutableList());

        testDecryptionRoundTrip(
                types,
                values,
                values,
                Optional.of(dwrfWriterEncryption),
                ImmutableMap.of(1, iek1, 5, iek2, 8, iek1, 9, iek2, 10, iek2),
                ImmutableMap.<Integer, Type>builder()
                        .put(0, rowType)
                        .put(1, BIGINT)
                        .put(2, VARCHAR)
                        .put(3, VARCHAR)
                        .put(4, BIGINT)
                        .put(5, VARCHAR)
                        .put(6, BIGINT)
                        .put(7, BIGINT)
                        .build(),
                ImmutableMap.of(),
                outputColumns);
    }

    @Test
    public void testEncryptionMultipleColumns()
            throws Exception
    {
        List<Long> columnValues = ImmutableList.copyOf(intsBetween(0, 31_234)).stream()
                .map(Number::longValue)
                .collect(toList());
        List<Type> types = ImmutableList.of(BIGINT, BIGINT);
        List<List<?>> values = ImmutableList.of(columnValues, columnValues);
        Slice iek1 = Slices.utf8Slice("iek1");
        Slice iek2 = Slices.utf8Slice("iek2");
        DwrfWriterEncryption dwrfWriterEncryption = new DwrfWriterEncryption(
                UNKNOWN,
                ImmutableList.of(
                        new WriterEncryptionGroup(ImmutableList.of(1), iek1),
                        new WriterEncryptionGroup(ImmutableList.of(2), iek2)));
        List<Integer> outputColumns = IntStream.range(0, types.size())
                .boxed()
                .collect(toImmutableList());
        testDecryptionRoundTrip(
                types,
                values,
                values,
                Optional.of(dwrfWriterEncryption),
                ImmutableMap.of(
                        1, iek1,
                        2, iek2),
                ImmutableMap.of(0, BIGINT, 1, BIGINT),
                ImmutableMap.of(),
                outputColumns);
    }

    @Test(expectedExceptions = OrcPermissionsException.class)
    public void testPermissionErrorForEncryptedWithoutKeys()
            throws Exception
    {
        List<Type> types = ImmutableList.of(rowType(BIGINT, BIGINT, BIGINT));
        List<Long> columnValues = ImmutableList.copyOf(intsBetween(0, 31_234)).stream()
                .map(Number::longValue)
                .collect(toList());
        List<List<?>> values = ImmutableList.of(columnValues.stream()
                .map(OrcTester::toHiveStruct)
                .collect(toList()));

        Slice iek = Slices.utf8Slice("iek");
        DwrfWriterEncryption dwrfWriterEncryption = new DwrfWriterEncryption(
                UNKNOWN,
                ImmutableList.of(
                        new WriterEncryptionGroup(ImmutableList.of(0), iek)));

        List<Integer> outputColumns = IntStream.range(0, types.size())
                .boxed()
                .collect(toImmutableList());
        testDecryptionRoundTrip(
                types,
                values,
                values,
                Optional.of(dwrfWriterEncryption),
                ImmutableMap.of(),
                ImmutableMap.of(0, rowType(BIGINT, BIGINT, BIGINT)),
                ImmutableMap.of(),
                outputColumns);
    }

    @Test
    public void testReadPermittedColumnsWithoutAllKeys()
            throws Exception
    {
        Subfield subfield1 = new Subfield("c.field_0");
        Subfield subfield3 = new Subfield("c.field_2");
        Type rowType = rowType(BIGINT, BIGINT, BIGINT);
        Slice iek1 = Slices.utf8Slice("iek1");
        Slice iek2 = Slices.utf8Slice("iek2");
        Slice iek3 = Slices.utf8Slice("iek3");
        DwrfWriterEncryption dwrfWriterEncryption = new DwrfWriterEncryption(
                UNKNOWN,
                ImmutableList.of(
                        new WriterEncryptionGroup(ImmutableList.of(2), iek1),
                        new WriterEncryptionGroup(ImmutableList.of(3, 6), iek2),
                        new WriterEncryptionGroup(ImmutableList.of(5), iek3)));
        List<Type> types = ImmutableList.of(rowType, BIGINT, BIGINT, BIGINT);
        List<Long> columnValues = ImmutableList.copyOf(intsBetween(0, 31_234)).stream()
                .map(Number::longValue)
                .collect(toList());

        List<List<?>> writtenValues = ImmutableList.of(columnValues.stream()
                        .map(OrcTester::toHiveStruct)
                        .collect(toList()),
                columnValues,
                columnValues,
                columnValues);
        List<List<?>> readValues = ImmutableList.of(columnValues.stream()
                        .map(value -> asList(value, null, value))
                        .collect(toList()),
                columnValues,
                columnValues);
        testDecryptionRoundTrip(
                types,
                writtenValues,
                readValues,
                Optional.of(dwrfWriterEncryption),
                ImmutableMap.of(2, iek1, 5, iek3),
                ImmutableMap.of(0, rowType, 1, BIGINT, 3, BIGINT),
                ImmutableMap.of(0, ImmutableList.of(subfield1, subfield3)),
                ImmutableList.of(0, 1, 3));
    }

    @Test
    public void testReadsEmptyFile()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT, BIGINT);
        List<List<?>> values = ImmutableList.of(ImmutableList.of(), ImmutableList.of());
        Slice iek1 = Slices.utf8Slice("iek1");
        Slice iek2 = Slices.utf8Slice("iek2");
        List<Integer> outputColumns = ImmutableList.of(0, 1);
        // empty files don't specify encryption groups
        testDecryptionRoundTrip(
                types,
                values,
                values,
                Optional.empty(),
                ImmutableMap.of(
                        1, iek1,
                        2, iek2),
                ImmutableMap.of(0, BIGINT, 1, BIGINT),
                ImmutableMap.of(),
                outputColumns);
    }

    private static void testDecryptionRoundTrip(
            List<Type> types,
            List<List<?>> writtenValues,
            List<List<?>> readValues,
            Optional<DwrfWriterEncryption> dwrfWriterEncryption,
            Map<Integer, Slice> readerIntermediateKeys,
            Map<Integer, Type> includedColumns,
            Map<Integer, List<Subfield>> requiredSubfields,
            List<Integer> outputColumns)
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            writeOrcColumnsPresto(tempFile.getFile(), OrcTester.Format.DWRF, ZSTD, dwrfWriterEncryption, types, writtenValues, new OrcWriterStats());

            assertFileContentsPresto(
                    types,
                    tempFile.getFile(),
                    readValues,
                    DWRF,
                    OrcPredicate.TRUE,
                    Optional.empty(),
                    ImmutableList.of(),
                    ImmutableMap.of(),
                    requiredSubfields,
                    readerIntermediateKeys,
                    includedColumns,
                    outputColumns);

            validateFileStatistics(tempFile.getFile(), dwrfWriterEncryption);
        }
    }

    private static void validateFileStatistics(File file, Optional<DwrfWriterEncryption> dwrfWriterEncryption)
            throws IOException
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(file, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
        DwrfMetadataReader metadataReader = new DwrfMetadataReader();
        OrcFileTail orcFileTail = new StorageOrcFileTailSource().getOrcFileTail(orcDataSource, metadataReader, Optional.empty(), false);
        Optional<OrcDecompressor> decompressor = createOrcDecompressor(orcDataSource.getId(), orcFileTail.getCompressionKind(), orcFileTail.getBufferSize(), false);
        try (InputStream inputStream = new OrcInputStream(
                orcDataSource.getId(),
                orcFileTail.getFooterSlice().getInput(),
                decompressor,
                Optional.empty(),
                NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                orcFileTail.getFooterSize())) {
            CodedInputStream input = CodedInputStream.newInstance(inputStream);
            DwrfProto.Footer footer = DwrfProto.Footer.parseFrom(input);
            List<DwrfProto.ColumnStatistics> fileStats = footer.getStatisticsList();

            if (footer.getStripesList().isEmpty()) {
                assertEquals(fileStats.size(), 0);
            }
            else {
                assertEquals(fileStats.size(), footer.getTypesCount());
            }

            if (dwrfWriterEncryption.isPresent()) {
                dwrfWriterEncryption.get().getWriterEncryptionGroups()
                        .stream()
                        .map(WriterEncryptionGroup::getNodes)
                        .flatMap(Collection::stream)
                        .forEach(node -> assertTrue(hasNoTypeStats(fileStats.get(node)), format("file stats for node %s had type stats %s", node, fileStats.get(node))));
                footer.getEncryption().getEncryptionGroupsList()
                        .forEach(group -> assertEquals(group.getNodesCount(), group.getStatisticsCount()));
            }
        }
    }

    private static boolean hasNoTypeStats(DwrfProto.ColumnStatistics columnStatistics)
    {
        return !columnStatistics.hasBinaryStatistics()
                && !columnStatistics.hasBucketStatistics()
                && !columnStatistics.hasDoubleStatistics()
                && !columnStatistics.hasIntStatistics()
                && !columnStatistics.hasStringStatistics();
    }
}
