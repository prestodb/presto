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
import com.facebook.presto.orc.metadata.DwrfEncryption;
import com.facebook.presto.orc.metadata.EncryptionGroup;
import com.facebook.presto.orc.metadata.Footer;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.orc.AbstractOrcRecordReader.getDecryptionKeyMetadata;
import static com.facebook.presto.orc.AbstractTestOrcReader.intsBetween;
import static com.facebook.presto.orc.DwrfEncryptionInfo.createNodeToGroupMap;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.OrcReader.validateEncryption;
import static com.facebook.presto.orc.OrcTester.assertFileContentsPresto;
import static com.facebook.presto.orc.OrcTester.rowType;
import static com.facebook.presto.orc.OrcTester.writeOrcColumnsPresto;
import static com.facebook.presto.orc.metadata.CompressionKind.ZSTD;
import static com.facebook.presto.orc.metadata.KeyProvider.UNKNOWN;
import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.INT;
import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.LIST;
import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.MAP;
import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.STRUCT;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

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
                        Slices.EMPTY_SLICE),
                new EncryptionGroup(
                        ImmutableList.of(4),
                        Optional.empty(),
                        Slices.EMPTY_SLICE));

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
                        Slices.EMPTY_SLICE),
                new EncryptionGroup(
                        ImmutableList.of(4),
                        Optional.empty(),
                        Slices.EMPTY_SLICE));

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
                        Slices.EMPTY_SLICE));

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
        Slice dek = Slices.utf8Slice("dek");
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
        Slice dek = Slices.utf8Slice("dek");
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
        Slice dek = Slices.utf8Slice("dek");
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
    public void testReadUnencryptedColumnsWithoutKeys()
            throws Exception
    {
        Subfield subfield1 = new Subfield("c.field_0");
        Subfield subfield3 = new Subfield("c.field_2");
        Type rowType = rowType(BIGINT, BIGINT, BIGINT);
        Slice iek1 = Slices.utf8Slice("iek1");
        Slice iek2 = Slices.utf8Slice("iek2");
        Slice dek = Slices.utf8Slice("dek");
        DwrfWriterEncryption dwrfWriterEncryption = new DwrfWriterEncryption(
                UNKNOWN,
                ImmutableList.of(
                        new WriterEncryptionGroup(ImmutableList.of(2, 5), iek1),
                        new WriterEncryptionGroup(ImmutableList.of(3, 6), iek2)));
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
                ImmutableMap.of(2, iek1, 5, iek1),
                ImmutableMap.of(0, rowType, 1, BIGINT, 3, BIGINT),
                ImmutableMap.of(0, ImmutableList.of(subfield1, subfield3)),
                ImmutableList.of(0, 1, 3));
    }

    private void testDecryptionRoundTrip(
            List<Type> types,
            List<List<?>> writtenalues,
            List<List<?>> readValues,
            Optional<DwrfWriterEncryption> dwrfWriterEncryption,
            Map<Integer, Slice> readerIntermediateKeys,
            Map<Integer, Type> includedColumns,
            Map<Integer, List<Subfield>> requiredSubfields,
            List<Integer> outputColumns)
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            writeOrcColumnsPresto(tempFile.getFile(), OrcTester.Format.DWRF, ZSTD, dwrfWriterEncryption, types, writtenalues, new OrcWriterStats());

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
        }
    }
}
