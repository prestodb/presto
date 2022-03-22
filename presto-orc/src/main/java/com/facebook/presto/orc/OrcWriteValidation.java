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

import com.facebook.presto.common.GenericInternalException;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.ColumnarMap;
import com.facebook.presto.common.block.ColumnarRow;
import com.facebook.presto.common.type.AbstractLongType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.PostScript.HiveWriterVersion;
import com.facebook.presto.orc.metadata.RowGroupIndex;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.facebook.presto.orc.metadata.statistics.BinaryStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.BooleanStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.DateStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.DoubleStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.IntegerStatistics;
import com.facebook.presto.orc.metadata.statistics.IntegerStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.LongDecimalStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.ShortDecimalStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.StatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.StatisticsHasher;
import com.facebook.presto.orc.metadata.statistics.StringStatistics;
import com.facebook.presto.orc.metadata.statistics.StringStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.StripeStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.common.block.ColumnarArray.toColumnarArray;
import static com.facebook.presto.common.block.ColumnarMap.toColumnarMap;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.StandardTypes.ARRAY;
import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.StandardTypes.ROW;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode.BOTH;
import static com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode.DETAILED;
import static com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode.HASHED;
import static com.facebook.presto.orc.metadata.DwrfMetadataWriter.STATIC_METADATA;
import static com.facebook.presto.orc.metadata.OrcMetadataReader.maxStringTruncateToValidRange;
import static com.facebook.presto.orc.metadata.OrcMetadataReader.minStringTruncateToValidRange;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class OrcWriteValidation
{
    public enum OrcWriteValidationMode
    {
        HASHED, DETAILED, BOTH
    }

    private final List<Integer> version;
    private final CompressionKind compression;
    private final int rowGroupMaxRowCount;
    private final List<String> columnNames;
    private final Map<String, Slice> metadata;
    private final WriteChecksum checksum;
    private final Map<Long, List<RowGroupStatistics>> rowGroupStatistics;
    private final Map<Long, StripeStatistics> stripeStatistics;
    private final List<ColumnStatistics> fileStatistics;
    private final int stringStatisticsLimitInBytes;

    private OrcWriteValidation(
            List<Integer> version,
            CompressionKind compression,
            int rowGroupMaxRowCount,
            List<String> columnNames,
            Map<String, Slice> metadata,
            WriteChecksum checksum,
            Map<Long, List<RowGroupStatistics>> rowGroupStatistics,
            Map<Long, StripeStatistics> stripeStatistics,
            List<ColumnStatistics> fileStatistics,
            int stringStatisticsLimitInBytes)
    {
        this.version = version;
        this.compression = compression;
        this.rowGroupMaxRowCount = rowGroupMaxRowCount;
        this.columnNames = columnNames;
        this.metadata = metadata;
        this.checksum = checksum;
        this.rowGroupStatistics = rowGroupStatistics;
        this.stripeStatistics = stripeStatistics;
        this.fileStatistics = fileStatistics;
        this.stringStatisticsLimitInBytes = stringStatisticsLimitInBytes;
    }

    public List<Integer> getVersion()
    {
        return version;
    }

    public CompressionKind getCompression()
    {
        return compression;
    }

    public int getRowGroupMaxRowCount()
    {
        return rowGroupMaxRowCount;
    }

    public List<String> getColumnNames()
    {
        return columnNames;
    }

    public Map<String, Slice> getMetadata()
    {
        return metadata;
    }

    public void validateMetadata(OrcDataSourceId orcDataSourceId, Map<String, Slice> actualMetadata)
            throws OrcCorruptionException
    {
        // Filter out metadata value statically added by the DWRF writer
        Map<String, Slice> filteredMetadata = actualMetadata.entrySet().stream()
                .filter(entry -> !STATIC_METADATA.containsKey(entry.getKey()))
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));

        if (!metadata.equals(filteredMetadata)) {
            throw new OrcCorruptionException(orcDataSourceId, "Unexpected metadata");
        }
    }

    public WriteChecksum getChecksum()
    {
        return checksum;
    }

    public void validateFileStatistics(OrcDataSourceId orcDataSourceId, List<ColumnStatistics> actualFileStatistics)
            throws OrcCorruptionException
    {
        validateColumnStatisticsEquivalent(orcDataSourceId, "file", actualFileStatistics, fileStatistics);
    }

    public void validateStripeStatistics(OrcDataSourceId orcDataSourceId, List<StripeInformation> actualStripes, List<StripeStatistics> actualStripeStatistics)
            throws OrcCorruptionException
    {
        requireNonNull(actualStripes, "actualStripes is null");
        requireNonNull(actualStripeStatistics, "actualStripeStatistics is null");

        if (actualStripeStatistics.isEmpty()) {
            // DWRF does not have stripe statistics
            return;
        }

        if (actualStripeStatistics.size() != stripeStatistics.size()) {
            throw new OrcCorruptionException(orcDataSourceId, "Write validation failed: unexpected number of columns in stripe statistics");
        }

        for (int stripeIndex = 0; stripeIndex < actualStripes.size(); stripeIndex++) {
            long stripeOffset = actualStripes.get(stripeIndex).getOffset();
            StripeStatistics actual = actualStripeStatistics.get(stripeIndex);
            validateStripeStatistics(orcDataSourceId, stripeOffset, actual.getColumnStatistics());
        }
    }

    public void validateStripeStatistics(OrcDataSourceId orcDataSourceId, long stripeOffset, List<ColumnStatistics> actual)
            throws OrcCorruptionException
    {
        StripeStatistics expected = stripeStatistics.get(stripeOffset);
        if (expected == null) {
            throw new OrcCorruptionException(orcDataSourceId, "Unexpected stripe at offset %s", stripeOffset);
        }
        validateColumnStatisticsEquivalent(orcDataSourceId, "Stripe at " + stripeOffset, actual, expected.getColumnStatistics());
    }

    public void validateRowGroupStatistics(OrcDataSourceId orcDataSourceId, long stripeOffset, Map<StreamId, List<RowGroupIndex>> actualRowGroupStatistics)
            throws OrcCorruptionException
    {
        requireNonNull(actualRowGroupStatistics, "actualRowGroupStatistics is null");
        List<RowGroupStatistics> expectedRowGroupStatistics = rowGroupStatistics.get(stripeOffset);
        if (expectedRowGroupStatistics == null) {
            throw new OrcCorruptionException(orcDataSourceId, "Unexpected stripe at offset %s", stripeOffset);
        }

        int rowGroupCount = expectedRowGroupStatistics.size();
        for (Entry<StreamId, List<RowGroupIndex>> entry : actualRowGroupStatistics.entrySet()) {
            // TODO: Remove once the Presto writer supports flat map
            if (entry.getKey().getSequence() > 0) {
                throw new OrcCorruptionException(orcDataSourceId, "Unexpected sequence ID for column %s at offset %s", entry.getKey().getColumn(), stripeOffset);
            }
            if (entry.getValue().size() != rowGroupCount) {
                throw new OrcCorruptionException(orcDataSourceId, "Unexpected row group count stripe in at offset %s", stripeOffset);
            }
        }

        for (int rowGroupIndex = 0; rowGroupIndex < expectedRowGroupStatistics.size(); rowGroupIndex++) {
            RowGroupStatistics expectedRowGroup = expectedRowGroupStatistics.get(rowGroupIndex);
            if (expectedRowGroup.getValidationMode() != HASHED) {
                Map<Integer, ColumnStatistics> expectedStatistics = expectedRowGroup.getColumnStatistics();
                Set<Integer> actualColumns = actualRowGroupStatistics.keySet().stream()
                        .map(StreamId::getColumn)
                        .collect(Collectors.toSet());
                if (!expectedStatistics.keySet().equals(actualColumns)) {
                    throw new OrcCorruptionException(orcDataSourceId, "Unexpected column in row group %s in stripe at offset %s", rowGroupIndex, stripeOffset);
                }
                for (Entry<StreamId, List<RowGroupIndex>> entry : actualRowGroupStatistics.entrySet()) {
                    ColumnStatistics actual = entry.getValue().get(rowGroupIndex).getColumnStatistics();
                    ColumnStatistics expected = expectedStatistics.get(entry.getKey().getColumn());
                    validateColumnStatisticsEquivalent(orcDataSourceId, "Row group " + rowGroupIndex + " in stripe at offset " + stripeOffset, actual, expected);
                }
            }

            if (expectedRowGroup.getValidationMode() != DETAILED) {
                RowGroupStatistics actualRowGroup = buildActualRowGroupStatistics(rowGroupIndex, actualRowGroupStatistics);
                if (expectedRowGroup.getHash() != actualRowGroup.getHash()) {
                    throw new OrcCorruptionException(orcDataSourceId, "Checksum mismatch for row group %s in stripe at offset %s", rowGroupIndex, stripeOffset);
                }
            }
        }
    }

    private static RowGroupStatistics buildActualRowGroupStatistics(int rowGroupIndex, Map<StreamId, List<RowGroupIndex>> actualRowGroupStatistics)
    {
        return new RowGroupStatistics(
                BOTH,
                actualRowGroupStatistics.entrySet()
                        .stream()
                        .collect(Collectors.toMap(entry -> entry.getKey().getColumn(), entry -> entry.getValue().get(rowGroupIndex).getColumnStatistics())));
    }

    public void validateRowGroupStatistics(
            OrcDataSourceId orcDataSourceId,
            long stripeOffset,
            int rowGroupIndex,
            List<ColumnStatistics> actual)
            throws OrcCorruptionException
    {
        List<RowGroupStatistics> rowGroups = rowGroupStatistics.get(stripeOffset);
        if (rowGroups == null) {
            throw new OrcCorruptionException(orcDataSourceId, "Unexpected stripe at offset %s", stripeOffset);
        }
        if (rowGroups.size() <= rowGroupIndex) {
            throw new OrcCorruptionException(orcDataSourceId, "Unexpected row group %s in stripe at offset %s", rowGroupIndex, stripeOffset);
        }

        RowGroupStatistics expectedRowGroup = rowGroups.get(rowGroupIndex);
        RowGroupStatistics actualRowGroup = new RowGroupStatistics(BOTH, IntStream.range(1, actual.size()).boxed().collect(toImmutableMap(identity(), actual::get)));

        if (expectedRowGroup.getValidationMode() != HASHED) {
            Map<Integer, ColumnStatistics> expectedByColumnIndex = expectedRowGroup.getColumnStatistics();

            // new writer does not write row group stats for column zero (table row column)
            List<ColumnStatistics> expected = IntStream.range(1, actual.size())
                    .mapToObj(expectedByColumnIndex::get)
                    .collect(toImmutableList());
            actual = actual.subList(1, actual.size());

            validateColumnStatisticsEquivalent(orcDataSourceId, "Row group " + rowGroupIndex + " in stripe at offset " + stripeOffset, actual, expected);
        }

        if (expectedRowGroup.getValidationMode() != DETAILED) {
            if (expectedRowGroup.getHash() != actualRowGroup.getHash()) {
                throw new OrcCorruptionException(orcDataSourceId, "Checksum mismatch for row group %s in stripe at offset %s", rowGroupIndex, stripeOffset);
            }
        }
    }

    public StatisticsValidation createWriteStatisticsBuilder(Map<Integer, Type> readColumns)
    {
        requireNonNull(readColumns, "readColumns is null");
        checkArgument(!readColumns.isEmpty(), "readColumns is empty");
        int columnCount = readColumns.keySet().stream()
                .mapToInt(Integer::intValue)
                .max().getAsInt() + 1;
        checkArgument(readColumns.size() == columnCount, "statistics validation requires all columns to be read");

        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (int column = 0; column < columnCount; column++) {
            Type type = readColumns.get(column);
            checkArgument(type != null, "statistics validation requires all columns to be read");
            types.add(type);
        }
        return new StatisticsValidation(types.build());
    }

    private static void validateColumnStatisticsEquivalent(
            OrcDataSourceId orcDataSourceId,
            String name,
            List<ColumnStatistics> actualColumnStatistics,
            List<ColumnStatistics> expectedColumnStatistics)
            throws OrcCorruptionException
    {
        requireNonNull(name, "name is null");
        requireNonNull(actualColumnStatistics, "actualColumnStatistics is null");
        requireNonNull(expectedColumnStatistics, "expectedColumnStatistics is null");
        if (actualColumnStatistics.size() != expectedColumnStatistics.size()) {
            throw new OrcCorruptionException(orcDataSourceId, "Write validation failed: unexpected number of columns in %s statistics", name);
        }
        for (int i = 0; i < actualColumnStatistics.size(); i++) {
            ColumnStatistics actual = actualColumnStatistics.get(i);
            ColumnStatistics expected = expectedColumnStatistics.get(i);
            validateColumnStatisticsEquivalent(orcDataSourceId, name + " column " + i, actual, expected);
        }
    }

    private static void validateColumnStatisticsEquivalent(
            OrcDataSourceId orcDataSourceId,
            String name,
            ColumnStatistics actualColumnStatistics,
            ColumnStatistics expectedColumnStatistics)
            throws OrcCorruptionException
    {
        requireNonNull(name, "name is null");
        requireNonNull(actualColumnStatistics, "actualColumnStatistics is null");
        requireNonNull(expectedColumnStatistics, "expectedColumnStatistics is null");

        if (actualColumnStatistics.getNumberOfValues() != expectedColumnStatistics.getNumberOfValues()) {
            throw new OrcCorruptionException(orcDataSourceId, "Write validation failed: unexpected number of values in %s statistics", name);
        }

        if (!Objects.equals(actualColumnStatistics.getBooleanStatistics(), expectedColumnStatistics.getBooleanStatistics())) {
            throw new OrcCorruptionException(orcDataSourceId, "Write validation failed: unexpected boolean counts in %s statistics", name);
        }
        if (!Objects.equals(actualColumnStatistics.getIntegerStatistics(), expectedColumnStatistics.getIntegerStatistics())) {
            IntegerStatistics actualIntegerStatistics = actualColumnStatistics.getIntegerStatistics();
            IntegerStatistics expectedIntegerStatistics = expectedColumnStatistics.getIntegerStatistics();
            // The sum of the integer stats depends on the order of how we merge them.
            // It is possible the sum can overflow with one order but not in another.
            // Ignore the validation of sum if one of the two sums is null.
            if (actualIntegerStatistics == null ||
                    expectedIntegerStatistics == null ||
                    !Objects.equals(actualIntegerStatistics.getMin(), expectedIntegerStatistics.getMin()) ||
                    !Objects.equals(actualIntegerStatistics.getMax(), expectedIntegerStatistics.getMax()) ||
                    (actualIntegerStatistics.getSum() != null &&
                            expectedIntegerStatistics.getSum() != null &&
                            !Objects.equals(actualIntegerStatistics.getSum(), expectedIntegerStatistics.getSum()))) {
                throw new OrcCorruptionException(orcDataSourceId, "Write validation failed: unexpected integer range in %s statistics", name);
            }
        }
        if (!Objects.equals(actualColumnStatistics.getDoubleStatistics(), expectedColumnStatistics.getDoubleStatistics())) {
            throw new OrcCorruptionException(orcDataSourceId, "Write validation failed: unexpected double range in %s statistics", name);
        }
        StringStatistics expectedStringStatistics = expectedColumnStatistics.getStringStatistics();
        if (expectedStringStatistics != null) {
            expectedStringStatistics = new StringStatistics(
                    minStringTruncateToValidRange(expectedStringStatistics.getMin(), HiveWriterVersion.ORC_HIVE_8732),
                    maxStringTruncateToValidRange(expectedStringStatistics.getMax(), HiveWriterVersion.ORC_HIVE_8732),
                    expectedStringStatistics.getSum());
        }
        StringStatistics actualStringStatistics = actualColumnStatistics.getStringStatistics();
        if (!Objects.equals(actualColumnStatistics.getStringStatistics(), expectedStringStatistics) && expectedStringStatistics != null) {
            // expectedStringStatistics (or the min/max of it) could be null while the actual one might not because
            // expectedStringStatistics is calculated by merging all row group stats in the stripe but the actual one is by scanning each row in the stripe on disk.
            // Merging row group stats can produce nulls given we have string stats limit.
            if (actualStringStatistics == null ||
                    actualStringStatistics.getSum() != expectedStringStatistics.getSum() ||
                    (expectedStringStatistics.getMax() != null && !Objects.equals(actualStringStatistics.getMax(), expectedStringStatistics.getMax())) ||
                    (expectedStringStatistics.getMin() != null && !Objects.equals(actualStringStatistics.getMin(), expectedStringStatistics.getMin()))) {
                throw new OrcCorruptionException(orcDataSourceId, "Write validation failed: unexpected string range in %s statistics", name);
            }
        }
        if (!Objects.equals(actualColumnStatistics.getDateStatistics(), expectedColumnStatistics.getDateStatistics())) {
            throw new OrcCorruptionException(orcDataSourceId, "Write validation failed: unexpected date range in %s statistics", name);
        }
        if (!Objects.equals(actualColumnStatistics.getDecimalStatistics(), expectedColumnStatistics.getDecimalStatistics())) {
            throw new OrcCorruptionException(orcDataSourceId, "Write validation failed: unexpected decimal range in %s statistics", name);
        }
        if (!Objects.equals(actualColumnStatistics.getBloomFilter(), expectedColumnStatistics.getBloomFilter())) {
            throw new OrcCorruptionException(orcDataSourceId, "Write validation failed: unexpected bloom filter in %s statistics", name);
        }
    }

    public static class WriteChecksum
    {
        private final long totalRowCount;
        private final long stripeHash;
        private final List<Long> columnHashes;

        public WriteChecksum(long totalRowCount, long stripeHash, List<Long> columnHashes)
        {
            this.totalRowCount = totalRowCount;
            this.stripeHash = stripeHash;
            this.columnHashes = columnHashes;
        }

        public long getTotalRowCount()
        {
            return totalRowCount;
        }

        public long getStripeHash()
        {
            return stripeHash;
        }

        public List<Long> getColumnHashes()
        {
            return columnHashes;
        }
    }

    public static class WriteChecksumBuilder
    {
        private static final long NULL_HASH_CODE = 0x6e3efbd56c16a0cbL;

        private final List<Type> types;
        private long totalRowCount;
        private final List<XxHash64> columnHashes;
        private final XxHash64 stripeHash = new XxHash64();

        private final byte[] longBuffer = new byte[Long.BYTES];
        private final Slice longSlice = Slices.wrappedBuffer(longBuffer);

        private WriteChecksumBuilder(List<Type> types)
        {
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));

            ImmutableList.Builder<XxHash64> columnHashes = ImmutableList.builder();
            for (Type ignored : types) {
                columnHashes.add(new XxHash64());
            }
            this.columnHashes = columnHashes.build();
        }

        public static WriteChecksumBuilder createWriteChecksumBuilder(Map<Integer, Type> readColumns)
        {
            requireNonNull(readColumns, "readColumns is null");
            checkArgument(!readColumns.isEmpty(), "readColumns is empty");
            int columnCount = readColumns.keySet().stream()
                    .mapToInt(Integer::intValue)
                    .max().getAsInt() + 1;
            checkArgument(readColumns.size() == columnCount, "checksum requires all columns to be read");

            ImmutableList.Builder<Type> types = ImmutableList.builder();
            for (int column = 0; column < columnCount; column++) {
                Type type = readColumns.get(column);
                checkArgument(type != null, "checksum requires all columns to be read");
                types.add(type);
            }
            return new WriteChecksumBuilder(types.build());
        }

        public void addStripe(long rowCount)
        {
            longSlice.setLong(0, rowCount);
            stripeHash.update(longBuffer, 0, Long.BYTES);
        }

        public void addPage(Page page)
        {
            requireNonNull(page, "page is null");
            checkArgument(page.getChannelCount() == columnHashes.size(), "invalid page");

            for (int channel = 0; channel < columnHashes.size(); channel++) {
                Type type = types.get(channel);
                Block block = page.getBlock(channel);
                XxHash64 xxHash64 = columnHashes.get(channel);
                for (int position = 0; position < block.getPositionCount(); position++) {
                    long hash = hashPositionSkipNullMapKeys(type, block, position);
                    longSlice.setLong(0, hash);
                    xxHash64.update(longBuffer);
                }
            }
            totalRowCount += page.getPositionCount();
        }

        private static long hashPositionSkipNullMapKeys(Type type, Block block, int position)
        {
            if (block.isNull(position)) {
                return NULL_HASH_CODE;
            }

            if (type.getTypeSignature().getBase().equals(MAP)) {
                Type keyType = type.getTypeParameters().get(0);
                Type valueType = type.getTypeParameters().get(1);
                Block mapBlock = (Block) type.getObject(block, position);
                long hash = 0;
                for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                    if (!mapBlock.isNull(i)) {
                        hash += hashPositionSkipNullMapKeys(keyType, mapBlock, i);
                        hash += hashPositionSkipNullMapKeys(valueType, mapBlock, i + 1);
                    }
                }
                return hash;
            }

            if (type.getTypeSignature().getBase().equals(ARRAY)) {
                Type elementType = type.getTypeParameters().get(0);
                Block array = (Block) type.getObject(block, position);
                long hash = 0;
                for (int i = 0; i < array.getPositionCount(); i++) {
                    hash = 31 * hash + hashPositionSkipNullMapKeys(elementType, array, i);
                }
                return hash;
            }

            if (type.getTypeSignature().getBase().equals(ROW)) {
                Block row = (Block) type.getObject(block, position);
                long hash = 0;
                for (int i = 0; i < row.getPositionCount(); i++) {
                    Type elementType = type.getTypeParameters().get(i);
                    hash = 31 * hash + hashPositionSkipNullMapKeys(elementType, row, i);
                }
                return hash;
            }

            if (type.getTypeSignature().getBase().equals(StandardTypes.TIMESTAMP)) {
                // A flaw in ORC encoding makes it impossible to represent timestamp
                // between 1969-12-31 23:59:59.000, exclusive, and 1970-01-01 00:00:00.000, exclusive.
                // Therefore, such data won't round trip. The data read back is expected to be 1 second later than the original value.
                long mills = TIMESTAMP.getLong(block, position);
                if (mills > -1000 && mills < 0) {
                    return AbstractLongType.hash(mills + 1000);
                }
            }

            return type.hash(block, position);
        }

        public WriteChecksum build()
        {
            return new WriteChecksum(
                    totalRowCount,
                    stripeHash.hash(),
                    columnHashes.stream()
                            .map(XxHash64::hash)
                            .collect(toImmutableList()));
        }
    }

    public class StatisticsValidation
    {
        private final List<Type> types;
        private List<ColumnStatisticsValidation> columnStatisticsValidations;
        private long rowCount;

        private StatisticsValidation(List<Type> types)
        {
            this.types = requireNonNull(types, "types is null");
            columnStatisticsValidations = types.stream()
                    .map(ColumnStatisticsValidation::new)
                    .collect(toImmutableList());
        }

        public void reset()
        {
            rowCount = 0;
            columnStatisticsValidations = types.stream()
                    .map(ColumnStatisticsValidation::new)
                    .collect(toImmutableList());
        }

        public void addPage(Page page)
        {
            rowCount += page.getPositionCount();
            for (int channel = 0; channel < columnStatisticsValidations.size(); channel++) {
                columnStatisticsValidations.get(channel).addBlock(page.getBlock(channel));
            }
        }

        public List<ColumnStatistics> build()
        {
            ImmutableList.Builder<ColumnStatistics> statisticsBuilders = ImmutableList.builder();
            // if there are no rows, there will be no stats
            if (rowCount > 0) {
                statisticsBuilders.add(new ColumnStatistics(rowCount, null));
                columnStatisticsValidations.forEach(validation -> validation.build(statisticsBuilders));
            }
            return statisticsBuilders.build();
        }
    }

    private class ColumnStatisticsValidation
    {
        private final Type type;
        private final StatisticsBuilder statisticsBuilder;
        private final Function<Block, List<Block>> fieldExtractor;
        private final List<ColumnStatisticsValidation> fieldBuilders;

        private ColumnStatisticsValidation(Type type)
        {
            this.type = requireNonNull(type, "type is null");

            if (BOOLEAN.equals(type)) {
                statisticsBuilder = new BooleanStatisticsBuilder();
                fieldExtractor = ignored -> ImmutableList.of();
                fieldBuilders = ImmutableList.of();
            }
            else if (TINYINT.equals(type)) {
                statisticsBuilder = new CountStatisticsBuilder();
                fieldExtractor = ignored -> ImmutableList.of();
                fieldBuilders = ImmutableList.of();
            }
            else if (SMALLINT.equals(type)) {
                statisticsBuilder = new IntegerStatisticsBuilder();
                fieldExtractor = ignored -> ImmutableList.of();
                fieldBuilders = ImmutableList.of();
            }
            else if (INTEGER.equals(type)) {
                statisticsBuilder = new IntegerStatisticsBuilder();
                fieldExtractor = ignored -> ImmutableList.of();
                fieldBuilders = ImmutableList.of();
            }
            else if (BIGINT.equals(type)) {
                statisticsBuilder = new IntegerStatisticsBuilder();
                fieldExtractor = ignored -> ImmutableList.of();
                fieldBuilders = ImmutableList.of();
            }
            else if (DOUBLE.equals(type)) {
                statisticsBuilder = new DoubleStatisticsBuilder();
                fieldExtractor = ignored -> ImmutableList.of();
                fieldBuilders = ImmutableList.of();
            }
            else if (REAL.equals(type)) {
                statisticsBuilder = new DoubleStatisticsBuilder();
                fieldExtractor = ignored -> ImmutableList.of();
                fieldBuilders = ImmutableList.of();
            }
            else if (type instanceof VarcharType) {
                statisticsBuilder = new StringStatisticsBuilder(stringStatisticsLimitInBytes);
                fieldExtractor = ignored -> ImmutableList.of();
                fieldBuilders = ImmutableList.of();
            }
            else if (type instanceof CharType) {
                statisticsBuilder = new StringStatisticsBuilder(stringStatisticsLimitInBytes);
                fieldExtractor = ignored -> ImmutableList.of();
                fieldBuilders = ImmutableList.of();
            }
            else if (VARBINARY.equals(type)) {
                statisticsBuilder = new BinaryStatisticsBuilder();
                fieldExtractor = ignored -> ImmutableList.of();
                fieldBuilders = ImmutableList.of();
            }
            else if (DATE.equals(type)) {
                statisticsBuilder = new DateStatisticsBuilder();
                fieldExtractor = ignored -> ImmutableList.of();
                fieldBuilders = ImmutableList.of();
            }
            else if (TIMESTAMP.equals(type)) {
                statisticsBuilder = new CountStatisticsBuilder();
                fieldExtractor = ignored -> ImmutableList.of();
                fieldBuilders = ImmutableList.of();
            }
            else if (type instanceof DecimalType) {
                DecimalType decimalType = (DecimalType) type;
                if (decimalType.isShort()) {
                    statisticsBuilder = new ShortDecimalStatisticsBuilder((decimalType).getScale());
                }
                else {
                    statisticsBuilder = new LongDecimalStatisticsBuilder();
                }
                fieldExtractor = ignored -> ImmutableList.of();
                fieldBuilders = ImmutableList.of();
            }
            else if (type.getTypeSignature().getBase().equals(ARRAY)) {
                statisticsBuilder = new CountStatisticsBuilder();
                fieldExtractor = block -> ImmutableList.of(toColumnarArray(block).getElementsBlock());
                fieldBuilders = ImmutableList.of(new ColumnStatisticsValidation(Iterables.getOnlyElement(type.getTypeParameters())));
            }
            else if (type.getTypeSignature().getBase().equals(MAP)) {
                statisticsBuilder = new CountStatisticsBuilder();
                fieldExtractor = block -> {
                    ColumnarMap columnarMap = toColumnarMap(block);
                    return ImmutableList.of(columnarMap.getKeysBlock(), columnarMap.getValuesBlock());
                };
                fieldBuilders = type.getTypeParameters().stream()
                        .map(ColumnStatisticsValidation::new)
                        .collect(toImmutableList());
            }
            else if (type.getTypeSignature().getBase().equals(ROW)) {
                statisticsBuilder = new CountStatisticsBuilder();
                fieldExtractor = block -> {
                    ColumnarRow columnarRow = ColumnarRow.toColumnarRow(block);
                    ImmutableList.Builder<Block> fields = ImmutableList.builder();
                    for (int index = 0; index < columnarRow.getFieldCount(); index++) {
                        fields.add(columnarRow.getField(index));
                    }
                    return fields.build();
                };
                fieldBuilders = type.getTypeParameters().stream()
                        .map(ColumnStatisticsValidation::new)
                        .collect(toImmutableList());
            }
            else {
                throw new GenericInternalException(format("Unsupported Hive type: %s", type));
            }
        }

        private void addBlock(Block block)
        {
            statisticsBuilder.addBlock(type, block);

            List<Block> fields = fieldExtractor.apply(block);
            for (int i = 0; i < fieldBuilders.size(); i++) {
                fieldBuilders.get(i).addBlock(fields.get(i));
            }
        }

        private void build(ImmutableList.Builder<ColumnStatistics> output)
        {
            output.add(statisticsBuilder.buildColumnStatistics());
            fieldBuilders.forEach(fieldBuilders -> fieldBuilders.build(output));
        }
    }

    private static class CountStatisticsBuilder
            implements StatisticsBuilder
    {
        private long rowCount;

        @Override
        public void addBlock(Type type, Block block)
        {
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (!block.isNull(position)) {
                    rowCount++;
                }
            }
        }

        @Override
        public ColumnStatistics buildColumnStatistics()
        {
            return new ColumnStatistics(rowCount, null);
        }
    }

    private static class RowGroupStatistics
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(RowGroupStatistics.class).instanceSize();

        private final OrcWriteValidationMode validationMode;
        private final SortedMap<Integer, ColumnStatistics> columnStatistics;
        private final long hash;

        public RowGroupStatistics(OrcWriteValidationMode validationMode, Map<Integer, ColumnStatistics> columnStatistics)
        {
            this.validationMode = validationMode;

            requireNonNull(columnStatistics, "columnStatistics is null");
            if (validationMode == HASHED) {
                this.columnStatistics = ImmutableSortedMap.of();
                hash = hashColumnStatistics(ImmutableSortedMap.copyOf(columnStatistics));
            }
            else if (validationMode == DETAILED) {
                this.columnStatistics = ImmutableSortedMap.copyOf(columnStatistics);
                hash = 0;
            }
            else if (validationMode == BOTH) {
                this.columnStatistics = ImmutableSortedMap.copyOf(columnStatistics);
                hash = hashColumnStatistics(this.columnStatistics);
            }
            else {
                throw new IllegalArgumentException("Unsupported validation mode");
            }
        }

        private static long hashColumnStatistics(SortedMap<Integer, ColumnStatistics> columnStatistics)
        {
            StatisticsHasher statisticsHasher = new StatisticsHasher();
            statisticsHasher.putInt(columnStatistics.size());
            for (Entry<Integer, ColumnStatistics> entry : columnStatistics.entrySet()) {
                statisticsHasher.putInt(entry.getKey())
                        .putOptionalHashable(entry.getValue());
            }
            return statisticsHasher.hash();
        }

        public OrcWriteValidationMode getValidationMode()
        {
            return validationMode;
        }

        public Map<Integer, ColumnStatistics> getColumnStatistics()
        {
            verify(validationMode != HASHED, "columnStatistics are not available in HASHED mode");
            return columnStatistics;
        }

        public long getHash()
        {
            return hash;
        }
    }

    public static class OrcWriteValidationBuilder
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(OrcWriteValidationBuilder.class).instanceSize();

        private final OrcWriteValidationMode validationMode;

        private List<Integer> version;
        private CompressionKind compression;
        private int rowGroupMaxRowCount;
        private int stringStatisticsLimitInBytes;
        private List<String> columnNames;
        private final Map<String, Slice> metadata = new HashMap<>();
        private final WriteChecksumBuilder checksum;
        private List<RowGroupStatistics> currentRowGroupStatistics = new ArrayList<>();
        private final Map<Long, List<RowGroupStatistics>> rowGroupStatisticsByStripe = new HashMap<>();
        private final Map<Long, StripeStatistics> stripeStatistics = new HashMap<>();
        private List<ColumnStatistics> fileStatistics;
        private long retainedSize = INSTANCE_SIZE;

        public OrcWriteValidationBuilder(OrcWriteValidationMode validationMode, List<Type> types)
        {
            this.validationMode = validationMode;
            this.checksum = new WriteChecksumBuilder(types);
        }

        public long getRetainedSize()
        {
            return retainedSize;
        }

        public OrcWriteValidationBuilder setVersion(List<Integer> version)
        {
            this.version = ImmutableList.copyOf(version);
            return this;
        }

        public void setCompression(CompressionKind compression)
        {
            this.compression = compression;
        }

        public void setRowGroupMaxRowCount(int rowGroupMaxRowCount)
        {
            this.rowGroupMaxRowCount = rowGroupMaxRowCount;
        }

        public OrcWriteValidationBuilder setStringStatisticsLimitInBytes(int stringStatisticsLimitInBytes)
        {
            this.stringStatisticsLimitInBytes = stringStatisticsLimitInBytes;
            return this;
        }

        public OrcWriteValidationBuilder setColumnNames(List<String> columnNames)
        {
            this.columnNames = ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null"));
            return this;
        }

        public OrcWriteValidationBuilder addMetadataProperty(String key, Slice value)
        {
            metadata.put(key, value);
            return this;
        }

        public OrcWriteValidationBuilder addStripe(long rowCount)
        {
            checksum.addStripe(rowCount);
            return this;
        }

        public OrcWriteValidationBuilder addPage(Page page)
        {
            checksum.addPage(page);
            return this;
        }

        public void addRowGroupStatistics(Map<Integer, ColumnStatistics> columnStatistics)
        {
            RowGroupStatistics rowGroupStatistics = new RowGroupStatistics(validationMode, columnStatistics);
            currentRowGroupStatistics.add(rowGroupStatistics);

            retainedSize += RowGroupStatistics.INSTANCE_SIZE;
            if (validationMode != HASHED) {
                for (ColumnStatistics statistics : rowGroupStatistics.getColumnStatistics().values()) {
                    retainedSize += Integer.BYTES + statistics.getRetainedSizeInBytes();
                }
            }
        }

        public void addStripeStatistics(long stripStartOffset, StripeStatistics columnStatistics)
        {
            stripeStatistics.put(stripStartOffset, columnStatistics);
            rowGroupStatisticsByStripe.put(stripStartOffset, currentRowGroupStatistics);
            currentRowGroupStatistics = new ArrayList<>();
        }

        public void setFileStatistics(List<ColumnStatistics> fileStatistics)
        {
            this.fileStatistics = fileStatistics;
        }

        public OrcWriteValidation build()
        {
            return new OrcWriteValidation(
                    version,
                    compression,
                    rowGroupMaxRowCount,
                    columnNames,
                    metadata,
                    checksum.build(),
                    rowGroupStatisticsByStripe,
                    stripeStatistics,
                    fileStatistics,
                    stringStatisticsLimitInBytes);
        }
    }
}
