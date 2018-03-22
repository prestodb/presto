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

import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.PostScript.HiveWriterVersion;
import com.facebook.presto.orc.metadata.RowGroupIndex;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.facebook.presto.orc.metadata.statistics.BinaryStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.BooleanStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.DateStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.DoubleStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.IntegerStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.LongDecimalStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.ShortDecimalStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.StatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.StringStatistics;
import com.facebook.presto.orc.metadata.statistics.StringStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.StripeStatistics;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.ColumnarMap;
import com.facebook.presto.spi.block.ColumnarRow;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.facebook.presto.orc.metadata.DwrfMetadataWriter.STATIC_METADATA;
import static com.facebook.presto.orc.metadata.OrcMetadataReader.maxStringTruncateToValidRange;
import static com.facebook.presto.orc.metadata.OrcMetadataReader.minStringTruncateToValidRange;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.block.ColumnarArray.toColumnarArray;
import static com.facebook.presto.spi.block.ColumnarMap.toColumnarMap;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OrcWriteValidation
{
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
        if (actualFileStatistics.isEmpty()) {
            // DWRF file statistics are disabled
            return;
        }
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

    public void validateRowGroupStatistics(OrcDataSourceId orcDataSourceId, long stripeOffset, Map<Integer, List<RowGroupIndex>> actualRowGroupStatistics)
            throws OrcCorruptionException
    {
        requireNonNull(actualRowGroupStatistics, "actualRowGroupStatistics is null");
        List<RowGroupStatistics> expectedRowGroupStatistics = rowGroupStatistics.get(stripeOffset);
        if (expectedRowGroupStatistics == null) {
            throw new OrcCorruptionException(orcDataSourceId, "Unexpected stripe at offset %s", stripeOffset);
        }

        int rowGroupCount = expectedRowGroupStatistics.size();
        for (Entry<Integer, List<RowGroupIndex>> entry : actualRowGroupStatistics.entrySet()) {
            if (entry.getValue().size() != rowGroupCount) {
                throw new OrcCorruptionException(orcDataSourceId, "Unexpected row group count stripe in at offset %s", stripeOffset);
            }
        }

        for (int rowGroupIndex = 0; rowGroupIndex < expectedRowGroupStatistics.size(); rowGroupIndex++) {
            Map<Integer, ColumnStatistics> expectedStatistics = expectedRowGroupStatistics.get(rowGroupIndex).getColumnStatistics();
            if (!expectedStatistics.keySet().equals(actualRowGroupStatistics.keySet())) {
                throw new OrcCorruptionException(orcDataSourceId, "Unexpected column in row group %s in stripe at offset %s", stripeOffset);
            }
            for (Entry<Integer, ColumnStatistics> entry : expectedStatistics.entrySet()) {
                int columnIndex = entry.getKey();
                ColumnStatistics actual = actualRowGroupStatistics.get(columnIndex).get(rowGroupIndex).getColumnStatistics();
                ColumnStatistics expected = entry.getValue();
                validateColumnStatisticsEquivalent(orcDataSourceId, "Row group " + rowGroupIndex + " in stripe at offset " + stripeOffset, actual, expected);
            }
        }
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
        Map<Integer, ColumnStatistics> expectedByColumnIndex = rowGroups.get(rowGroupIndex).getColumnStatistics();

        // new writer does not write row group stats for column zero (table row column)
        List<ColumnStatistics> expected = IntStream.range(1, actual.size())
                .mapToObj(expectedByColumnIndex::get)
                .collect(toImmutableList());
        actual = actual.subList(1, actual.size());

        validateColumnStatisticsEquivalent(orcDataSourceId, "Row group " + rowGroupIndex + " in stripe at offset " + stripeOffset, actual, expected);
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
            throw new OrcCorruptionException(orcDataSourceId, "Write validation failed: unexpected integer range in %s statistics", name);
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

        public void addStripe(int rowCount)
        {
            longSlice.setInt(0, rowCount);
            stripeHash.update(longBuffer, 0, Integer.BYTES);
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
                statisticsBuilders.add(new ColumnStatistics(rowCount, 0, null, null, null, null, null, null, null, null));
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
                throw new PrestoException(NOT_SUPPORTED, format("Unsupported Hive type: %s", type));
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
            return new ColumnStatistics(rowCount, 0, null, null, null, null, null, null, null, null);
        }
    }

    private static class RowGroupStatistics
    {
        private final Map<Integer, ColumnStatistics> columnStatistics;

        public RowGroupStatistics(Map<Integer, ColumnStatistics> columnStatistics)
        {
            this.columnStatistics = ImmutableMap.copyOf(requireNonNull(columnStatistics, "columnStatistics is null"));
        }

        public Map<Integer, ColumnStatistics> getColumnStatistics()
        {
            return columnStatistics;
        }
    }

    public static class OrcWriteValidationBuilder
    {
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

        public OrcWriteValidationBuilder(List<Type> types)
        {
            this.checksum = new WriteChecksumBuilder(types);
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

        public OrcWriteValidationBuilder addStripe(int rowCount)
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
            currentRowGroupStatistics.add(new RowGroupStatistics(columnStatistics));
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
