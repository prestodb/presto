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
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.StringStatistics;
import com.facebook.presto.orc.metadata.statistics.StripeStatistics;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static com.facebook.presto.orc.metadata.OrcMetadataReader.maxStringTruncateToValidRange;
import static com.facebook.presto.orc.metadata.OrcMetadataReader.minStringTruncateToValidRange;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
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

    private OrcWriteValidation(
            List<Integer> version,
            CompressionKind compression,
            int rowGroupMaxRowCount,
            List<String> columnNames,
            Map<String, Slice> metadata,
            WriteChecksum checksum,
            Map<Long, List<RowGroupStatistics>> rowGroupStatistics,
            Map<Long, StripeStatistics> stripeStatistics,
            List<ColumnStatistics> fileStatistics)
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
            StripeStatistics expected = stripeStatistics.get(stripeOffset);
            if (expected == null) {
                throw new OrcCorruptionException(orcDataSourceId, "Unexpected stripe at offset %s", stripeOffset);
            }
            validateColumnStatisticsEquivalent(orcDataSourceId, "Stripe at " + stripeOffset, actual.getColumnStatistics(), expected.getColumnStatistics());
        }
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
        if (!Objects.equals(actualColumnStatistics.getStringStatistics(), expectedStringStatistics)) {
            throw new OrcCorruptionException(orcDataSourceId, "Write validation failed: unexpected string range in %s statistics", name);
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

            if (type.getTypeSignature().getBase().equals(StandardTypes.MAP)) {
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

            if (type.getTypeSignature().getBase().equals(StandardTypes.ARRAY)) {
                Type elementType = type.getTypeParameters().get(0);
                Block array = (Block) type.getObject(block, position);
                long hash = 0;
                for (int i = 0; i < array.getPositionCount(); i++) {
                    hash = 31 * hash + hashPositionSkipNullMapKeys(elementType, array, i);
                }
                return hash;
            }

            if (type.getTypeSignature().getBase().equals(StandardTypes.ROW)) {
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
                    fileStatistics);
        }
    }
}
