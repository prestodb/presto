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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.OrcFileWriter;
import com.facebook.presto.orc.DwrfEncryptionProvider;
import com.facebook.presto.orc.DwrfWriterEncryption;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.OrcWriteValidation;
import com.facebook.presto.orc.OrcWriterOptions;
import com.facebook.presto.orc.OrcWriterStats;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.DateStatistics;
import com.facebook.presto.orc.metadata.statistics.DecimalStatistics;
import com.facebook.presto.orc.metadata.statistics.DoubleStatistics;
import com.facebook.presto.orc.metadata.statistics.IntegerStatistics;
import com.facebook.presto.orc.metadata.statistics.StringStatistics;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Conversions;
import org.joda.time.DateTimeZone;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static com.facebook.presto.iceberg.TypeConverter.ORC_ICEBERG_ID_KEY;
import static com.google.common.base.Verify.verify;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.types.Types.DecimalType;
import static org.apache.iceberg.types.Types.NestedField;

public class IcebergOrcFileWriter
        extends OrcFileWriter
        implements IcebergFileWriter
{
    private final Schema icebergSchema;
    private final List<OrcType> orcColumn;

    public IcebergOrcFileWriter(
            Schema icebergSchema,
            DataSink dataSink,
            Callable<Void> rollbackAction,
            OrcEncoding orcEncoding,
            List<String> columnNames,
            List<Type> fileColumnTypes,
            List<OrcType> fileColumnOrcTypes,
            CompressionKind compression,
            OrcWriterOptions options,
            int[] fileInputColumnIndexes,
            Map<String, String> metadata,
            DateTimeZone hiveStorageTimeZone,
            Optional<Supplier<OrcDataSource>> validationInputFactory,
            OrcWriteValidation.OrcWriteValidationMode validationMode,
            OrcWriterStats stats,
            DwrfEncryptionProvider dwrfEncryptionProvider,
            Optional<DwrfWriterEncryption> dwrfWriterEncryption)
    {
        super(dataSink, rollbackAction, orcEncoding, columnNames, fileColumnTypes, Optional.ofNullable(fileColumnOrcTypes), compression, options, fileInputColumnIndexes, metadata, hiveStorageTimeZone, validationInputFactory, validationMode, stats, dwrfEncryptionProvider, dwrfWriterEncryption);
        this.icebergSchema = requireNonNull(icebergSchema, "icebergSchema is null");
        this.orcColumn = fileColumnOrcTypes;
    }

    @Override
    public Metrics getMetrics()
    {
        return computeMetrics(icebergSchema, orcColumn, orcWriter.getFileRowCount(), orcWriter.getFileStats());
    }

    private static Metrics computeMetrics(Schema icebergSchema, List<OrcType> orcRowTypes, long fileRowCount, List<ColumnStatistics> columnStatistics)
    {
        if (columnStatistics.isEmpty()) {
            return new Metrics(fileRowCount, null, null, null, null, null);
        }
        // Columns that are descendants of LIST or MAP types are excluded because:
        // 1. Their stats are not used by Apache Iceberg to filter out data files
        // 2. Their record count can be larger than table-level row count. There's no good way to calculate nullCounts for them.
        // See https://github.com/apache/iceberg/pull/199#discussion_r429443627
        Set<Integer> excludedColumns = getExcludedColumns(orcRowTypes);

        ImmutableMap.Builder<Integer, Long> valueCountsBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<Integer, Long> nullCountsBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<Integer, ByteBuffer> lowerBoundsBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<Integer, ByteBuffer> upperBoundsBuilder = ImmutableMap.builder();

        // OrcColumnId(0) is the root column that represents file-level schema
        for (int i = 1; i < orcRowTypes.size(); i++) {
            if (excludedColumns.contains(i)) {
                continue;
            }
            OrcType orcColumn = orcRowTypes.get(i);
            ColumnStatistics orcColumnStats = columnStatistics.get(i);
            int icebergId = getIcebergId(orcColumn);
            NestedField icebergField = icebergSchema.findField(icebergId);
            verify(icebergField != null, "Cannot find Iceberg column with ID %s in schema %s", icebergId, icebergSchema);
            valueCountsBuilder.put(icebergId, fileRowCount);
            if (orcColumnStats.hasNumberOfValues()) {
                nullCountsBuilder.put(icebergId, fileRowCount - orcColumnStats.getNumberOfValues());
            }
            toIcebergMinMax(orcColumnStats, icebergField.type()).ifPresent(minMax -> {
                lowerBoundsBuilder.put(icebergId, minMax.getMin());
                upperBoundsBuilder.put(icebergId, minMax.getMax());
            });
        }
        Map<Integer, Long> valueCounts = valueCountsBuilder.build();
        Map<Integer, Long> nullCounts = nullCountsBuilder.build();
        Map<Integer, ByteBuffer> lowerBounds = lowerBoundsBuilder.build();
        Map<Integer, ByteBuffer> upperBounds = upperBoundsBuilder.build();
        return new Metrics(
                fileRowCount,
                null, // TODO: Add column size accounting to ORC column writers
                valueCounts.isEmpty() ? null : valueCounts,
                nullCounts.isEmpty() ? null : nullCounts,
                lowerBounds.isEmpty() ? null : lowerBounds,
                upperBounds.isEmpty() ? null : upperBounds);
    }

    private static Set<Integer> getExcludedColumns(List<OrcType> orcRowTypes)
    {
        ImmutableSet.Builder<Integer> excludedColumns = ImmutableSet.builder();
        populateExcludedColumns(orcRowTypes, 0, false, excludedColumns);
        return excludedColumns.build();
    }

    private static void populateExcludedColumns(List<OrcType> orcRowTypes, int orcColumnId, boolean exclude, ImmutableSet.Builder<Integer> excludedColumns)
    {
        if (exclude) {
            excludedColumns.add(orcColumnId);
        }
        OrcType orcColumn = orcRowTypes.get(orcColumnId);
        switch (orcColumn.getOrcTypeKind()) {
            case LIST:
            case MAP:
                for (Integer child : orcColumn.getFieldTypeIndexes()) {
                    populateExcludedColumns(orcRowTypes, child, true, excludedColumns);
                }
                return;
            case STRUCT:
                for (Integer child : orcColumn.getFieldTypeIndexes()) {
                    populateExcludedColumns(orcRowTypes, child, exclude, excludedColumns);
                }
                return;
        }
    }

    private static int getIcebergId(OrcType orcColumn)
    {
        String icebergId = orcColumn.getAttributes().get(ORC_ICEBERG_ID_KEY);
        verify(icebergId != null, "ORC column %s doesn't have an associated Iceberg ID", orcColumn);
        return Integer.parseInt(icebergId);
    }

    private static Optional<IcebergMinMax> toIcebergMinMax(ColumnStatistics orcColumnStats, org.apache.iceberg.types.Type icebergType)
    {
        IntegerStatistics integerStatistics = orcColumnStats.getIntegerStatistics();
        if (integerStatistics != null) {
            Object min = integerStatistics.getMin();
            Object max = integerStatistics.getMax();
            if (min == null || max == null) {
                return Optional.empty();
            }
            if (icebergType.typeId() == org.apache.iceberg.types.Type.TypeID.INTEGER) {
                min = toIntExact((Long) min);
                max = toIntExact((Long) max);
            }
            return Optional.of(new IcebergMinMax(icebergType, min, max));
        }
        DoubleStatistics doubleStatistics = orcColumnStats.getDoubleStatistics();
        if (doubleStatistics != null) {
            Object min = doubleStatistics.getMin();
            Object max = doubleStatistics.getMax();
            if (min == null || max == null) {
                return Optional.empty();
            }
            if (icebergType.typeId() == org.apache.iceberg.types.Type.TypeID.FLOAT) {
                min = ((Double) min).floatValue();
                max = ((Double) max).floatValue();
            }
            return Optional.of(new IcebergMinMax(icebergType, min, max));
        }
        StringStatistics stringStatistics = orcColumnStats.getStringStatistics();
        if (stringStatistics != null) {
            Slice min = stringStatistics.getMin();
            Slice max = stringStatistics.getMax();
            if (min == null || max == null) {
                return Optional.empty();
            }
            return Optional.of(new IcebergMinMax(icebergType, min.toStringUtf8(), max.toStringUtf8()));
        }
        DateStatistics dateStatistics = orcColumnStats.getDateStatistics();
        if (dateStatistics != null) {
            Integer min = dateStatistics.getMin();
            Integer max = dateStatistics.getMax();
            if (min == null || max == null) {
                return Optional.empty();
            }
            return Optional.of(new IcebergMinMax(icebergType, min, max));
        }
        DecimalStatistics decimalStatistics = orcColumnStats.getDecimalStatistics();
        if (decimalStatistics != null) {
            BigDecimal min = decimalStatistics.getMin();
            BigDecimal max = decimalStatistics.getMax();
            if (min == null || max == null) {
                return Optional.empty();
            }
            min = min.setScale(((DecimalType) icebergType).scale());
            max = max.setScale(((DecimalType) icebergType).scale());
            return Optional.of(new IcebergMinMax(icebergType, min, max));
        }
        return Optional.empty();
    }

    private static class IcebergMinMax
    {
        private ByteBuffer min;
        private ByteBuffer max;

        private IcebergMinMax(org.apache.iceberg.types.Type type, Object min, Object max)
        {
            this.min = Conversions.toByteBuffer(type, min);
            this.max = Conversions.toByteBuffer(type, max);
        }

        public ByteBuffer getMin()
        {
            return min;
        }

        public ByteBuffer getMax()
        {
            return max;
        }
    }
}
