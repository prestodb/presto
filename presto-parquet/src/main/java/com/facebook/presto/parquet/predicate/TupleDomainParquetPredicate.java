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
package com.facebook.presto.parquet.predicate;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.parquet.DictionaryPage;
import com.facebook.presto.parquet.ParquetDataSourceId;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.dictionary.Dictionary;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.WarningCollector;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.hive.HiveWarningCode.HIVE_FILE_STATISTICS_CORRUPTION;
import static com.facebook.presto.parquet.predicate.PredicateUtils.isStatisticsOverflow;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.format;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

public class TupleDomainParquetPredicate
        implements Predicate
{
    private final TupleDomain<ColumnDescriptor> effectivePredicate;
    private final List<RichColumnDescriptor> columns;
    private final ColumnIndexValueConverter converter;

    public TupleDomainParquetPredicate(TupleDomain<ColumnDescriptor> effectivePredicate, List<RichColumnDescriptor> columns)
    {
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.converter = new ColumnIndexValueConverter(columns);
    }

    private static boolean effectivePredicateMatches(Domain effectivePredicateDomain, DictionaryDescriptor dictionary)
    {
        return !effectivePredicateDomain.intersect(getDomain(effectivePredicateDomain.getType(), dictionary)).isNone();
    }

    @VisibleForTesting
    public static Domain getDomain(
            ColumnDescriptor column,
            Type type,
            long rowCount,
            Statistics<?> statistics,
            ParquetDataSourceId id,
            Optional<WarningCollector> warningCollector)
    {
        if (statistics == null || statistics.isEmpty()) {
            return Domain.all(type);
        }

        if (statistics.getNumNulls() == rowCount) {
            return Domain.onlyNull(type);
        }

        boolean hasNullValue = statistics.getNumNulls() != 0L;

        if (!statistics.hasNonNullValue() || statistics.genericGetMin() == null || statistics.genericGetMax() == null) {
            return Domain.create(ValueSet.all(type), hasNullValue);
        }

        try {
            return getDomain(
                    column,
                    type,
                    ImmutableList.of(statistics.genericGetMin()),
                    ImmutableList.of(statistics.genericGetMax()),
                    hasNullValue);
        }
        catch (Exception exception) {
            if (warningCollector.isPresent()) {
                warningCollector.get().add(new PrestoWarning(HIVE_FILE_STATISTICS_CORRUPTION, format("Corrupted statistics for column \"%s\" in Parquet file \"%s\": [%s]", column.toString(), id, statistics)));
            }
            return Domain.create(ValueSet.all(type), hasNullValue);
        }
    }

    /**
     * Get a domain for the ranges defined by each pair of elements from {@code minimums} and {@code maximums}.
     * Both arrays must have the same length.
     */
    private static Domain getDomain(
            ColumnDescriptor column,
            Type type,
            List<Object> minimums,
            List<Object> maximums,
            boolean hasNullValue)
    {
        checkArgument(minimums.size() == maximums.size(), "Expected minimums and maximums to have the same size");

        List<Range> ranges = new ArrayList<>();
        if (type.equals(BOOLEAN)) {
            boolean hasTrueValues = minimums.stream().anyMatch(value -> (boolean) value) || maximums.stream().anyMatch(value -> (boolean) value);
            boolean hasFalseValues = minimums.stream().anyMatch(value -> !(boolean) value) || maximums.stream().anyMatch(value -> !(boolean) value);
            if (hasTrueValues && hasFalseValues) {
                return Domain.all(type);
            }
            if (hasTrueValues) {
                return Domain.create(ValueSet.of(type, true), hasNullValue);
            }
            if (hasFalseValues) {
                return Domain.create(ValueSet.of(type, false), hasNullValue);
            }
            // All nulls case is handled earlier
            throw new VerifyException("Impossible boolean statistics");
        }

        if ((type.equals(BIGINT) || type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER))) {
            for (int i = 0; i < minimums.size(); i++) {
                long min = asLong(minimums.get(i));
                long max = asLong(maximums.get(i));
                if (isStatisticsOverflow(type, min, max)) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }

                ranges.add(Range.range(type, min, true, max, true));
            }
            checkArgument(!ranges.isEmpty(), "cannot use empty ranges");
            return Domain.create(ValueSet.ofRanges(ranges), hasNullValue);
        }

        if (type.equals(REAL)) {
            for (int i = 0; i < minimums.size(); i++) {
                Float min = (Float) minimums.get(i);
                Float max = (Float) maximums.get(i);

                if (min.isNaN() || max.isNaN()) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }
                ranges.add(Range.range(type, (long) floatToRawIntBits(min), true, (long) floatToRawIntBits(max), true));
            }
            checkArgument(!ranges.isEmpty(), "cannot use empty ranges");
            return Domain.create(ValueSet.ofRanges(ranges), hasNullValue);
        }

        if (type.equals(DOUBLE)) {
            for (int i = 0; i < minimums.size(); i++) {
                Double min = (Double) minimums.get(i);
                Double max = (Double) maximums.get(i);
                if (min.isNaN() || max.isNaN()) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }

                ranges.add(Range.range(type, min, true, max, true));
            }
            checkArgument(!ranges.isEmpty(), "cannot use empty ranges");
            return Domain.create(ValueSet.ofRanges(ranges), hasNullValue);
        }

        if (isVarcharType(type)) {
            for (int i = 0; i < minimums.size(); i++) {
                Slice min = Slices.wrappedBuffer(((Binary) minimums.get(i)).toByteBuffer());
                Slice max = Slices.wrappedBuffer(((Binary) maximums.get(i)).toByteBuffer());
                ranges.add(Range.range(type, min, true, max, true));
            }
            checkArgument(!ranges.isEmpty(), "cannot use empty ranges");
            return Domain.create(ValueSet.ofRanges(ranges), hasNullValue);
        }

        if (type.equals(DATE)) {
            for (int i = 0; i < minimums.size(); i++) {
                long min = asLong(minimums.get(i));
                long max = asLong(maximums.get(i));
                if (isStatisticsOverflow(type, min, max)) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }
                ranges.add(Range.range(type, min, true, max, true));
            }
            checkArgument(!ranges.isEmpty(), "cannot use empty ranges");
            return Domain.create(ValueSet.ofRanges(ranges), hasNullValue);
        }
        return Domain.create(ValueSet.all(type), hasNullValue);
    }

    @VisibleForTesting
    public static Domain getDomain(Type type, DictionaryDescriptor dictionaryDescriptor)
    {
        if (dictionaryDescriptor == null) {
            return Domain.all(type);
        }

        ColumnDescriptor columnDescriptor = dictionaryDescriptor.getColumnDescriptor();
        Optional<DictionaryPage> dictionaryPage = dictionaryDescriptor.getDictionaryPage();
        if (!dictionaryPage.isPresent()) {
            return Domain.all(type);
        }

        Dictionary dictionary;
        try {
            dictionary = dictionaryPage.get().getEncoding().initDictionary(columnDescriptor, dictionaryPage.get());
        }
        catch (Exception e) {
            // In case of exception, just continue reading the data, not using dictionary page at all
            // OK to ignore exception when reading dictionaries
            return Domain.all(type);
        }

        int dictionarySize = dictionaryPage.get().getDictionarySize();
        DictionaryValueConverter converter = new DictionaryValueConverter(dictionary);
        Function<Integer, Object> convertFunction = converter.getConverter(columnDescriptor.getPrimitiveType());
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < dictionarySize; i++) {
            values.add(convertFunction.apply(i));
        }

        // TODO: when min == max (i.e., singleton ranges, the construction of Domains can be done more efficiently
        return getDomain(columnDescriptor, type, values, values, true);
    }

    public static long asLong(Object value)
    {
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            return ((Number) value).longValue();
        }

        throw new IllegalArgumentException("Can't convert value to long: " + value.getClass().getName());
    }

    private static <T extends Comparable<T>> Domain createDomain(Type type, ColumnIndex columnIndex, boolean hasNullValue, List<T> mins, List<T> maxs)
    {
        if (mins.isEmpty() || maxs.isEmpty() || mins.size() != maxs.size()) {
            return Domain.create(ValueSet.all(type), hasNullValue);
        }
        int pageCount = columnIndex.getMinValues().size();
        List<Range> ranges = new ArrayList<>();
        for (int i = 0; i < pageCount; i++) {
            T min = mins.get(i);
            T max = maxs.get(i);
            if (min.compareTo(max) > 0) {
                return Domain.create(ValueSet.all(type), hasNullValue);
            }

            if (min instanceof Long) {
                if (isStatisticsOverflow(type, asLong(min), asLong(max))) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }
                ranges.add(Range.range(type, min, true, max, true));
            }
            else if (min instanceof Double) {
                if (((Double) min).isNaN() || ((Double) max).isNaN()) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }
                ranges.add(Range.range(type, min, true, max, true));
            }
            else if (min instanceof Slice) {
                ranges.add(Range.range(type, min, true, max, true));
            }
        }
        checkArgument(!ranges.isEmpty(), "cannot use empty ranges");
        return Domain.create(ValueSet.ofRanges(ranges), hasNullValue);
    }

    @Override
    public boolean matches(long numberOfRows, Map<ColumnDescriptor, Statistics<?>> statistics, ParquetDataSourceId id, Optional<WarningCollector> warningCollector)
    {
        if (numberOfRows == 0) {
            return false;
        }

        if (effectivePredicate.isNone()) {
            return false;
        }

        Map<ColumnDescriptor, Domain> effectivePredicateDomains = effectivePredicate.getDomains()
                .orElseThrow(() -> new IllegalStateException("Effective predicate other than none should have domains"));

        for (RichColumnDescriptor column : columns) {
            Domain effectivePredicateDomain = effectivePredicateDomains.get(column);
            if (effectivePredicateDomain == null) {
                continue;
            }

            Statistics<?> columnStatistics = statistics.get(column);
            if (columnStatistics != null && !columnStatistics.isEmpty()) {
                Domain domain = getDomain(
                        column,
                        effectivePredicateDomain.getType(),
                        numberOfRows,
                        columnStatistics,
                        id,
                        warningCollector);
                if (effectivePredicateDomain.intersect(domain).isNone()) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean matches(DictionaryDescriptor dictionary)
    {
        requireNonNull(dictionary, "dictionary is null");
        if (effectivePredicate.isNone()) {
            return false;
        }

        Map<ColumnDescriptor, Domain> effectivePredicateDomains = effectivePredicate.getDomains()
                .orElseThrow(() -> new IllegalStateException("Effective predicate other than none should have domains"));

        Domain effectivePredicateDomain = effectivePredicateDomains.get(dictionary.getColumnDescriptor());

        return effectivePredicateDomain == null || effectivePredicateMatches(effectivePredicateDomain, dictionary);
    }

    @Override
    public boolean matches(long numberOfRows, Optional<ColumnIndexStore> columnIndexStore)
    {
        if (numberOfRows == 0 || !columnIndexStore.isPresent()) {
            return false;
        }

        if (effectivePredicate.isNone()) {
            return false;
        }
        Map<ColumnDescriptor, Domain> effectivePredicateDomains = effectivePredicate.getDomains()
                .orElseThrow(() -> new IllegalStateException("Effective predicate other than none should have domains"));

        for (RichColumnDescriptor column : columns) {
            Domain effectivePredicateDomain = effectivePredicateDomains.get(column);
            if (effectivePredicateDomain == null) {
                continue;
            }

            if (columnIndexStore.isPresent()) {
                ColumnIndex columnIndex = columnIndexStore.get().getColumnIndex(ColumnPath.get(column.getPath()));
                if (columnIndex == null || columnIndex.getMinValues().isEmpty() || columnIndex.getMaxValues().isEmpty() || columnIndex.getMinValues().size() != columnIndex.getMaxValues().size()) {
                    continue;
                }
                else {
                    Domain domain = getDomain(effectivePredicateDomain.getType(), numberOfRows, columnIndex, column);
                    if (effectivePredicateDomain.intersect(domain).isNone()) {
                        return false;
                    }
                }
            }
        }

        return true;
    }

    @VisibleForTesting
    public Domain getDomain(Type type, long rowCount, ColumnIndex columnIndex, RichColumnDescriptor descriptor)
    {
        if (columnIndex == null) {
            return Domain.all(type);
        }

        String columnName = descriptor.getPrimitiveType().getName();
        if (isCorruptedColumnIndex(columnIndex)) {
            return Domain.all(type);
        }

        if (isEmptyColumnIndex(columnIndex)) {
            return Domain.all(type);
        }

        long totalNullCount = columnIndex.getNullCounts().stream().reduce(0L, (a, b) -> a + b);
        if (totalNullCount == rowCount) {
            return Domain.onlyNull(type);
        }

        boolean hasNullValue = totalNullCount > 0;

        if (descriptor.getType().equals(PrimitiveTypeName.BOOLEAN)) {
            // After row-group filtering for boolean, page filtering shouldn't do more
            return Domain.all(type);
        }

        if (descriptor.getType().equals(INT32) || descriptor.getType().equals(INT64) || descriptor.getType().equals(FLOAT)) {
            List<Long> mins = converter.getMinValuesAsLong(type, columnIndex, columnName);
            List<Long> maxs = converter.getMaxValuesAsLong(type, columnIndex, columnName);
            return createDomain(type, columnIndex, hasNullValue, mins, maxs);
        }

        if (descriptor.getType().equals(PrimitiveTypeName.DOUBLE)) {
            List<Double> mins = converter.getMinValuesAsDouble(type, columnIndex, columnName);
            List<Double> maxs = converter.getMaxValuesAsDouble(type, columnIndex, columnName);
            return createDomain(type, columnIndex, hasNullValue, mins, maxs);
        }

        if (descriptor.getType().equals(BINARY)) {
            List<Slice> mins = converter.getMinValuesAsSlice(type, columnIndex);
            List<Slice> maxs = converter.getMaxValuesAsSlice(type, columnIndex);
            return createDomain(type, columnIndex, hasNullValue, mins, maxs);
        }

        //TODO: Add INT96 and FIXED_LEN_BYTE_ARRAY later

        return Domain.create(ValueSet.all(type), hasNullValue);
    }

    private boolean isCorruptedColumnIndex(ColumnIndex columnIndex)
    {
        if (columnIndex.getMaxValues() == null || columnIndex.getMinValues() == null ||
                columnIndex.getNullCounts() == null || columnIndex.getNullPages() == null) {
            return true;
        }

        if (columnIndex.getMaxValues().size() != columnIndex.getMinValues().size() ||
                columnIndex.getMaxValues().size() != columnIndex.getNullPages().size() ||
                columnIndex.getMaxValues().size() != columnIndex.getNullCounts().size()) {
            return true;
        }

        return false;
    }

    // Caller should verify isCorruptedColumnIndex is false first
    private boolean isEmptyColumnIndex(ColumnIndex columnIndex)
    {
        return columnIndex.getMaxValues().isEmpty();
    }

    public FilterPredicate getParquetUserDefinedPredicate()
    {
        FilterPredicate filter = null;

        //  It could be a Presto bug that we don't see effectivePredicate.getDomains().get() has more than 1 domain.
        //  For example, the 'where c1=3 or c1=10002' clause should have two domains but it has none
        //  we assume the relation cross domains are 'or'
        for (RichColumnDescriptor column : columns) {
            Domain domain = effectivePredicate.getDomains().get().get(column);
            if (domain == null || domain.isNone()) {
                continue;
            }

            if (domain.isAll()) {
                continue;
            }

            FilterPredicate columnFilter = FilterApi.userDefined(FilterApi.intColumn(ColumnPath.get(column.getPath()).toDotString()), new ParquetUserDefinedPredicateTupleDomain(domain));
            if (filter == null) {
                filter = columnFilter;
            }
            else {
                filter = FilterApi.or(filter, columnFilter);
            }
        }

        return filter;
    }

    private static class DictionaryValueConverter
    {
        private final Dictionary dictionary;

        private DictionaryValueConverter(Dictionary dictionary)
        {
            this.dictionary = dictionary;
        }

        private Function<Integer, Object> getConverter(PrimitiveType primitiveType)
        {
            switch (primitiveType.getPrimitiveTypeName()) {
                case INT32:
                    return (i) -> dictionary.decodeToInt(i);
                case INT64:
                    return (i) -> dictionary.decodeToLong(i);
                case FLOAT:
                    return (i) -> dictionary.decodeToFloat(i);
                case DOUBLE:
                    return (i) -> dictionary.decodeToDouble(i);
                case FIXED_LEN_BYTE_ARRAY:
                case BINARY:
                case INT96:
                    return (i) -> dictionary.decodeToBinary(i);
                default:
                    throw new IllegalArgumentException("Unsupported Parquet primitive type: " + primitiveType.getPrimitiveTypeName());
            }
        }
    }

    /**
     * This class implements methods defined in UserDefinedPredicate based on the page statistic and tuple domain(for a column).
     */
    static class ParquetUserDefinedPredicateTupleDomain<T extends Comparable<T>>
            extends UserDefinedPredicate<T>
            implements Serializable
    {
        private Domain columnDomain;

        ParquetUserDefinedPredicateTupleDomain(Domain domain)
        {
            this.columnDomain = domain;
        }

        @Override
        public boolean keep(T value)
        {
            if (value == null && !columnDomain.isNullAllowed()) {
                return false;
            }

            return true;
        }

        @Override
        public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<T> statistic)
        {
            if (statistic == null) {
                return false;
            }
            List<Range> ranges = new ArrayList<>();
            ranges.add(Range.range(columnDomain.getType(), statistic.getMin(), true, statistic.getMax(), true));
            return canDropWithRangeStatistics(ranges);
        }

        @Override
        public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<T> statistics)
        {
            // !canDrop() cannot be used because it might not be correct. To be safe, we just keep the record by returning false.
            // Since we don't use LogicalNotUserDefined, this method is not called.
            return false;
        }

        private boolean canDropWithRangeStatistics(List<Range> ranges)
        {
            checkArgument(!ranges.isEmpty(), "cannot use empty ranges");
            Domain domain = Domain.create(ValueSet.ofRanges(ranges), true);
            return columnDomain.intersect(domain).isNone();
        }
    }

    class ColumnIndexValueConverter
    {
        private final Map<String, Function<Object, Object>> conversions;

        private ColumnIndexValueConverter(List<RichColumnDescriptor> columns)
        {
            this.conversions = new HashMap<>();
            for (RichColumnDescriptor column : columns) {
                conversions.put(column.getPrimitiveType().getName(), getColumnIndexConversions(column.getPrimitiveType()));
            }
        }

        public List<Long> getMinValuesAsLong(Type type, ColumnIndex columnIndex, String column)
        {
            return getValuesAsLong(type, column, columnIndex.getMinValues().size(), columnIndex.getMinValues());
        }

        public List<Long> getMaxValuesAsLong(Type type, ColumnIndex columnIndex, String column)
        {
            return getValuesAsLong(type, column, columnIndex.getMaxValues().size(), columnIndex.getMaxValues());
        }

        public List<Double> getMinValuesAsDouble(Type type, ColumnIndex columnIndex, String column)
        {
            return getValuesAsDouble(type, column, columnIndex.getMinValues().size(), columnIndex.getMinValues());
        }

        public List<Double> getMaxValuesAsDouble(Type type, ColumnIndex columnIndex, String column)
        {
            return getValuesAsDouble(type, column, columnIndex.getMaxValues().size(), columnIndex.getMaxValues());
        }

        public List<Slice> getMinValuesAsSlice(Type type, ColumnIndex columnIndex)
        {
            return getValuesAsSlice(type, columnIndex.getMinValues().size(), columnIndex.getMinValues());
        }

        public List<Slice> getMaxValuesAsSlice(Type type, ColumnIndex columnIndex)
        {
            return getValuesAsSlice(type, columnIndex.getMaxValues().size(), columnIndex.getMaxValues());
        }

        private List<Long> getValuesAsLong(Type type, String column, int pageCount, List<ByteBuffer> values)
        {
            List<Long> result = new ArrayList<>();
            if (TINYINT.equals(type) || SMALLINT.equals(type) || INTEGER.equals(type)) {
                for (int i = 0; i < pageCount; i++) {
                    result.add((long) converter.convert(values.get(i), column));
                }
            }
            else if (BIGINT.equals(type)) {
                for (int i = 0; i < pageCount; i++) {
                    result.add((long) converter.convert(values.get(i), column));
                }
            }
            else if (REAL.equals(type)) {
                for (int i = 0; i < pageCount; i++) {
                    result.add((long) floatToRawIntBits(converter.convert(values.get(i), column)));
                }
            }
            return result;
        }

        private List<Double> getValuesAsDouble(Type type, String column, int pageCount, List<ByteBuffer> values)
        {
            List<Double> result = new ArrayList<>();
            if (DOUBLE.equals(type)) {
                for (int i = 0; i < pageCount; i++) {
                    result.add(converter.convert(values.get(i), column));
                }
            }
            return result;
        }

        private List<Slice> getValuesAsSlice(Type type, int pageCount, List<ByteBuffer> values)
        {
            List<Slice> result = new ArrayList<>();
            if (isVarcharType(type)) {
                for (int i = 0; i < pageCount; i++) {
                    result.add(Slices.wrappedBuffer(values.get(i)));
                }
            }
            return result;
        }

        private <T> T convert(ByteBuffer buffer, String name)
        {
            return (T) conversions.get(name).apply(buffer);
        }

        private Function<Object, Object> getColumnIndexConversions(PrimitiveType type)
        {
            switch (type.getPrimitiveTypeName()) {
                case BOOLEAN:
                    return buffer -> ((ByteBuffer) buffer).get(0) != 0;
                case INT32:
                    return buffer -> ((ByteBuffer) buffer).order(LITTLE_ENDIAN).getInt(0);
                case INT64:
                    return buffer -> ((ByteBuffer) buffer).order(LITTLE_ENDIAN).getLong(0);
                case FLOAT:
                    return buffer -> ((ByteBuffer) buffer).order(LITTLE_ENDIAN).getFloat(0);
                case DOUBLE:
                    return buffer -> ((ByteBuffer) buffer).order(LITTLE_ENDIAN).getDouble(0);
                case BINARY:
                case FIXED_LEN_BYTE_ARRAY:
                case INT96:
                    return binary -> ByteBuffer.wrap(((Binary) binary).getBytes());
                default:
                    throw new IllegalArgumentException("Unsupported Parquet type: " + type.getPrimitiveTypeName());
            }
        }
    }
}
