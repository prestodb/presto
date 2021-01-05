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
import com.facebook.presto.parquet.ParquetCorruptionException;
import com.facebook.presto.parquet.ParquetDataSourceId;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.dictionary.Dictionary;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
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
import static com.facebook.presto.parquet.predicate.PredicateUtils.isStatisticsOverflow;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.format;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.requireNonNull;

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

    @Override
    public boolean matches(long numberOfRows, Map<ColumnDescriptor, Statistics<?>> statistics, ParquetDataSourceId id, boolean failOnCorruptedParquetStatistics)
            throws ParquetCorruptionException
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
            if (columnStatistics == null || columnStatistics.isEmpty()) {
                // no stats for column
            }
            else {
                Domain domain = getDomain(effectivePredicateDomain.getType(), numberOfRows, columnStatistics, id, column.toString(), failOnCorruptedParquetStatistics);
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
    public boolean matches(long numberOfRows, ColumnIndexStore ciStore, ParquetDataSourceId id, boolean failOnCorruptedParquetStatistics)
            throws ParquetCorruptionException
    {
        if (numberOfRows == 0 || ciStore == null) {
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

            ColumnIndex columnIndex = ciStore.getColumnIndex(ColumnPath.get(column.getPath()));
            if (columnIndex == null || columnIndex.getMinValues().size() == 0 || columnIndex.getMaxValues().size() == 0 || columnIndex.getMinValues().size() != columnIndex.getMaxValues().size()) {
                continue;
            }
            else {
                Domain domain = getDomain(effectivePredicateDomain.getType(), numberOfRows, columnIndex, id, column, failOnCorruptedParquetStatistics);
                if (effectivePredicateDomain.intersect(domain).isNone()) {
                    return false;
                }
            }
        }

        return true;
    }

    private static boolean effectivePredicateMatches(Domain effectivePredicateDomain, DictionaryDescriptor dictionary)
    {
        return !effectivePredicateDomain.intersect(getDomain(effectivePredicateDomain.getType(), dictionary)).isNone();
    }

    @VisibleForTesting
    public static Domain getDomain(Type type, long rowCount, Statistics<?> statistics, ParquetDataSourceId id, String column, boolean failOnCorruptedParquetStatistics)
            throws ParquetCorruptionException
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

        if (type.equals(BOOLEAN) && statistics instanceof BooleanStatistics) {
            BooleanStatistics booleanStatistics = (BooleanStatistics) statistics;

            boolean hasTrueValues = !(booleanStatistics.getMax() == false && booleanStatistics.getMin() == false);
            boolean hasFalseValues = !(booleanStatistics.getMax() == true && booleanStatistics.getMin() == true);
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

        if ((type.equals(BIGINT) || type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER)) && (statistics instanceof LongStatistics || statistics instanceof IntStatistics)) {
            ParquetIntegerStatistics parquetIntegerStatistics;
            if (statistics instanceof LongStatistics) {
                LongStatistics longStatistics = (LongStatistics) statistics;
                if (longStatistics.genericGetMin() > longStatistics.genericGetMax()) {
                    failWithCorruptionException(failOnCorruptedParquetStatistics, column, id, longStatistics);
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }
                parquetIntegerStatistics = new ParquetIntegerStatistics(longStatistics.genericGetMin(), longStatistics.genericGetMax());
            }
            else {
                IntStatistics intStatistics = (IntStatistics) statistics;
                if (intStatistics.genericGetMin() > intStatistics.genericGetMax()) {
                    failWithCorruptionException(failOnCorruptedParquetStatistics, column, id, intStatistics);
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }
                parquetIntegerStatistics = new ParquetIntegerStatistics((long) intStatistics.getMin(), (long) intStatistics.getMax());
            }
            if (isStatisticsOverflow(type, parquetIntegerStatistics)) {
                return Domain.create(ValueSet.all(type), hasNullValue);
            }
            return createDomain(type, hasNullValue, parquetIntegerStatistics);
        }

        if (type.equals(REAL) && statistics instanceof FloatStatistics) {
            FloatStatistics floatStatistics = (FloatStatistics) statistics;
            if (floatStatistics.genericGetMin() > floatStatistics.genericGetMax()) {
                failWithCorruptionException(failOnCorruptedParquetStatistics, column, id, floatStatistics);
                return Domain.create(ValueSet.all(type), hasNullValue);
            }

            if (floatStatistics.genericGetMin().isNaN() || floatStatistics.genericGetMax().isNaN()) {
                return Domain.create(ValueSet.all(type), hasNullValue);
            }

            ParquetIntegerStatistics parquetStatistics = new ParquetIntegerStatistics(
                    (long) floatToRawIntBits(floatStatistics.getMin()),
                    (long) floatToRawIntBits(floatStatistics.getMax()));

            return createDomain(type, hasNullValue, parquetStatistics);
        }

        if (type.equals(DOUBLE) && statistics instanceof DoubleStatistics) {
            DoubleStatistics doubleStatistics = (DoubleStatistics) statistics;
            if (doubleStatistics.genericGetMin() > doubleStatistics.genericGetMax()) {
                failWithCorruptionException(failOnCorruptedParquetStatistics, column, id, doubleStatistics);
                return Domain.create(ValueSet.all(type), hasNullValue);
            }

            if (doubleStatistics.genericGetMin().isNaN() || doubleStatistics.genericGetMax().isNaN()) {
                return Domain.create(ValueSet.all(type), hasNullValue);
            }

            ParquetDoubleStatistics parquetDoubleStatistics = new ParquetDoubleStatistics(doubleStatistics.genericGetMin(), doubleStatistics.genericGetMax());
            return createDomain(type, hasNullValue, parquetDoubleStatistics);
        }

        if (isVarcharType(type) && statistics instanceof BinaryStatistics) {
            BinaryStatistics binaryStatistics = (BinaryStatistics) statistics;
            Slice minSlice = Slices.wrappedBuffer(binaryStatistics.genericGetMin().getBytes());
            Slice maxSlice = Slices.wrappedBuffer(binaryStatistics.genericGetMax().getBytes());
            if (minSlice.compareTo(maxSlice) > 0) {
                failWithCorruptionException(failOnCorruptedParquetStatistics, column, id, binaryStatistics);
                return Domain.create(ValueSet.all(type), hasNullValue);
            }
            ParquetStringStatistics parquetStringStatistics = new ParquetStringStatistics(minSlice, maxSlice);
            return createDomain(type, hasNullValue, parquetStringStatistics);
        }

        if (type.equals(DATE) && statistics instanceof IntStatistics) {
            IntStatistics intStatistics = (IntStatistics) statistics;
            if (intStatistics.genericGetMin() > intStatistics.genericGetMax()) {
                failWithCorruptionException(failOnCorruptedParquetStatistics, column, id, intStatistics);
                return Domain.create(ValueSet.all(type), hasNullValue);
            }
            ParquetIntegerStatistics parquetIntegerStatistics = new ParquetIntegerStatistics((long) intStatistics.getMin(), (long) intStatistics.getMax());
            return createDomain(type, hasNullValue, parquetIntegerStatistics);
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
            // TODO take failOnCorruptedParquetStatistics parameter and handle appropriately
            return Domain.all(type);
        }

        int dictionarySize = dictionaryPage.get().getDictionarySize();
        if (type.equals(BIGINT) && columnDescriptor.getType() == PrimitiveTypeName.INT64) {
            List<Long> values = new ArrayList<>(dictionarySize);
            for (int i = 0; i < dictionarySize; i++) {
                values.add(dictionary.decodeToLong(i));
            }
            return Domain.create(ValueSet.copyOf(type, values), true);
        }

        if ((type.equals(BIGINT) || type.equals(DATE)) && columnDescriptor.getType() == PrimitiveTypeName.INT32) {
            List<Long> values = new ArrayList<>(dictionarySize);
            for (int i = 0; i < dictionarySize; i++) {
                values.add((long) dictionary.decodeToInt(i));
            }
            return Domain.create(ValueSet.copyOf(type, values), true);
        }

        if (type.equals(DOUBLE) && columnDescriptor.getType() == PrimitiveTypeName.DOUBLE) {
            List<Double> values = new ArrayList<>(dictionarySize);
            for (int i = 0; i < dictionarySize; i++) {
                double value = dictionary.decodeToDouble(i);
                if (Double.isNaN(value)) {
                    return Domain.all(type);
                }
                values.add(value);
            }
            return Domain.create(ValueSet.copyOf(type, values), true);
        }

        if (type.equals(DOUBLE) && columnDescriptor.getType() == PrimitiveTypeName.FLOAT) {
            List<Double> values = new ArrayList<>(dictionarySize);
            for (int i = 0; i < dictionarySize; i++) {
                float value = dictionary.decodeToFloat(i);
                if (Float.isNaN(value)) {
                    return Domain.all(type);
                }
                values.add((double) value);
            }
            return Domain.create(ValueSet.copyOf(type, values), true);
        }

        if (isVarcharType(type) && columnDescriptor.getType() == PrimitiveTypeName.BINARY) {
            List<Slice> values = new ArrayList<>(dictionarySize);
            for (int i = 0; i < dictionarySize; i++) {
                values.add(Slices.wrappedBuffer(dictionary.decodeToBinary(i).getBytes()));
            }
            return Domain.create(ValueSet.copyOf(type, values), true);
        }

        return Domain.all(type);
    }

    @VisibleForTesting
    public Domain getDomain(Type type, long rowCount, ColumnIndex columnIndex, ParquetDataSourceId id, RichColumnDescriptor descriptor, boolean failOnCorruptedParquetStatistics)
            throws ParquetCorruptionException
    {
        if (columnIndex == null) {
            return Domain.all(type);
        }

        String columnName = descriptor.getPrimitiveType().getName();

        if (isCorruptedColumnIndex(columnIndex)) {
            if (failOnCorruptedParquetStatistics) {
                failWithCorruptionException(failOnCorruptedParquetStatistics, columnName, id, columnIndex);
            }
            else {
                return Domain.all(type);
            }
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

        if (descriptor.getType().equals(PrimitiveTypeName.INT32) || descriptor.getType().equals(PrimitiveTypeName.INT64) || descriptor.getType().equals(PrimitiveTypeName.FLOAT)) {
            List<Long> mins = converter.getMinValuesAsLong(type, columnIndex, columnName);
            List<Long> maxs = converter.getMaxValuesAsLong(type, columnIndex, columnName);
            return createDomain(type, columnIndex, id, failOnCorruptedParquetStatistics, hasNullValue, columnName, mins, maxs);
        }

        if (descriptor.getType().equals(PrimitiveTypeName.DOUBLE)) {
            List<Double> mins = converter.getMinValuesAsDouble(type, columnIndex, columnName);
            List<Double> maxs = converter.getMaxValuesAsDouble(type, columnIndex, columnName);
            return createDomain(type, columnIndex, id, failOnCorruptedParquetStatistics, hasNullValue, columnName, mins, maxs);
        }

        if (descriptor.getType().equals(PrimitiveTypeName.BINARY)) {
            List<Slice> mins = converter.getMinValuesAsSlice(type, columnIndex);
            List<Slice> maxs = converter.getMaxValuesAsSlice(type, columnIndex);
            return createDomain(type, columnIndex, id, failOnCorruptedParquetStatistics, hasNullValue, columnName, mins, maxs);
        }

        //TODO: Add INT96 and FIXED_LEN_BYTE_ARRAY later

        return Domain.create(ValueSet.all(type), hasNullValue);
    }

    private static void failWithCorruptionException(boolean failOnCorruptedParquetStatistics, String column, ParquetDataSourceId id, Statistics statistics)
            throws ParquetCorruptionException
    {
        if (failOnCorruptedParquetStatistics) {
            throw new ParquetCorruptionException(format("Corrupted statistics for column \"%s\" in Parquet file \"%s\": [%s]", column, id, statistics));
        }
    }

    private static void failWithCorruptionException(boolean failOnCorruptedParquetStatistics, String column, ParquetDataSourceId id, ColumnIndex columnIndex)
            throws ParquetCorruptionException
    {
        if (failOnCorruptedParquetStatistics) {
            throw new ParquetCorruptionException(format("Corrupted statistics for column \"%s\" in Parquet file \"%s\": [%s]", column, id, columnIndex));
        }
    }

    private static <T extends Comparable<T>> Domain createDomain(Type type, boolean hasNullValue, ParquetRangeStatistics<T> rangeStatistics)
    {
        return createDomain(type, hasNullValue, rangeStatistics, value -> value);
    }

    private static <T extends Comparable<T>> Domain createDomain(Type type, boolean hasNullValue, List<ParquetRangeStatistics<T>> rangeStatistics)
    {
        return createDomain(type, hasNullValue, rangeStatistics, value -> value);
    }

    private static <F, T extends Comparable<T>> Domain createDomain(Type type,
            boolean hasNullValue,
            ParquetRangeStatistics<F> rangeStatistics,
            Function<F, T> function)
    {
        F min = rangeStatistics.getMin();
        F max = rangeStatistics.getMax();

        if (min != null && max != null) {
            return Domain.create(ValueSet.ofRanges(Range.range(type, function.apply(min), true, function.apply(max), true)), hasNullValue);
        }
        if (max != null) {
            return Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, function.apply(max))), hasNullValue);
        }
        if (min != null) {
            return Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, function.apply(min))), hasNullValue);
        }
        return Domain.create(ValueSet.all(type), hasNullValue);
    }

    // rangeStatistics is neither null nor empty, checked by the caller
    private static <F, T extends Comparable<T>> Domain createDomain(Type type,
            boolean hasNullValue,
            List<ParquetRangeStatistics<F>> rangeStatistics,
            Function<F, T> function)
    {
        Range firstRange = null;
        Range[] restRanges = new Range[rangeStatistics.size() - 1];
        for (int i = 0; i < rangeStatistics.size(); i++) {
            F min = rangeStatistics.get(i).getMin();
            F max = rangeStatistics.get(i).getMax();
            Range range;
            if (min != null && max != null) {
                range = Range.range(type, function.apply(min), true, function.apply(max), true);
            }
            else if (max != null) {
                range = Range.lessThanOrEqual(type, function.apply(max));
            }
            else if (min != null) {
                range = Range.greaterThanOrEqual(type, function.apply(min));
            }
            else {
                return Domain.create(ValueSet.all(type), hasNullValue);
            }

            if (i == 0) {
                firstRange = range;
            }
            else {
                restRanges[i - 1] = range;
            }
        }

        return Domain.create(ValueSet.ofRanges(firstRange, restRanges), hasNullValue);
    }

    private static <T extends Comparable<T>> Domain createDomain(Type type, ColumnIndex columnIndex, ParquetDataSourceId id, boolean failOnCorruptedParquetStatistics, boolean hasNullValue, String columnName, List<T> mins, List<T> maxs)
            throws ParquetCorruptionException
    {
        if (mins.size() == 0 || maxs.size() == 0 || mins.size() != maxs.size()) {
            return Domain.create(ValueSet.all(type), hasNullValue);
        }
        int pageCount = columnIndex.getMinValues().size();
        List<ParquetRangeStatistics<T>> ranges = new ArrayList<>();
        for (int i = 0; i < pageCount; i++) {
            T min = mins.get(i);
            T max = maxs.get(i);
            if (min.compareTo(max) > 0) {
                failWithCorruptionException(failOnCorruptedParquetStatistics, columnName, id, columnIndex);
                return Domain.create(ValueSet.all(type), hasNullValue);
            }
            //TODO:
            if (min instanceof Long) {
                ParquetIntegerStatistics statistics = new ParquetIntegerStatistics((Long) min, (Long) max);
                ranges.add((ParquetRangeStatistics<T>) statistics);
            }
            else if (min instanceof Double) {
                ParquetDoubleStatistics statistics = new ParquetDoubleStatistics((Double) min, (Double) max);
                ranges.add((ParquetRangeStatistics<T>) statistics);
            }
            else if (min instanceof Slice) {
                ParquetStringStatistics statistics = new ParquetStringStatistics((Slice) min, (Slice) max);
                ranges.add((ParquetRangeStatistics<T>) statistics);
            }
        }
        return createDomain(type, hasNullValue, ranges);
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
        return columnIndex.getMaxValues().size() == 0;
    }

    public FilterPredicate convertToParquetUdp()
    {
        FilterPredicate filter = null;

        // TODO: It could be a Presto bug that we don't see effectivePredicate.getDomains().get() has more than 1 domain.
        //  For example, the 'where c1=3 or c1=10002' clause should have two domains but it has none
        //  Todo: we assume the relation cross domains are 'or'
        for (RichColumnDescriptor column : columns) {
            Domain domain = effectivePredicate.getDomains().get().get(column);
            if (domain == null || domain.isNone()) {
                continue;
            }

            if (domain.isAll()) {
                continue;
            }

            FilterPredicate columnFilter = FilterApi.userDefined(FilterApi.intColumn(ColumnPath.get(column.getPath()).toDotString()), new DomainUserDefinedPredicate(domain));
            if (filter == null) {
                filter = columnFilter;
            }
            else {
                filter = FilterApi.or(filter, columnFilter);
            }
        }

        return filter;
    }

    /**
     * This class implements methods defined in UserDefinedPredicate based on the page statistic and tuple domain(for a column).
     */
    static class DomainUserDefinedPredicate<T extends Comparable<T>>
            extends UserDefinedPredicate<T>
            implements Serializable
    {
        private Domain columnDomain;

        DomainUserDefinedPredicate(Domain domain)
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
            else {
                if (statistic.getMin() instanceof Integer) {
                    Integer min = (Integer) statistic.getMin();
                    Integer max = (Integer) statistic.getMax();
                    return canDropCanWithRangeStats(new ParquetIntegerStatistics((long) min, (long) max));
                }
                else if (statistic.getMin() instanceof Long) {
                    Long min = (Long) statistic.getMin();
                    Long max = (Long) statistic.getMax();
                    return canDropCanWithRangeStats(new ParquetIntegerStatistics(min, max));
                }
                else if (statistic.getMin() instanceof Float) {
                    Integer min = floatToRawIntBits((Float) statistic.getMin());
                    Integer max = floatToRawIntBits((Float) statistic.getMax());
                    return canDropCanWithRangeStats(new ParquetIntegerStatistics((long) min, (long) max));
                }
                else if (statistic.getMin() instanceof Double) {
                    Double min = (Double) statistic.getMin();
                    Double max = (Double) statistic.getMax();
                    return canDropCanWithRangeStats(new ParquetDoubleStatistics(min, max));
                }
                else if (statistic.getMin() instanceof Binary) {
                    Binary min = (Binary) statistic.getMin();
                    Binary max = (Binary) statistic.getMax();
                    return canDropCanWithRangeStats(new ParquetStringStatistics((Slices.wrappedBuffer(min.getBytes())), Slices.wrappedBuffer(max.getBytes())));
                }
                //TODO: Add other types
            }
            return false;
        }

        @Override
        public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<T> statistics)
        {
            // !canDrop() cannot be used because it might not be correct. To be safe, we just keep the record by returning false.
            // Since we don't use LogicalNotUserDefined, this method is not called.
            return false;
        }

        private boolean canDropCanWithRangeStats(ParquetRangeStatistics parquetStatistics)
        {
            //TODO: hasNullValue is set true. Would it generate false negative?
            Domain domain = createDomain(columnDomain.getType(), true, parquetStatistics);
            if (columnDomain.intersect(domain).isNone()) {
                return true;
            }
            return false;
        }
    }

    class ColumnIndexValueConverter
    {
        private final Map<String, Function<Object, Object>> ciConversions;

        private ColumnIndexValueConverter(List<RichColumnDescriptor> columns)
        {
            this.ciConversions = new HashMap<>();
            for (RichColumnDescriptor column : columns) {
                ciConversions.put(column.getPrimitiveType().getName(), getColumnIndexConversions(column.getPrimitiveType()));
            }
        }

        public List<Long> getMinValuesAsLong(Type type, ColumnIndex columnIndex, String column)
        {
            int pageCount = columnIndex.getMinValues().size();
            List<ByteBuffer> minValues = columnIndex.getMinValues();
            List<Long> mins = new ArrayList<>();
            for (int i = 0; i < pageCount; i++) {
                if (TINYINT.equals(type) || SMALLINT.equals(type) || INTEGER.equals(type)) {
                    int minValue = converter.convert(minValues.get(i), column);
                    mins.add((long) minValue);
                }
                else if (BIGINT.equals(type)) {
                    long minValue = converter.convert(minValues.get(i), column);
                    mins.add(minValue);
                }
                else if (REAL.equals(type)) {
                    float minValue = floatToRawIntBits(converter.convert(minValues.get(i), column));
                    mins.add((long) minValue);
                }
            }
            return mins;
        }

        public List<Long> getMaxValuesAsLong(Type type, ColumnIndex columnIndex, String column)
        {
            int pageCount = columnIndex.getMaxValues().size();
            List<ByteBuffer> maxValues = columnIndex.getMaxValues();
            List<Long> maxs = new ArrayList<>();
            if (TINYINT.equals(type) || SMALLINT.equals(type) || INTEGER.equals(type)) {
                for (int i = 0; i < pageCount; i++) {
                    int maxValue = converter.convert(maxValues.get(i), column);
                    maxs.add((long) maxValue);
                }
            }
            else if (BIGINT.equals(type)) {
                for (int i = 0; i < pageCount; i++) {
                    long maxValue = converter.convert(maxValues.get(i), column);
                    maxs.add((long) maxValue);
                }
            }
            else if (REAL.equals(type)) {
                for (int i = 0; i < pageCount; i++) {
                    float maxValue = floatToRawIntBits(converter.convert(maxValues.get(i), column));
                    maxs.add((long) maxValue);
                }
            }
            //TODO: Else
            return maxs;
        }

        public List<Double> getMinValuesAsDouble(Type type, ColumnIndex columnIndex, String column)
        {
            int pageCount = columnIndex.getMinValues().size();
            List<ByteBuffer> minValues = columnIndex.getMinValues();
            List<Double> mins = new ArrayList<>();
            if (DOUBLE.equals(type)) {
                for (int i = 0; i < pageCount; i++) {
                    double minValue = converter.convert(minValues.get(i), column);
                    mins.add(minValue);
                }
            }
            //TODO: Else
            return mins;
        }

        public List<Double> getMaxValuesAsDouble(Type type, ColumnIndex columnIndex, String column)
        {
            int pageCount = columnIndex.getMaxValues().size();
            List<ByteBuffer> maxValues = columnIndex.getMaxValues();
            List<Double> maxs = new ArrayList<>();
            if (DOUBLE.equals(type)) {
                for (int i = 0; i < pageCount; i++) {
                    double maxValue = converter.convert(maxValues.get(i), column);
                    maxs.add(maxValue);
                }
            }
            return maxs;
        }

        public List<Slice> getMinValuesAsSlice(Type type, ColumnIndex columnIndex)
        {
            int pageCount = columnIndex.getMinValues().size();
            List<ByteBuffer> minValues = columnIndex.getMinValues();
            List<Slice> mins = new ArrayList<>();
            if (isVarcharType(type)) {
                for (int i = 0; i < pageCount; i++) {
                    Slice minValue = Slices.wrappedBuffer(minValues.get(i));
                    mins.add(minValue);
                }
            }
            //TODO: Else
            return mins;
        }

        public List<Slice> getMaxValuesAsSlice(Type type, ColumnIndex columnIndex)
        {
            int pageCount = columnIndex.getMaxValues().size();
            List<ByteBuffer> maxValues = columnIndex.getMaxValues();
            List<Slice> maxs = new ArrayList<>();
            if (isVarcharType(type)) {
                for (int i = 0; i < pageCount; i++) {
                    Slice maxValue = Slices.wrappedBuffer(maxValues.get(i));
                    maxs.add(maxValue);
                }
            }
            //TODO: Else
            return maxs;
        }

        private <T> T convert(ByteBuffer buf, String name)
        {
            return (T) ciConversions.get(name).apply(buf);
        }

        private Function<Object, Object> getColumnIndexConversions(PrimitiveType type)
        {
            //TODO: getBoundaryOrder() is not used, should replace LITTLE_ENDIAN with getBoundaryOrder
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
                    // TODO: return buffer -> Binary.fromReusedByteBuffer((ByteBuffer) buffer);
                    return binary -> ByteBuffer.wrap(((Binary) binary).getBytes());
                default:
            }

            return obj -> obj;
        }
    }
}
