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
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

import java.util.ArrayList;
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
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TupleDomainParquetPredicate
        implements Predicate
{
    private final TupleDomain<ColumnDescriptor> effectivePredicate;
    private final List<RichColumnDescriptor> columns;

    public TupleDomainParquetPredicate(TupleDomain<ColumnDescriptor> effectivePredicate, List<RichColumnDescriptor> columns)
    {
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
    }

    @Override
    public boolean matches(long numberOfRows, Map<ColumnDescriptor, Statistics<?>> statistics, ParquetDataSourceId id)
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
                Domain domain = getDomain(
                                    column,
                                    effectivePredicateDomain.getType(),
                                    numberOfRows,
                                    columnStatistics,
                                    id);
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
            ParquetDataSourceId id)
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

        try {
            return getDomain(
                    column,
                    type,
                    ImmutableList.of(statistics.genericGetMin()),
                    ImmutableList.of(statistics.genericGetMax()),
                    hasNullValue);
        }
        catch (Exception exception) {
            throw new ParquetCorruptionException(exception, format("Corrupted statistics for column \"%s\" in Parquet file \"%s\": [%s]", column.toString(), id, statistics));
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
}
