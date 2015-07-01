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
package com.facebook.presto.hive.parquet.predicate;

import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import parquet.column.ColumnDescriptor;
import parquet.column.Dictionary;
import parquet.column.page.DictionaryPage;
import parquet.column.statistics.BinaryStatistics;
import parquet.column.statistics.BooleanStatistics;
import parquet.column.statistics.DoubleStatistics;
import parquet.column.statistics.FloatStatistics;
import parquet.column.statistics.IntStatistics;
import parquet.column.statistics.LongStatistics;
import parquet.column.statistics.Statistics;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TupleDomainParquetPredicate<C>
        implements ParquetPredicate
{
    private final TupleDomain<C> effectivePredicate;
    private final List<ColumnReference<C>> columnReferences;

    public TupleDomainParquetPredicate(TupleDomain<C> effectivePredicate, List<ColumnReference<C>> columnReferences)
    {
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
        this.columnReferences = ImmutableList.copyOf(requireNonNull(columnReferences, "columnReferences is null"));
    }

    @Override
    public boolean matches(long numberOfRows, Map<Integer, Statistics<?>> statisticsByColumnIndex)
    {
        if (numberOfRows == 0) {
            return false;

        }
        ImmutableMap.Builder<C, Domain> domains = ImmutableMap.builder();

        for (ColumnReference<C> columnReference : columnReferences) {
            Statistics<?> statistics = statisticsByColumnIndex.get(columnReference.getOrdinal());
            Domain domain = getDomain(columnReference.getType(), numberOfRows, statistics);
            if (domain != null) {
                domains.put(columnReference.getColumn(), domain);
            }
        }
        TupleDomain<C> stripeDomain = TupleDomain.withColumnDomains(domains.build());

        return effectivePredicate.overlaps(stripeDomain);
    }

    @Override
    public boolean matches(Map<Integer, ParquetDictionaryDescriptor> dictionariesByColumnIndex)
    {
        ImmutableMap.Builder<C, Domain> domains = ImmutableMap.builder();

        for (ColumnReference<C> columnReference : columnReferences) {
            ParquetDictionaryDescriptor dictionaryDescriptor = dictionariesByColumnIndex.get(columnReference.getOrdinal());
            Domain domain = getDomain(columnReference.getType(), dictionaryDescriptor);
            if (domain != null) {
                domains.put(columnReference.getColumn(), domain);
            }
        }
        TupleDomain<C> stripeDomain = TupleDomain.withColumnDomains(domains.build());

        return effectivePredicate.overlaps(stripeDomain);
    }

    @VisibleForTesting
    public static Domain getDomain(Type type, long rowCount, Statistics<?> statistics)
    {
        Class<?> boxedJavaType = Primitives.wrap(type.getJavaType());
        if (statistics == null || statistics.isEmpty() || !(Comparable.class.isAssignableFrom(type.getJavaType()))) {
            return null;
        }

        if (statistics.getNumNulls() == rowCount) {
            return Domain.onlyNull(boxedJavaType);
        }

        boolean hasNullValue = statistics.getNumNulls() != 0L;

        if (boxedJavaType == Boolean.class && statistics instanceof BooleanStatistics) {
            BooleanStatistics booleanStatistics = (BooleanStatistics) statistics;

            boolean hasTrueValues = !(booleanStatistics.getMax() == false && booleanStatistics.getMin() == false);
            boolean hasFalseValues = !(booleanStatistics.getMax() == true && booleanStatistics.getMin() == true);
            if (hasTrueValues && hasFalseValues) {
                return Domain.all(Boolean.class);
            }
            if (hasTrueValues) {
                return Domain.create(SortedRangeSet.singleValue(true), hasNullValue);
            }
            if (hasFalseValues) {
                return Domain.create(SortedRangeSet.singleValue(false), hasNullValue);
            }
        }
        else if (boxedJavaType == Long.class && (statistics instanceof LongStatistics || statistics instanceof IntStatistics)) {
            ParquetIntegerStatistics parquetIntegerStatistics;
            if (statistics instanceof LongStatistics) {
                LongStatistics longStatistics = (LongStatistics) statistics;
                parquetIntegerStatistics = new ParquetIntegerStatistics(longStatistics.genericGetMin(), longStatistics.genericGetMax());
            }
            else {
                IntStatistics intStatistics = (IntStatistics) statistics;
                parquetIntegerStatistics = new ParquetIntegerStatistics((long) intStatistics.getMin(), (long) intStatistics.getMax());
            }
            return createDomain(boxedJavaType, hasNullValue, parquetIntegerStatistics);
        }
        else if (boxedJavaType == Double.class && (statistics instanceof DoubleStatistics || statistics instanceof FloatStatistics)) {
            ParquetDoubleStatistics parquetDoubleStatistics;
            if (statistics instanceof DoubleStatistics) {
                DoubleStatistics doubleStatistics = (DoubleStatistics) statistics;
                parquetDoubleStatistics = new ParquetDoubleStatistics(doubleStatistics.genericGetMin(), doubleStatistics.genericGetMax());
            }
            else {
                FloatStatistics floatStatistics = (FloatStatistics) statistics;
                parquetDoubleStatistics = new ParquetDoubleStatistics((double) floatStatistics.getMin(), (double) floatStatistics.getMax());
            }
            return createDomain(boxedJavaType, hasNullValue, parquetDoubleStatistics);
        }
        else if (boxedJavaType == Slice.class && statistics instanceof BinaryStatistics) {
            BinaryStatistics binaryStatistics = (BinaryStatistics) statistics;
            ParquetStringStatistics parquetStringStatistics = new ParquetStringStatistics(
                    Slices.wrappedBuffer(binaryStatistics.getMin().getBytes()),
                    Slices.wrappedBuffer(binaryStatistics.getMax().getBytes()));
            return createDomain(boxedJavaType, hasNullValue, parquetStringStatistics);
        }
        return Domain.create(SortedRangeSet.all(boxedJavaType), hasNullValue);
    }

    @VisibleForTesting
    public static Domain getDomain(Type type, ParquetDictionaryDescriptor dictionaryDescriptor)
    {
        Class<?> boxedJavaType = Primitives.wrap(type.getJavaType());
        if (dictionaryDescriptor == null) {
            return null;
        }

        ColumnDescriptor columnDescriptor = dictionaryDescriptor.getColumnDescriptor();
        DictionaryPage dictionaryPage = dictionaryDescriptor.getDictionaryPage();
        if (dictionaryPage == null) {
            return null;
        }

        Dictionary dictionary;
        try {
            dictionary = dictionaryPage.getEncoding().initDictionary(columnDescriptor, dictionaryPage);
        }
        catch (IOException e) {
            return null;
        }

        int dictionarySize = dictionaryPage.getDictionarySize();
        if (boxedJavaType == Long.class && columnDescriptor.getType() == PrimitiveTypeName.INT64) {
            List<Domain> domains = new ArrayList<>();
            for (int i = 0; i < dictionarySize; i++) {
                domains.add(Domain.singleValue(dictionary.decodeToLong(i)));
            }
            return Domain.create(Domain.union(domains).getRanges(), true);
        }
        else if (boxedJavaType == Long.class && columnDescriptor.getType() == PrimitiveTypeName.INT32) {
            List<Domain> domains = new ArrayList<>();
            for (int i = 0; i < dictionarySize; i++) {
                domains.add(Domain.singleValue((long) dictionary.decodeToInt(i)));
            }
            return Domain.create(Domain.union(domains).getRanges(), true);
        }
        else if (boxedJavaType == Double.class && columnDescriptor.getType() == PrimitiveTypeName.DOUBLE) {
            List<Domain> domains = new ArrayList<>();
            for (int i = 0; i < dictionarySize; i++) {
                domains.add(Domain.singleValue(dictionary.decodeToDouble(i)));
            }
            return Domain.create(Domain.union(domains).getRanges(), true);
        }
        else if (boxedJavaType == Double.class && columnDescriptor.getType() == PrimitiveTypeName.FLOAT) {
            List<Domain> domains = new ArrayList<>();
            for (int i = 0; i < dictionarySize; i++) {
                domains.add(Domain.singleValue((double) dictionary.decodeToFloat(i)));
            }
            return Domain.create(Domain.union(domains).getRanges(), true);
        }
        else if (boxedJavaType == Slice.class && columnDescriptor.getType() == PrimitiveTypeName.BINARY) {
            List<Domain> domains = new ArrayList<>();
            for (int i = 0; i < dictionarySize; i++) {
                domains.add(Domain.singleValue(Slices.wrappedBuffer(dictionary.decodeToBinary(i).getBytes())));
            }
            return Domain.create(Domain.union(domains).getRanges(), true);
        }
        return null;
    }

    private static <T extends Comparable<T>> Domain createDomain(Class<?> boxedJavaType, boolean hasNullValue, ParquetRangeStatistics<T> rangeStatistics)
    {
        return createDomain(boxedJavaType, hasNullValue, rangeStatistics, value -> value);
    }

    private static <F, T extends Comparable<T>> Domain createDomain(Class<?> boxedJavaType,
            boolean hasNullValue,
            ParquetRangeStatistics<F> rangeStatistics,
            Function<F, T> function)
    {
        F min = rangeStatistics.getMin();
        F max = rangeStatistics.getMax();

        if (min != null && max != null) {
            return Domain.create(SortedRangeSet.of(Range.range(function.apply(min), true, function.apply(max), true)), hasNullValue);
        }
        if (max != null) {
            return Domain.create(SortedRangeSet.of(Range.lessThanOrEqual(function.apply(max))), hasNullValue);
        }
        if (min != null) {
            return Domain.create(SortedRangeSet.of(Range.greaterThanOrEqual(function.apply(min))), hasNullValue);
        }
        return Domain.create(SortedRangeSet.all(boxedJavaType), hasNullValue);
    }

    public static class ColumnReference<C>
    {
        private final C column;
        private final int ordinal;
        private final Type type;

        public ColumnReference(C column, int ordinal, Type type)
        {
            this.column = requireNonNull(column, "column is null");
            checkArgument(ordinal >= 0, "ordinal is negative");
            this.ordinal = ordinal;
            this.type = requireNonNull(type, "type is null");
        }

        public C getColumn()
        {
            return column;
        }

        public int getOrdinal()
        {
            return ordinal;
        }

        public Type getType()
        {
            return type;
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("column", column)
                    .add("ordinal", ordinal)
                    .add("type", type)
                    .toString();
        }
    }
}
