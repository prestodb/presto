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
package com.facebook.presto.delta;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.utils.CloseableIterator;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Predicate;

import static com.facebook.presto.delta.DeltaColumnHandle.ColumnType.PARTITION;
import static com.facebook.presto.delta.DeltaTypeUtils.convertPartitionValue;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;

public final class DeltaExpressionUtils
{
    private static final Logger logger = Logger.get(DeltaExpressionUtils.class);
    private DeltaExpressionUtils()
    {
    }

    /**
     * Split the predicate into partition and regular column predicates
     */
    public static List<TupleDomain<ColumnHandle>> splitPredicate(
            TupleDomain<ColumnHandle> predicate)
    {
        ImmutableMap.Builder<ColumnHandle, Domain> partitionColumnPredicates = ImmutableMap.builder();
        ImmutableMap.Builder<ColumnHandle, Domain> regularColumnPredicates = ImmutableMap.builder();

        Optional<Map<ColumnHandle, Domain>> domains = predicate.getDomains();
        domains.ifPresent(columnHandleDomainMap -> columnHandleDomainMap.forEach((key, value) -> {
            DeltaColumnHandle columnHandle = (DeltaColumnHandle) key;
            if (columnHandle.getColumnType() == PARTITION) {
                partitionColumnPredicates.put(key, value);
            }
            else {
                regularColumnPredicates.put(key, value);
            }
        }));

        return ImmutableList.of(
                TupleDomain.withColumnDomains(partitionColumnPredicates.build()),
                TupleDomain.withColumnDomains(regularColumnPredicates.build()));
    }

    /**
     * Utility method that takes an iterator of {@link FilteredColumnarBatch}s and a predicate and returns an iterator
     * of {@link FilteredColumnarBatch}s that satisfy the predicate (predicate evaluates to a deterministic NO)
     */
    public static CloseableIterator<Row> iterateWithPartitionPruning(
            CloseableIterator<FilteredColumnarBatch> inputIterator,
            TupleDomain<DeltaColumnHandle> predicate,
            TypeManager typeManager,
            boolean legacyTimestamp,
            ZoneId sessionZone)
    {
        TupleDomain<String> partitionPredicate = extractPartitionColumnsPredicate(predicate);
        if (partitionPredicate.isAll()) {
            return new AllFilesIterator(inputIterator);
        }

        if (partitionPredicate.isNone()) {
            // nothing passes the partition predicate, return empty iterator
            return new NoneFilesIterator(inputIterator);
        }

        Optional<List<TupleDomain.ColumnDomain<DeltaColumnHandle>>> columnDomains = predicate.getColumnDomains();
        List<DeltaColumnHandle> partitionColumns = columnDomains.map(domains -> domains.stream()
                .filter(entry -> entry.getColumn().getColumnType() == PARTITION)
                .map(TupleDomain.ColumnDomain::getColumn)
                .collect(toImmutableList())).orElse(ImmutableList.of());

        return new FilteredByPredicateIterator(inputIterator, partitionPredicate, partitionColumns, typeManager, legacyTimestamp, sessionZone);
    }

    private static TupleDomain<String> extractPartitionColumnsPredicate(TupleDomain<DeltaColumnHandle> predicate)
    {
        return predicate.transform(
                deltaColumnHandle -> {
                    if (deltaColumnHandle.getColumnType() != PARTITION) {
                        return null;
                    }
                    return deltaColumnHandle.getLogicalName();
                });
    }

    private static class NoneFilesIterator
            implements CloseableIterator<Row>
    {
        private final CloseableIterator<FilteredColumnarBatch> inputIterator;

        NoneFilesIterator(CloseableIterator<FilteredColumnarBatch> inputIterator)
        {
            this.inputIterator = inputIterator;
        }

        @Override
        public boolean hasNext()
        {
            return false;
        }

        @Override
        public Row next()
        {
            throw new NoSuchElementException();
        }

        @Override
        public void close()
                throws IOException
        {
            inputIterator.close();
        }
    }

    private static class BatchRowIterator
            implements CloseableIterator<Row>
    {
        private final CloseableIterator<FilteredColumnarBatch> inputIterator;
        private final Iterator<Row> rows;
        private CloseableIterator<Row> prev;

        public BatchRowIterator(CloseableIterator<FilteredColumnarBatch> inputIterator,
                Optional<Predicate<Row>> rowFilter)
        {
            this.inputIterator = inputIterator;
            this.rows = Streams.stream(inputIterator)
                    .flatMap(batch -> {
                        if (prev != null) {
                            try {
                                prev.close();
                            }
                            catch (IOException e) {
                                throw new RuntimeException("Failed to close previous row batch", e);
                            }
                        }
                        prev = batch.getRows();
                        return Streams.stream(prev);
                    })
                    // if there is a filter to be applied, it applies it
                    .filter(row -> rowFilter.map(predicate -> predicate.test(row)).orElse(true))
                    .iterator();
        }

        @Override
        public boolean hasNext()
        {
            return rows.hasNext();
        }

        @Override
        public Row next()
        {
            return rows.next();
        }

        @Override
        public void close() throws IOException
        {
            if (prev != null) {
                prev.close();
            }
            if (inputIterator != null) {
                inputIterator.close();
            }
        }
    }

    private static class AllFilesIterator
            extends BatchRowIterator
    {
        public AllFilesIterator(CloseableIterator<FilteredColumnarBatch> inputIterator)
        {
            super(inputIterator, Optional.empty());
        }
    }

    private static class FilteredByPredicateIterator
            extends BatchRowIterator
    {
        public FilteredByPredicateIterator(CloseableIterator<FilteredColumnarBatch> inputIterator,
                TupleDomain<String> partitionPredicate,
                List<DeltaColumnHandle> partitionColumns,
                TypeManager typeManager,
                boolean legacyTimestamp,
                ZoneId sessionZone)
        {
            super(inputIterator,
                    Optional.of(row -> evaluatePartitionPredicate(
                            partitionPredicate, partitionColumns, typeManager, legacyTimestamp, sessionZone, row)));
        }

        private static boolean evaluatePartitionPredicate(
                TupleDomain<String> partitionPredicate,
                List<DeltaColumnHandle> partitionColumns,
                TypeManager typeManager,
                boolean legacyTimestamp,
                ZoneId sessionZone,
                Row row)
        {
            checkArgument(!partitionPredicate.isNone(), "Expecting a predicate with at least one expression");
            for (DeltaColumnHandle partitionColumn : partitionColumns) {
                String columnName = partitionColumn.getLogicalName();
                String partitionValue = InternalScanFileUtils.getPartitionValues(row).get(columnName);
                String filePath = InternalScanFileUtils.getAddFileStatus(row).getPath();
                logger.debug("Obtaining domain of file: " + filePath);
                Domain domain = getDomain(partitionColumn, partitionValue, typeManager, legacyTimestamp, sessionZone);
                Optional<Map<String, Domain>> domains = partitionPredicate.getDomains();
                if (!domains.isPresent()) {
                    logger.debug("Domain is not present in file: " + filePath);
                    return false;
                }
                Domain columnPredicate = domains.get().get(columnName);

                if (columnPredicate == null) {
                    continue; // there is no predicate on this column
                }

                if (columnPredicate.intersect(domain).isNone()) {
                    logger.debug("Empty set after domain intersection with file: " + filePath);
                    return false;
                }
            }

            return true;
        }

        /**
         * Returns the domain a file's partition value stands for. It is
         * intersected with the query's predicate to decide whether the file can
         * be skipped, so the value has to be in the same internal representation
         * the column is read as -- days for DATE, a wall clock for TIMESTAMP, a
         * packed value for TIMESTAMP WITH TIME ZONE. Converting it the way
         * DeltaPageSource materializes the column is what keeps the two in step;
         * a second conversion here drifted from it three times over.
         */
        private static Domain getDomain(
                DeltaColumnHandle columnHandle,
                String partitionValue,
                TypeManager typeManager,
                boolean legacyTimestamp,
                ZoneId sessionZone)
        {
            Type type = typeManager.getType(columnHandle.getDataType());
            if (partitionValue == null) {
                return Domain.onlyNull(type);
            }

            return Domain.create(
                    ValueSet.of(
                            type,
                            convertPartitionValue(
                                    columnHandle.getLogicalName(),
                                    partitionValue,
                                    type,
                                    legacyTimestamp,
                                    sessionZone)),
                    false);
        }
    }
}
