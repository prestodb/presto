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
package com.facebook.presto.lance;

import com.facebook.airlift.log.Logger;
import com.facebook.plugin.arrow.ArrowBlockBuilder;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.memory.BufferAllocator;
import org.lance.Dataset;
import org.lance.index.DistanceType;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.Query;
import org.lance.ipc.ScanOptions;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class LanceFragmentPageSource
        extends LanceBasePageSource
{
    private static final Logger log = Logger.get(LanceFragmentPageSource.class);

    public LanceFragmentPageSource(
            LanceTableHandle tableHandle,
            List<LanceColumnHandle> columns,
            List<Integer> fragments,
            String tablePath,
            int readBatchSize,
            LanceNamespaceHolder namespaceHolder,
            Optional<Long> datasetVersion,
            ArrowBlockBuilder arrowBlockBuilder,
            BufferAllocator parentAllocator,
            Optional<String> filter,
            List<String> filterProjectionColumns)
    {
        this(tableHandle, columns, fragments, tablePath, readBatchSize, namespaceHolder, datasetVersion, arrowBlockBuilder, parentAllocator, filter, filterProjectionColumns, false);
    }

    public LanceFragmentPageSource(
            LanceTableHandle tableHandle,
            List<LanceColumnHandle> columns,
            List<Integer> fragments,
            String tablePath,
            int readBatchSize,
            LanceNamespaceHolder namespaceHolder,
            Optional<Long> datasetVersion,
            ArrowBlockBuilder arrowBlockBuilder,
            BufferAllocator parentAllocator,
            Optional<String> filter,
            List<String> filterProjectionColumns,
            boolean includeRowAddress)
    {
        super(tableHandle, columns, new FragmentScannerFactory(fragments, tablePath, readBatchSize, namespaceHolder, datasetVersion, filterProjectionColumns, tableHandle.getLimit(), includeRowAddress, tableHandle.getVectorSearch()), arrowBlockBuilder, parentAllocator, filter);
    }

    private static class FragmentScannerFactory
            implements ScannerFactory
    {
        private final List<Integer> fragmentIds;
        private final String tablePath;
        private final int readBatchSize;
        private final LanceNamespaceHolder namespaceHolder;
        private final Optional<Long> datasetVersion;
        private final List<String> filterProjectionColumns;
        private final OptionalLong limit;
        private final boolean includeRowAddress;
        private final Optional<LanceVectorSearch> vectorSearch;
        private LanceScanner scanner;

        FragmentScannerFactory(List<Integer> fragmentIds, String tablePath, int readBatchSize, LanceNamespaceHolder namespaceHolder, Optional<Long> datasetVersion, List<String> filterProjectionColumns, OptionalLong limit, boolean includeRowAddress, Optional<LanceVectorSearch> vectorSearch)
        {
            this.fragmentIds = ImmutableList.copyOf(fragmentIds);
            this.tablePath = requireNonNull(tablePath, "tablePath is null");
            this.readBatchSize = readBatchSize;
            this.namespaceHolder = requireNonNull(namespaceHolder, "namespaceHolder is null");
            this.datasetVersion = requireNonNull(datasetVersion, "datasetVersion is null");
            this.filterProjectionColumns = ImmutableList.copyOf(filterProjectionColumns);
            this.limit = limit;
            this.includeRowAddress = includeRowAddress;
            this.vectorSearch = requireNonNull(vectorSearch, "vectorSearch is null");
        }

        @Override
        public LanceScanner open(BufferAllocator allocator, List<String> columns, Optional<String> filter)
        {
            ScanOptions.Builder optionsBuilder = new ScanOptions.Builder();

            // Combine output columns with filter projection columns. The _rowaddr metadata
            // column is produced via withRowAddress(true), not via the column projection list,
            // so it must be excluded from columns(). _distance is appended by Lance to every
            // nearest-scan batch and is likewise not a stored column.
            List<String> allColumns = ImmutableList.<String>builder()
                    .addAll(columns)
                    .addAll(filterProjectionColumns)
                    .build()
                    .stream()
                    .filter(name -> !RowAddress.ROW_ADDRESS_COLUMN_NAME.equals(name)
                            && !LanceVectorSearch.DISTANCE_COLUMN_NAME.equals(name))
                    .distinct()
                    .collect(toImmutableList());

            if (!allColumns.isEmpty()) {
                optionsBuilder.columns(allColumns);
            }
            optionsBuilder.batchSize(readBatchSize);
            // Do NOT set fragmentIds when a nearest (vector search) query is active:
            // lance-core (verified through 11.0.0) silently downgrades a nearest scan to a plain scan when
            // fragment IDs are specified, producing batches without the _distance column.
            if (!vectorSearch.isPresent()) {
                optionsBuilder.fragmentIds(fragmentIds);
            }
            if (includeRowAddress) {
                optionsBuilder.withRowAddress(true);
            }
            filter.ifPresent(optionsBuilder::filter);
            limit.ifPresent(optionsBuilder::limit);
            vectorSearch.ifPresent(search -> optionsBuilder.nearest(buildNearestQuery(search)));

            Dataset dataset = namespaceHolder.getCachedDataset(tablePath, datasetVersion);
            this.scanner = dataset.newScan(optionsBuilder.build());
            return scanner;
        }

        private static Query buildNearestQuery(LanceVectorSearch search)
        {
            Query.Builder queryBuilder = new Query.Builder()
                    .setColumn(search.getColumn())
                    .setKey(search.getQueryVectorAsFloats())
                    .setK(search.getK())
                    .setDistanceType(toDistanceType(search.getMetric()))
                    .setUseIndex(true);
            search.getNprobes().ifPresent(queryBuilder::setNprobes);
            search.getEf().ifPresent(queryBuilder::setEf);
            search.getRefineFactor().ifPresent(queryBuilder::setRefineFactor);
            return queryBuilder.build();
        }

        private static DistanceType toDistanceType(String metric)
        {
            switch (metric) {
                case LanceVectorSearch.METRIC_L2:
                    return DistanceType.L2;
                case LanceVectorSearch.METRIC_COSINE:
                    return DistanceType.Cosine;
                case LanceVectorSearch.METRIC_DOT:
                    return DistanceType.Dot;
                default:
                    throw new IllegalArgumentException("Unsupported Lance vector distance metric: " + metric);
            }
        }

        @Override
        public void close()
        {
            try {
                if (scanner != null) {
                    scanner.close();
                }
            }
            catch (Exception e) {
                log.warn(e, "Error closing lance scanner");
            }
            // Do NOT close the dataset — the cache manages its lifecycle
        }
    }
}
