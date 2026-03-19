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
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;

import java.util.List;
import java.util.Optional;

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
            Long datasetVersion,
            ArrowBlockBuilder arrowBlockBuilder,
            BufferAllocator parentAllocator,
            Optional<String> filter,
            List<String> filterProjectionColumns)
    {
        super(tableHandle, columns, new FragmentScannerFactory(fragments, tablePath, readBatchSize, namespaceHolder, datasetVersion, filterProjectionColumns), arrowBlockBuilder, parentAllocator, filter);
    }

    private static class FragmentScannerFactory
            implements ScannerFactory
    {
        private final List<Integer> fragmentIds;
        private final String tablePath;
        private final int readBatchSize;
        private final LanceNamespaceHolder namespaceHolder;
        private final Long datasetVersion;
        private final List<String> filterProjectionColumns;
        private LanceScanner scanner;

        FragmentScannerFactory(List<Integer> fragmentIds, String tablePath, int readBatchSize, LanceNamespaceHolder namespaceHolder, Long datasetVersion, List<String> filterProjectionColumns)
        {
            this.fragmentIds = ImmutableList.copyOf(fragmentIds);
            this.tablePath = requireNonNull(tablePath, "tablePath is null");
            this.readBatchSize = readBatchSize;
            this.namespaceHolder = requireNonNull(namespaceHolder, "namespaceHolder is null");
            this.datasetVersion = datasetVersion;
            this.filterProjectionColumns = ImmutableList.copyOf(filterProjectionColumns);
        }

        @Override
        public LanceScanner open(BufferAllocator allocator, List<String> columns, Optional<String> filter)
        {
            ScanOptions.Builder optionsBuilder = new ScanOptions.Builder();

            // Combine output columns with filter projection columns
            List<String> allColumns = ImmutableList.<String>builder()
                    .addAll(columns)
                    .addAll(filterProjectionColumns)
                    .build();

            if (!allColumns.isEmpty()) {
                optionsBuilder.columns(allColumns);
            }
            optionsBuilder.batchSize(readBatchSize);
            optionsBuilder.fragmentIds(fragmentIds);
            filter.ifPresent(optionsBuilder::filter);

            Dataset dataset = namespaceHolder.getCachedDataset(tablePath, datasetVersion);
            this.scanner = dataset.newScan(optionsBuilder.build());
            return scanner;
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
