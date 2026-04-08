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
package com.facebook.presto.iceberg.procedure.splits;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergSplitSource;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.iceberg.util.FilteredTableScan;
import com.facebook.presto.spi.ConnectorSession;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Split source for rewrite_data_files procedure that applies min-input-files filtering.
 * Filters out partitions with fewer files than the specified threshold.
 */
public class RewriteDataFilesIcebergSplitSource
        extends IcebergSplitSource
{
    public RewriteDataFilesIcebergSplitSource(
            ConnectorSession session,
            TableScan tableScan,
            TupleDomain<IcebergColumnHandle> metadataColumnConstraints,
            Map<String, String> options)
    {
        super(session, applyFilters(tableScan, options), metadataColumnConstraints);
    }

    private static TableScan applyFilters(TableScan tableScan, Map<String, String> options)
    {
        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            List<FileScanTask> tasks = new ArrayList<>();
            fileScanTasks.forEach(tasks::add);

            // Apply filtering using options
            List<FileScanTask> filteredTasks = IcebergUtil.filterByFile(tasks, options);
            filteredTasks = IcebergUtil.filterByGroup(filteredTasks, options);

            return new FilteredTableScan(tableScan, filteredTasks);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to filter table scan", e);
        }
    }
}
