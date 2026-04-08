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
package com.facebook.presto.iceberg.util;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A TableScan wrapper that returns a pre-filtered list of FileScanTasks.
 * All other TableScan methods delegate to the original scan.
 *
 * This is useful when you need to filter files before they are processed by split sources,
 * such as filtering by partition, file count, file size, etc.
 */
public class FilteredTableScan
        implements TableScan
{
    private final TableScan delegate;
    private final List<FileScanTask> filteredTasks;

    public FilteredTableScan(TableScan delegate, List<FileScanTask> filteredTasks)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.filteredTasks = requireNonNull(filteredTasks, "filteredTasks is null");
    }

    @Override
    public CloseableIterable<FileScanTask> planFiles()
    {
        return CloseableIterable.withNoopClose(filteredTasks);
    }

    // All other methods delegate to the original scan

    @Override
    public org.apache.iceberg.Table table()
    {
        return delegate.table();
    }

    @Override
    public TableScan useSnapshot(long snapshotId)
    {
        return delegate.useSnapshot(snapshotId);
    }

    @Override
    public TableScan asOfTime(long timestampMillis)
    {
        return delegate.asOfTime(timestampMillis);
    }

    @Override
    public TableScan option(String property, String value)
    {
        return delegate.option(property, value);
    }

    @Override
    public TableScan project(org.apache.iceberg.Schema schema)
    {
        return delegate.project(schema);
    }

    @Override
    public TableScan caseSensitive(boolean caseSensitive)
    {
        return delegate.caseSensitive(caseSensitive);
    }

    @Override
    public TableScan includeColumnStats()
    {
        return delegate.includeColumnStats();
    }

    @Override
    public TableScan select(java.util.Collection<String> columns)
    {
        return delegate.select(columns);
    }

    @Override
    public TableScan filter(org.apache.iceberg.expressions.Expression expr)
    {
        return delegate.filter(expr);
    }

    @Override
    public org.apache.iceberg.expressions.Expression filter()
    {
        return delegate.filter();
    }

    @Override
    public TableScan planWith(java.util.concurrent.ExecutorService executorService)
    {
        return delegate.planWith(executorService);
    }

    @Override
    public org.apache.iceberg.Schema schema()
    {
        return delegate.schema();
    }

    @Override
    public org.apache.iceberg.Snapshot snapshot()
    {
        return delegate.snapshot();
    }

    @Override
    public CloseableIterable<org.apache.iceberg.CombinedScanTask> planTasks()
    {
        return delegate.planTasks();
    }

    @Override
    public boolean isCaseSensitive()
    {
        return delegate.isCaseSensitive();
    }

    @Override
    public TableScan metricsReporter(org.apache.iceberg.metrics.MetricsReporter reporter)
    {
        return delegate.metricsReporter(reporter);
    }

    @Override
    public long targetSplitSize()
    {
        return delegate.targetSplitSize();
    }

    @Override
    public int splitLookback()
    {
        return delegate.splitLookback();
    }

    @Override
    public long splitOpenFileCost()
    {
        return delegate.splitOpenFileCost();
    }

    @Override
    public TableScan ignoreResiduals()
    {
        return delegate.ignoreResiduals();
    }
}
