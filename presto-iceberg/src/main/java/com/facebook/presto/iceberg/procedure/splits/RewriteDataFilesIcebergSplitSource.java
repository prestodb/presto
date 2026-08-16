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
import com.facebook.presto.spi.ConnectorSession;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;

import java.util.Map;

import static com.facebook.presto.iceberg.IcebergUtil.filterByFile;
import static com.facebook.presto.iceberg.IcebergUtil.filterByGroup;
import static com.facebook.presto.iceberg.IcebergUtil.getTargetSplitSize;

/**
 * Split source for rewrite_data_files procedure that applies filtering based on procedure options.
 * Supports file-level filters (min-file-size-bytes, max-file-size-bytes) and group-level filters (min-input-files).
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
        super(session, getTargetSplitSize(session, tableScan).toBytes(), applyFilters(tableScan, options), metadataColumnConstraints);
    }

    private static CloseableIterable<FileScanTask> applyFilters(TableScan tableScan, Map<String, String> options)
    {
        return filterByGroup(filterByFile(tableScan.planFiles(), options), options);
    }
}
