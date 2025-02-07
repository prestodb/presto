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
package com.facebook.presto.iceberg;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.SortOrder.unsorted;

/**
 * This is a dummy class required for {@link org.apache.iceberg.MetricsConfig#forTable}
 */
public class PrestoIcebergTableForMetricsConfig
        implements Table
{
    private final Schema schema;
    private final PartitionSpec spec;
    private final Optional<SortOrder> sortOrder;
    private final Map<String, String> properties;

    protected PrestoIcebergTableForMetricsConfig(Schema schema, PartitionSpec spec, Map<String, String> properties, Optional<SortOrder> sortOrder)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.spec = requireNonNull(spec, "partition spec is null");
        this.sortOrder = requireNonNull(sortOrder, "sortOrder is null");
        this.properties = requireNonNull(properties, "table properties is null");
    }

    @Override
    public void refresh()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public TableScan newScan()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public Schema schema()
    {
        return this.schema;
    }

    @Override
    public Map<Integer, Schema> schemas()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public PartitionSpec spec()
    {
        return this.spec;
    }

    @Override
    public Map<Integer, PartitionSpec> specs()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public SortOrder sortOrder()
    {
        return this.sortOrder.orElse(unsorted());
    }

    @Override
    public Map<Integer, SortOrder> sortOrders()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public Map<String, String> properties()
    {
        return this.properties;
    }

    @Override
    public String location()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public Snapshot currentSnapshot()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public Snapshot snapshot(long snapshotId)
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public Iterable<Snapshot> snapshots()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public List<HistoryEntry> history()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public UpdateSchema updateSchema()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public UpdatePartitionSpec updateSpec()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public UpdateProperties updateProperties()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public ReplaceSortOrder replaceSortOrder()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public UpdateLocation updateLocation()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public AppendFiles newAppend()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public RewriteFiles newRewrite()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public RewriteManifests rewriteManifests()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public OverwriteFiles newOverwrite()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public RowDelta newRowDelta()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public ReplacePartitions newReplacePartitions()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public DeleteFiles newDelete()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public ExpireSnapshots expireSnapshots()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public ManageSnapshots manageSnapshots()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public Transaction newTransaction()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public FileIO io()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public EncryptionManager encryption()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public LocationProvider locationProvider()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public List<StatisticsFile> statisticsFiles()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }

    @Override
    public Map<String, SnapshotRef> refs()
    {
        throw new UnsupportedOperationException("Operation is not supported in PrestoIcebergTable");
    }
}
