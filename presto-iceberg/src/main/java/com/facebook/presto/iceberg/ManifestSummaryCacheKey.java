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

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Identifies a folded manifest summary. Every input the summary is derived from is part of the key,
 * so an entry can never go stale and may be shared by any query, session or user:
 *
 * <ul>
 *   <li>the snapshot pins the set of data files, and a snapshot is immutable once written
 *   <li>the schema id pins the field-id to type mapping used to decode the per-file bounds
 *   <li>the partition spec id pins which fields the summary folds min/max over
 *   <li>the predicate and the requested columns determine which files are scanned and which
 *       columns' metrics are read
 * </ul>
 *
 * The table name discriminates too, and is compared, so that identity does not rest on snapshot ids
 * never colliding across two tables.
 *
 * <p>Two premises this rests on, stated because the absence of any invalidation logic depends on
 * them:
 *
 * <ul>
 *   <li>A snapshot id is not reused within the life of a table. Iceberg generates them randomly, so
 *       normal commits satisfy this; hand-rolled metadata rollback or re-registering a table against
 *       different metadata does not necessarily, and would need the cache flushed.
 *   <li>The cache holding these keys is scoped to one catalog. {@code SchemaTableName} carries no
 *       catalog name, so a cache shared across catalogs would collide on same-named tables. The
 *       provider in {@code IcebergCommonModule} is {@code @Singleton} within the connector's
 *       injector, which gives one cache per catalog.
 * </ul>
 */
public class ManifestSummaryCacheKey
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(ManifestSummaryCacheKey.class).instanceSize();
    // One column id, one domain, or one range or value of a domain. Rough on purpose, like the entry
    // size Partition uses: the point is that a predicate with a long IN list is charged for, not that
    // it is charged exactly.
    private static final long ENTRY_SIZE = 64;

    private final SchemaTableName tableName;
    private final long snapshotId;
    private final int schemaId;
    private final int partitionSpecId;
    private final TupleDomain<IcebergColumnHandle> predicate;
    private final List<Integer> columnIds;

    public ManifestSummaryCacheKey(
            SchemaTableName tableName,
            long snapshotId,
            int schemaId,
            int partitionSpecId,
            TupleDomain<IcebergColumnHandle> predicate,
            List<Integer> columnIds)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.snapshotId = snapshotId;
        this.schemaId = schemaId;
        this.partitionSpecId = partitionSpecId;
        this.predicate = requireNonNull(predicate, "predicate is null");
        // Sorted here rather than by the caller: the requested columns are a set, so two callers
        // naming the same columns in a different order must land on the same entry, and leaving that
        // to each call site is a silent hit-rate regression waiting to happen.
        this.columnIds = ImmutableList.sortedCopyOf(requireNonNull(columnIds, "columnIds is null"));
    }

    /**
     * Rough retained size, for weighing cache entries. The key holds the predicate, which can be far
     * larger than the summary it names: an IN list of thousands of values on a narrow table.
     */
    public long getEstimatedSize()
    {
        long predicateEntries = predicate.getDomains()
                .map(domains -> domains.size() + domains.values().stream()
                        .mapToLong(ManifestSummaryCacheKey::valueCount)
                        .sum())
                .orElse(0L);
        return INSTANCE_SIZE + (columnIds.size() + predicateEntries) * ENTRY_SIZE;
    }

    private static long valueCount(Domain domain)
    {
        return domain.getValues().getValuesProcessor().transform(
                ranges -> (long) ranges.getRangeCount(),
                discreteValues -> (long) discreteValues.getValues().size(),
                allOrNone -> 0L);
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        ManifestSummaryCacheKey that = (ManifestSummaryCacheKey) other;
        return snapshotId == that.snapshotId &&
                schemaId == that.schemaId &&
                partitionSpecId == that.partitionSpecId &&
                tableName.equals(that.tableName) &&
                predicate.equals(that.predicate) &&
                columnIds.equals(that.columnIds);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, snapshotId, schemaId, partitionSpecId, predicate, columnIds);
    }

    @Override
    public String toString()
    {
        return "ManifestSummaryCacheKey{" +
                "tableName=" + tableName +
                ", snapshotId=" + snapshotId +
                ", schemaId=" + schemaId +
                ", partitionSpecId=" + partitionSpecId +
                ", columnIds=" + columnIds +
                '}';
    }
}
