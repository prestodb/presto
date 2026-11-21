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
package com.facebook.presto.ducklake.catalog;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

/**
 * A row versioned with {@code begin_snapshot}/{@code end_snapshot} columns is visible at a given
 * snapshot when {@code snapshotId >= begin_snapshot AND (snapshotId < end_snapshot OR
 * end_snapshot IS NULL)}. This class renders that predicate as parameterized SQL for a given
 * table alias, for use against any of the versioned DuckLake catalog tables (for example
 * {@code ducklake_schema}, {@code ducklake_table}, {@code ducklake_column}, {@code
 * ducklake_data_file}, {@code ducklake_delete_file}, {@code ducklake_partition_info}).
 */
public final class SnapshotPredicate
{
    private final String alias;
    private final long snapshotId;

    public SnapshotPredicate(String alias, long snapshotId)
    {
        requireNonNull(alias, "alias is null");
        checkArgument(!alias.isEmpty(), "alias is empty");
        this.alias = alias;
        this.snapshotId = snapshotId;
    }

    /**
     * Returns the predicate as parameterized SQL with two {@code ?} placeholders, both bound to
     * the snapshot id. Combine with {@link #parameters()} when building a prepared statement.
     */
    public String sql()
    {
        return "? >= " + alias + ".begin_snapshot AND (? < " + alias + ".end_snapshot OR " + alias + ".end_snapshot IS NULL)";
    }

    /**
     * Returns the parameter values for the two {@code ?} placeholders in {@link #sql()}, in
     * order: the snapshot id, twice.
     */
    public List<Object> parameters()
    {
        return asList(snapshotId, snapshotId);
    }
}
