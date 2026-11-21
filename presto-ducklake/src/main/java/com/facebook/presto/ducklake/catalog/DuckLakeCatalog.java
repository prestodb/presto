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

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * The Java-side view of a DuckLake catalog database. Every versioned catalog row is visible at a
 * snapshot when {@code snapshotId >= begin_snapshot AND (snapshotId < end_snapshot OR
 * end_snapshot IS NULL)} (see {@link SnapshotPredicate}); a snapshot id is resolved once per
 * table handle and passed to every catalog call so that time travel is just a different id.
 * Task 1.2 and Task 1.3 implement this interface against a PostgreSQL catalog database over JDBC;
 * this interface itself opens no database connection.
 */
public interface DuckLakeCatalog
{
    /**
     * Resolves the latest snapshot id, {@code max(snapshot_id)} of {@code ducklake_snapshot}.
     *
     * @throws com.facebook.presto.spi.PrestoException with {@link
     *     com.facebook.presto.ducklake.DuckLakeErrorCode#DUCKLAKE_INVALID_METADATA} if {@code
     *     ducklake_snapshot} has no rows
     */
    long getLatestSnapshotId();

    /**
     * Looks up one row of {@code ducklake_snapshot} (joined with {@code
     * ducklake_snapshot_changes}) by {@code snapshot_id}. Not subject to the snapshot predicate:
     * a snapshot row is never versioned.
     */
    Optional<DuckLakeSnapshot> getSnapshot(long snapshotId);

    /**
     * Finds the greatest {@code ducklake_snapshot} row whose {@code snapshot_time <= time}, for
     * resolving a table handle from an {@code AS OF <timestamp>} query. Not subject to the
     * snapshot predicate.
     */
    Optional<DuckLakeSnapshot> getSnapshotAtOrBefore(Instant time);

    /**
     * Lists every row of {@code ducklake_snapshot}, joined with {@code
     * ducklake_snapshot_changes}, ascending by {@code snapshot_id}. Backs the {@code $snapshots}
     * system table. Not subject to the snapshot predicate: all snapshots are always listed.
     */
    List<DuckLakeSnapshot> listSnapshots();

    /**
     * Lists the {@code ducklake_schema} rows visible at {@code snapshotId}.
     */
    List<DuckLakeSchema> listSchemas(long snapshotId);

    /**
     * Looks up the {@code ducklake_schema} row named {@code schemaName} visible at {@code
     * snapshotId}.
     */
    Optional<DuckLakeSchema> getSchema(long snapshotId, String schemaName);

    /**
     * Lists the {@code ducklake_table} rows of {@code schema} visible at {@code snapshotId}.
     */
    List<DuckLakeTable> listTables(long snapshotId, DuckLakeSchema schema);

    /**
     * Looks up the {@code ducklake_table} row named {@code tableName} in {@code schema}, visible
     * at {@code snapshotId}.
     */
    Optional<DuckLakeTable> getTable(long snapshotId, DuckLakeSchema schema, String tableName);

    /**
     * Lists every {@code ducklake_column} row of {@code tableId} visible at {@code snapshotId},
     * including nested children, ordered by {@code column_order}.
     */
    List<DuckLakeColumnRow> listColumns(long snapshotId, long tableId);

    /**
     * Lists the {@code ducklake_partition_column} rows of the {@code ducklake_partition_info} row
     * of {@code tableId} visible at {@code snapshotId}, ordered by {@code partition_key_index}.
     */
    List<DuckLakePartitionField> listPartitionFields(long snapshotId, long tableId);

    /**
     * Lists the {@code ducklake_data_file} rows of {@code table} visible at {@code snapshotId},
     * ordered by {@code file_order}, each with its {@code ducklake_delete_file} (if any, also
     * subject to the snapshot predicate), {@code ducklake_file_partition_value} rows and {@code
     * ducklake_file_column_stats} rows attached.
     */
    List<DuckLakeDataFile> listDataFiles(long snapshotId, DuckLakeTable table);

    /**
     * Looks up the {@code ducklake_table_stats} row of {@code tableId}. Not subject to the
     * snapshot predicate: table stats are maintained as a single current row, not versioned.
     */
    Optional<DuckLakeTableStats> getTableStats(long tableId);

    /**
     * Lists the {@code ducklake_table_column_stats} rows of {@code tableId}. Not subject to the
     * snapshot predicate: table column stats are maintained as current rows, not versioned.
     */
    List<DuckLakeTableColumnStats> listTableColumnStats(long tableId);

    /**
     * Lists the {@code ducklake_inlined_data_tables} rows of {@code tableId}. Not subject to the
     * snapshot predicate: this table has no {@code begin_snapshot}/{@code end_snapshot} columns.
     */
    List<DuckLakeInlinedTable> listInlinedTables(long tableId);

    /**
     * Opens a streaming reader over the rows of {@code inlinedTable}, restricted to {@code
     * columnNames} and visible at {@code snapshotId}. Used only by the page source implemented in
     * Task 4.4.
     */
    DuckLakeInlinedRowSource openInlinedRows(long snapshotId, DuckLakeInlinedTable inlinedTable, List<String> columnNames);
}
