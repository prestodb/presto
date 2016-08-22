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
package com.facebook.presto.raptor.metadata;

import com.facebook.presto.raptor.metadata.Table.TableMapper;
import com.facebook.presto.spi.SchemaTableName;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;

import java.util.List;
import java.util.Set;

public interface MetadataDao
{
    String TABLE_COLUMN_SELECT = "" +
            "SELECT t.schema_name, t.table_name,\n" +
            "  c.column_id, c.column_name, c.data_type,\n" +
            "  c.bucket_ordinal_position, c.sort_ordinal_position,\n" +
            "  t.temporal_column_id = c.column_id AS temporal\n" +
            "FROM tables t\n" +
            "JOIN columns c ON (t.table_id = c.table_id)\n";

    @SqlQuery("SELECT t.table_id, t.distribution_id, d.bucket_count, t.temporal_column_id\n" +
            "FROM tables t\n" +
            "LEFT JOIN distributions d ON (t.distribution_id = d.distribution_id)\n" +
            "WHERE t.table_id = :tableId")
    @Mapper(TableMapper.class)
    Table getTableInformation(@Bind("tableId") long tableId);

    @SqlQuery("SELECT t.table_id, t.distribution_id, d.distribution_name, d.bucket_count, t.temporal_column_id\n" +
            "FROM tables t\n" +
            "LEFT JOIN distributions d ON (t.distribution_id = d.distribution_id)\n" +
            "WHERE t.schema_name = :schemaName\n" +
            "  AND t.table_name = :tableName")
    @Mapper(TableMapper.class)
    Table getTableInformation(
            @Bind("schemaName") String schemaName,
            @Bind("tableName") String tableName);

    @SqlQuery(TABLE_COLUMN_SELECT +
            "WHERE t.table_id = :tableId\n" +
            "  AND c.column_id = :columnId\n" +
            "ORDER BY c.ordinal_position\n")
    TableColumn getTableColumn(
            @Bind("tableId") long tableId,
            @Bind("columnId") long columnId);

    @SqlQuery("SELECT schema_name, table_name\n" +
            "FROM tables\n" +
            "WHERE (schema_name = :schemaName OR :schemaName IS NULL)")
    @Mapper(SchemaTableNameMapper.class)
    List<SchemaTableName> listTables(
            @Bind("schemaName") String schemaName);

    @SqlQuery("SELECT DISTINCT schema_name FROM tables")
    List<String> listSchemaNames();

    @SqlQuery(TABLE_COLUMN_SELECT +
            "WHERE (schema_name = :schemaName OR :schemaName IS NULL)\n" +
            "  AND (table_name = :tableName OR :tableName IS NULL)\n" +
            "ORDER BY schema_name, table_name, ordinal_position")
    List<TableColumn> listTableColumns(
            @Bind("schemaName") String schemaName,
            @Bind("tableName") String tableName);

    @SqlQuery(TABLE_COLUMN_SELECT +
            "WHERE t.table_id = :tableId\n" +
            "ORDER BY c.ordinal_position")
    List<TableColumn> listTableColumns(@Bind("tableId") long tableId);

    @SqlQuery(TABLE_COLUMN_SELECT +
            "WHERE t.table_id = :tableId\n" +
            "  AND c.sort_ordinal_position IS NOT NULL\n" +
            "ORDER BY c.sort_ordinal_position")
    List<TableColumn> listSortColumns(@Bind("tableId") long tableId);

    @SqlQuery(TABLE_COLUMN_SELECT +
            "WHERE t.table_id = :tableId\n" +
            "  AND c.bucket_ordinal_position IS NOT NULL\n" +
            "ORDER BY c.bucket_ordinal_position")
    List<TableColumn> listBucketColumns(@Bind("tableId") long tableId);

    @SqlQuery("SELECT schema_name, table_name, data\n" +
            "FROM views\n" +
            "WHERE (schema_name = :schemaName OR :schemaName IS NULL)")
    @Mapper(SchemaTableNameMapper.class)
    List<SchemaTableName> listViews(
            @Bind("schemaName") String schemaName);

    @SqlQuery("SELECT schema_name, table_name, data\n" +
            "FROM views\n" +
            "WHERE (schema_name = :schemaName OR :schemaName IS NULL)\n" +
            "  AND (table_name = :tableName OR :tableName IS NULL)\n" +
            "ORDER BY schema_name, table_name\n")
    @Mapper(ViewResult.Mapper.class)
    List<ViewResult> getViews(
            @Bind("schemaName") String schemaName,
            @Bind("tableName") String tableName);

    @SqlUpdate("INSERT INTO tables (\n" +
            "  schema_name, table_name, compaction_enabled, organization_enabled, distribution_id,\n" +
            "  create_time, update_time, table_version,\n" +
            "  shard_count, row_count, compressed_size, uncompressed_size)\n" +
            "VALUES (\n" +
            "  :schemaName, :tableName, :compactionEnabled, :organizationEnabled, :distributionId,\n" +
            "  :createTime, :createTime, 0,\n" +
            "  0, 0, 0, 0)\n")
    @GetGeneratedKeys
    long insertTable(
            @Bind("schemaName") String schemaName,
            @Bind("tableName") String tableName,
            @Bind("compactionEnabled") boolean compactionEnabled,
            @Bind("organizationEnabled") boolean organizationEnabled,
            @Bind("distributionId") Long distributionId,
            @Bind("createTime") long createTime);

    @SqlUpdate("UPDATE tables SET\n" +
            "  update_time = :updateTime\n" +
            ", table_version = table_version + 1\n" +
            "WHERE table_id = :tableId")
    void updateTableVersion(
            @Bind("tableId") long tableId,
            @Bind("updateTime") long updateTime);

    @SqlUpdate("UPDATE tables SET\n" +
            "  shard_count = shard_count + :shardCount \n" +
            ", row_count = row_count + :rowCount\n" +
            ", compressed_size = compressed_size + :compressedSize\n" +
            ", uncompressed_size = uncompressed_size + :uncompressedSize\n" +
            "WHERE table_id = :tableId")
    void updateTableStats(
            @Bind("tableId") long tableId,
            @Bind("shardCount") long shardCount,
            @Bind("rowCount") long rowCount,
            @Bind("compressedSize") long compressedSize,
            @Bind("uncompressedSize") long uncompressedSize);

    @SqlUpdate("INSERT INTO columns (table_id, column_id, column_name, ordinal_position, data_type, sort_ordinal_position, bucket_ordinal_position)\n" +
            "VALUES (:tableId, :columnId, :columnName, :ordinalPosition, :dataType, :sortOrdinalPosition, :bucketOrdinalPosition)")
    void insertColumn(
            @Bind("tableId") long tableId,
            @Bind("columnId") long columnId,
            @Bind("columnName") String columnName,
            @Bind("ordinalPosition") int ordinalPosition,
            @Bind("dataType") String dataType,
            @Bind("sortOrdinalPosition") Integer sortOrdinalPosition,
            @Bind("bucketOrdinalPosition") Integer bucketOrdinalPosition);

    @SqlUpdate("UPDATE tables SET\n" +
            "  schema_name = :newSchemaName\n" +
            ", table_name = :newTableName\n" +
            "WHERE table_id = :tableId")
    void renameTable(
            @Bind("tableId") long tableId,
            @Bind("newSchemaName") String newSchemaName,
            @Bind("newTableName") String newTableName);

    @SqlUpdate("UPDATE columns SET column_name = :target\n" +
            "WHERE table_id = :tableId\n" +
            "  AND column_id = :columnId")
    void renameColumn(
            @Bind("tableId") long tableId,
            @Bind("columnId") long columnId,
            @Bind("target") String target);

    @SqlUpdate("INSERT INTO views (schema_name, table_name, data)\n" +
            "VALUES (:schemaName, :tableName, :data)")
    void insertView(
            @Bind("schemaName") String schemaName,
            @Bind("tableName") String tableName,
            @Bind("data") String data);

    @SqlUpdate("DELETE FROM tables WHERE table_id = :tableId")
    int dropTable(@Bind("tableId") long tableId);

    @SqlUpdate("DELETE FROM columns WHERE table_id = :tableId")
    int dropColumns(@Bind("tableId") long tableId);

    @SqlUpdate("DELETE FROM views\n" +
            "WHERE schema_name = :schemaName\n" +
            "  AND table_name = :tableName")
    int dropView(
            @Bind("schemaName") String schemaName,
            @Bind("tableName") String tableName);

    // JDBI returns 0 as the column_id when the temporal_column_id is set to NULL
    // jdbi issue https://github.com/jdbi/jdbi/issues/154
    @SqlQuery("SELECT temporal_column_id\n" +
            "FROM tables\n" +
            "WHERE table_id = :tableId\n" +
            "  AND temporal_column_id IS NOT NULL")
    Long getTemporalColumnId(@Bind("tableId") long tableId);

    @SqlUpdate("UPDATE tables SET\n" +
            "temporal_column_id = :columnId\n" +
            "WHERE table_id = :tableId")
    void updateTemporalColumnId(
            @Bind("tableId") long tableId,
            @Bind("columnId") long columnId);

    @SqlQuery("SELECT compaction_enabled FROM tables WHERE table_id = :tableId")
    boolean isCompactionEnabled(@Bind("tableId") long tableId);

    @SqlQuery("SELECT table_id FROM tables WHERE table_id = :tableId FOR UPDATE")
    Long getLockedTableId(@Bind("tableId") long tableId);

    @SqlQuery("SELECT distribution_id, distribution_name, column_types, bucket_count\n" +
            "FROM distributions\n" +
            "WHERE distribution_id = :distributionId")
    Distribution getDistribution(@Bind("distributionId") long distributionId);

    @SqlQuery("SELECT distribution_id, distribution_name, column_types, bucket_count\n" +
            "FROM distributions\n" +
            "WHERE distribution_name = :distributionName")
    Distribution getDistribution(@Bind("distributionName") String distributionName);

    @SqlUpdate("INSERT INTO distributions (distribution_name, column_types, bucket_count)\n" +
            "VALUES (:distributionName, :columnTypes, :bucketCount)")
    @GetGeneratedKeys
    long insertDistribution(
            @Bind("distributionName") String distributionName,
            @Bind("columnTypes") String columnTypes,
            @Bind("bucketCount") int bucketCount);

    @SqlQuery("SELECT table_id, schema_name, table_name, temporal_column_id, distribution_name, bucket_count\n" +
            "FROM tables\n" +
            "LEFT JOIN distributions\n" +
            "ON tables.distribution_id = distributions.distribution_id\n" +
            "WHERE (schema_name = :schemaName OR :schemaName IS NULL)\n" +
            "  AND (table_name = :tableName OR :tableName IS NULL)\n" +
            "ORDER BY table_id")
    @Mapper(TableMetadataRow.Mapper.class)
    List<TableMetadataRow> getTableMetadataRows(
            @Bind("schemaName") String schemaName,
            @Bind("tableName") String tableName);

    @SqlQuery("SELECT table_id, column_id, column_name, sort_ordinal_position, bucket_ordinal_position\n" +
            "FROM columns\n" +
            "WHERE table_id IN (\n" +
            "  SELECT table_id\n" +
            "  FROM tables\n" +
            "  WHERE (schema_name = :schemaName OR :schemaName IS NULL)\n" +
            "    AND (table_name = :tableName OR :tableName IS NULL))\n" +
            "ORDER BY table_id")
    @Mapper(ColumnMetadataRow.Mapper.class)
    List<ColumnMetadataRow> getColumnMetadataRows(
            @Bind("schemaName") String schemaName,
            @Bind("tableName") String tableName);

    @SqlQuery("SELECT schema_name, table_name, create_time, update_time, table_version,\n" +
            "  shard_count, row_count, compressed_size, uncompressed_size\n" +
            "FROM tables\n" +
            "WHERE (schema_name = :schemaName OR :schemaName IS NULL)\n" +
            "  AND (table_name = :tableName OR :tableName IS NULL)\n" +
            "ORDER BY schema_name, table_name")
    @Mapper(TableStatsRow.Mapper.class)
    List<TableStatsRow> getTableStatsRows(
            @Bind("schemaName") String schemaName,
            @Bind("tableName") String tableName);

    @SqlQuery("SELECT table_id\n" +
            "FROM tables\n" +
            "WHERE organization_enabled\n" +
            "  AND table_id IN\n" +
            "       (SELECT table_id\n" +
            "        FROM columns\n" +
            "        WHERE sort_ordinal_position IS NOT NULL)")
    Set<Long> getOrganizationEligibleTables();
}
