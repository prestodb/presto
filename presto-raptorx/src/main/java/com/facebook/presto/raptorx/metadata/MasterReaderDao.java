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
package com.facebook.presto.raptorx.metadata;

import org.jdbi.v3.sqlobject.config.KeyColumn;
import org.jdbi.v3.sqlobject.config.ValueColumn;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.UseRowMapper;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface MasterReaderDao
{
    @SqlQuery("SELECT commit_id FROM current_commit")
    long getCurrentCommitId();

    @SqlQuery("SELECT schema_id\n" +
            "FROM schemata\n" +
            "WHERE start_commit_id <= :commitId\n" +
            "  AND (end_commit_id > :commitId OR end_commit_id IS NULL)\n" +
            "  AND schema_name = :schemaName")
    Long getSchemaId(
            @Bind long commitId,
            @Bind byte[] schemaName);

    @SqlQuery("SELECT *\n" +
            "FROM schemata\n" +
            "WHERE start_commit_id <= :commitId\n" +
            "  AND (end_commit_id > :commitId OR end_commit_id IS NULL)\n" +
            "  AND schema_id = :schemaId")
    @UseRowMapper(SchemaInfo.Mapper.class)
    SchemaInfo getSchemaInfo(
            @Bind long commitId,
            @Bind long schemaId);

    @SqlQuery("SELECT schema_id, schema_name\n" +
            "FROM schemata\n" +
            "WHERE start_commit_id <= :commitId\n" +
            "  AND (end_commit_id > :commitId OR end_commit_id IS NULL)")
    @KeyColumn("schema_id")
    @ValueColumn("schema_name")
    Map<Long, byte[]> listSchemas(
            @Bind long commitId);

    @SqlQuery("SELECT table_id\n" +
            "FROM tables\n" +
            "WHERE start_commit_id <= :commitId\n" +
            "  AND (end_commit_id > :commitId OR end_commit_id IS NULL)\n" +
            "  AND schema_id = :schemaId\n" +
            "  AND table_name = :tableName")
    Long getTableId(
            @Bind long commitId,
            @Bind long schemaId,
            @Bind byte[] tableName);

    @SqlQuery("SELECT view_id\n" +
            "FROM views\n" +
            "WHERE start_commit_id <= :commitId\n" +
            "  AND (end_commit_id > :commitId OR end_commit_id IS NULL)\n" +
            "  AND schema_id = :schemaId\n" +
            "  AND view_name = :viewName")
    Long getViewId(
            @Bind long commitId,
            @Bind long schemaId,
            @Bind byte[] viewName);

    @SqlQuery("SELECT *\n" +
            "FROM tables\n" +
            "WHERE (end_commit_id IS NULL)\n" +
            "  AND table_id = :tableId")
    @UseRowMapper(TableInfo.Mapper.class)
    TableInfo getTableInfo(@Bind long tableId);

    @SqlQuery("SELECT *\n" +
            "FROM tables\n" +
            "WHERE start_commit_id <= :commitId\n" +
            "  AND (end_commit_id > :commitId OR end_commit_id IS NULL)\n" +
            "  AND table_id = :tableId")
    @UseRowMapper(TableInfo.Mapper.class)
    TableInfo getTableInfo(
            @Bind long commitId,
            @Bind long tableId);

    @SqlQuery("SELECT table_id\n" +
            "FROM tables\n" +
            "WHERE organization_enabled\n" +
            "  AND temporal_column_id is NULL\n" +
            "  AND end_commit_id IS NULL\n" +
            "  AND table_id IN\n" +
            "       (SELECT table_id\n" +
            "        FROM columns\n" +
            "        WHERE sort_ordinal_position IS NOT NULL AND end_commit_id is NULL)")
    Set<Long> getOrganizationEligibleTables();

    @SqlQuery("SELECT *\n" +
            "FROM columns\n" +
            "WHERE (end_commit_id IS NULL)\n" +
            "  AND table_id = :tableId")
    List<ColumnInfo> getColumnInfo(@Bind long tableId);

    @SqlQuery("SELECT *\n" +
            "FROM columns\n" +
            "WHERE start_commit_id <= :commitId\n" +
            "  AND (end_commit_id > :commitId OR end_commit_id IS NULL)\n" +
            "  AND table_id = :tableId")
    List<ColumnInfo> getColumnInfo(
            @Bind long commitId,
            @Bind long tableId);

    @SqlQuery("SELECT *\n" +
            "FROM views\n" +
            "WHERE start_commit_id <= :commitId\n" +
            "  AND (end_commit_id > :commitId OR end_commit_id IS NULL)\n" +
            "  AND view_id = :viewId")
    @UseRowMapper(ViewInfo.Mapper.class)
    ViewInfo getViewInfo(
            @Bind long commitId,
            @Bind long viewId);

    @SqlQuery("SELECT table_name\n" +
            "FROM tables\n" +
            "WHERE start_commit_id <= :commitId\n" +
            "  AND (end_commit_id > :commitId OR end_commit_id IS NULL)\n" +
            "  AND schema_id = :schemaId")
    Collection<byte[]> listTableNames(
            @Bind long commitId,
            @Bind long schemaId);

    @SqlQuery("SELECT view_name\n" +
            "FROM views\n" +
            "WHERE start_commit_id <= :commitId\n" +
            "  AND (end_commit_id > :commitId OR end_commit_id IS NULL)\n" +
            "  AND schema_id = :schemaId")
    Collection<byte[]> listViewNames(
            @Bind long commitId,
            @Bind long schemaId);

    @SqlQuery("SELECT\n" +
            "  table_id,\n" +
            "  table_name,\n" +
            "  schema_id,\n" +
            "  create_time,\n" +
            "  update_time,\n" +
            "  start_commit_id AS table_version,\n" +
            "  row_count\n" +
            "FROM tables\n" +
            "WHERE start_commit_id <= :commitId\n" +
            "  AND (end_commit_id > :commitId OR end_commit_id IS NULL)\n" +
            "  AND (schema_id = :schemaId OR :schemaId IS NULL)\n" +
            "  AND (table_id = :tableId OR :tableId IS NULL)")
    @UseRowMapper(TableSummary.Mapper.class)
    Collection<TableSummary> listTableSummaries(
            @Bind long commitId,
            @Bind Optional<Long> schemaId,
            @Bind Optional<Long> tableId);
}
