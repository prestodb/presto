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

import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.statement.UseRowMapper;

import java.util.List;
import java.util.Set;

public interface MasterWriterDao
{
    // transaction

    @SqlQuery("SELECT commit_id FROM current_commit FOR UPDATE")
    Long getLockedCurrentCommitId();

    @SqlUpdate("INSERT INTO active_commit (singleton, commit_id, start_time, rolling_back)\n" +
            "VALUES ('X', :commitId, :startTime, FALSE)")
    void insertActiveCommit(
            @Bind long commitId,
            @Bind long startTime);

    @SqlQuery("SELECT * FROM active_commit")
    @UseRowMapper(ActiveCommit.Mapper.class)
    ActiveCommit getActiveCommit();

    @SqlUpdate("DELETE FROM active_commit")
    int deleteActiveCommit();

    @SqlUpdate("UPDATE active_commit SET rolling_back = TRUE")
    void abortActiveCommit();

    @SqlUpdate("UPDATE active_commit SET rollback_info = :rollbackInfo")
    void updateRollbackInfo(
            @Bind byte[] rollbackInfo);

    @SqlUpdate("INSERT INTO commits (commit_id, commit_time)\n" +
            "VALUES (:commitId, :commitTime)")
    void insertCommit(
            @Bind long commitId,
            @Bind long commitTime);

    @SqlUpdate("UPDATE current_commit SET commit_id = :commitId")
    int updateCurrentCommit(
            @Bind long commitId);

    // The below 2 is for testing purpose
    @SqlQuery("SELECT transaction_id FROM transactions WHERE successful")
    Set<Long> getSuccessfulTransactionIds();

    @SqlQuery("SELECT transaction_id FROM transactions WHERE successful is NULL or NOT successful")
    Set<Long> getNotSuccessfulTransactionIds();

    @SqlUpdate("UPDATE transactions SET successful = TRUE, end_time = :endTime\n" +
            "WHERE successful IS NULL\n" +
            "  AND transaction_id = :transactionId")
    int finalizeTransaction(
            @Bind long transactionId,
            @Bind long endTime);

    default void rollback(long commitId)
    {
        rollbackCreatedSchemas(commitId);
        rollbackDeletedSchemas(commitId);

        rollbackCreatedTables(commitId);
        rollbackDeletedTables(commitId);

        rollbackCreatedColumns(commitId);
        rollbackDeletedColumns(commitId);

        rollbackCreatedViews(commitId);
        rollbackDeletedViews(commitId);
    }

    // schema

    @SqlUpdate("INSERT INTO schemata (start_commit_id, schema_id, schema_name)\n" +
            "VALUES (:commitId, :schemaId, :schemaName)")
    void insertSchema(
            @Bind long commitId,
            @Bind long schemaId,
            @Bind byte[] schemaName);

    @SqlUpdate("UPDATE schemata SET end_commit_id = :commitId\n" +
            "WHERE end_commit_id IS NULL \n" +
            "  AND schema_id = :schemaId")
    int deleteSchema(
            @Bind long commitId,
            @Bind long schemaId);

    @SqlUpdate("DELETE FROM schemata WHERE start_commit_id = :commitId")
    void rollbackCreatedSchemas(
            @Bind long commitId);

    @SqlUpdate("UPDATE schemata SET end_commit_id = NULL\n" +
            "WHERE end_commit_id = :commitId")
    void rollbackDeletedSchemas(
            @Bind long commitId);

    @SqlQuery("SELECT count(*)\n" +
            "FROM schemata\n" +
            "WHERE end_commit_id IS NULL\n" +
            "  AND schema_id = :schemaId")
    boolean schemaIdExists(
            @Bind long schemaId);

    @SqlQuery("SELECT count(*)\n" +
            "FROM schemata\n" +
            "WHERE end_commit_id IS NULL\n" +
            "  AND schema_name = :schemaName")
    boolean schemaNameExists(
            @Bind byte[] schemaName);

    // distribution

    @SqlQuery("SELECT count(*)\n" +
            "FROM distributions\n" +
            "WHERE distribution_id = :distributionId")
    boolean distributionIdExists(
            @Bind long distributionId);

    // table

    @SqlUpdate("INSERT INTO tables (\n" +
            "  start_commit_id,\n" +
            "  table_id,\n" +
            "  table_name,\n" +
            "  schema_id,\n" +
            "  distribution_id,\n" +
            "  temporal_column_id,\n" +
            "  organization_enabled,\n" +
            "  compression_type,\n" +
            "  create_time,\n" +
            "  update_time,\n" +
            "  row_count,\n" +
            "  comment)\n" +
            "VALUES (\n" +
            "  :commitId,\n" +
            "  :tableId,\n" +
            "  :tableName,\n" +
            "  :schemaId,\n" +
            "  :distributionId,\n" +
            "  :temporalColumnId,\n" +
            "  :organized,\n" +
            "  :compressionType,\n" +
            "  :createTime,\n" +
            "  :updateTime,\n" +
            "  :rowCount,\n" +
            "  :comment)\n")
    void insertTable(
            @Bind long commitId,
            @Bind long tableId,
            @Bind byte[] tableName,
            @Bind long schemaId,
            @Bind long distributionId,
            @Bind Long temporalColumnId,
            @Bind boolean organized,
            @Bind String compressionType,
            @Bind long createTime,
            @Bind long updateTime,
            @Bind long rowCount,
            @Bind byte[] comment);

    @SqlUpdate("UPDATE tables SET end_commit_id = :commitId\n" +
            "WHERE end_commit_id IS NULL\n" +
            "  AND table_id = :tableId")
    int deleteTable(
            @Bind long commitId,
            @Bind long tableId);

    @SqlUpdate("DELETE FROM chunk_organizer_jobs WHERE table_id = :tableId")
    void dropOrganizerJobs(@Bind("tableId") long tableId);

    @SqlUpdate("DELETE FROM tables WHERE start_commit_id = :commitId")
    void rollbackCreatedTables(
            @Bind long commitId);

    @SqlUpdate("UPDATE tables SET end_commit_id = NULL\n" +
            "WHERE end_commit_id = :commitId")
    void rollbackDeletedTables(
            @Bind long commitId);

    @SqlQuery("SELECT count(*)\n" +
            "FROM tables\n" +
            "WHERE end_commit_id IS NULL\n" +
            "  AND table_id = :tableId")
    boolean tableIdExists(
            @Bind long tableId);

    @SqlQuery("SELECT count(*)\n" +
            "FROM tables\n" +
            "WHERE end_commit_id IS NULL\n" +
            "  AND schema_id = :schemaId\n" +
            "  AND table_name = :tableName")
    boolean tableNameExists(
            @Bind long schemaId,
            @Bind byte[] tableName);

    @SqlQuery("SELECT *\n" +
            "FROM tables\n" +
            "WHERE end_commit_id IS NULL\n" +
            "  AND table_id = :tableId\n")
    @UseRowMapper(TableInfo.Mapper.class)
    TableInfo getTableInfo(
            @Bind long tableId);

    // column

    @SqlUpdate("INSERT INTO columns (\n" +
            "  start_commit_id,\n" +
            "  column_id,\n" +
            "  column_name,\n" +
            "  data_type,\n" +
            "  table_id,\n" +
            "  ordinal_position,\n" +
            "  bucket_ordinal_position,\n" +
            "  sort_ordinal_position,\n" +
            "  comment)\n" +
            "VALUES (\n" +
            "  :commitId,\n" +
            "  :columnId,\n" +
            "  :columnName,\n" +
            "  :dataType,\n" +
            "  :tableId,\n" +
            "  :ordinalPosition,\n" +
            "  :bucketOrdinalPosition,\n" +
            "  :sortOrdinalPosition,\n" +
            "  :comment)")
    void insertColumn(
            @Bind long commitId,
            @Bind long columnId,
            @Bind byte[] columnName,
            @Bind byte[] dataType,
            @Bind long tableId,
            @Bind long ordinalPosition,
            @Bind Integer bucketOrdinalPosition,
            @Bind Integer sortOrdinalPosition,
            @Bind byte[] comment);

    @SqlUpdate("UPDATE columns SET end_commit_id = :commitId\n" +
            "WHERE end_commit_id IS NULL \n" +
            "  AND table_id = :tableId\n" +
            "  AND column_id = :columnId")
    int deleteColumn(
            @Bind long commitId,
            @Bind long tableId,
            @Bind long columnId);

    @SqlUpdate("DELETE FROM columns WHERE start_commit_id = :commitId")
    void rollbackCreatedColumns(
            @Bind long commitId);

    @SqlUpdate("UPDATE columns SET end_commit_id = NULL\n" +
            "WHERE end_commit_id = :commitId")
    void rollbackDeletedColumns(
            @Bind long commitId);

    @SqlQuery("SELECT *\n" +
            "FROM columns\n" +
            "WHERE end_commit_id IS NULL\n" +
            "  AND table_id = :tableId")
    List<ColumnInfo> getColumnInfo(
            @Bind long tableId);

    // view

    @SqlUpdate("INSERT INTO views (\n" +
            "  start_commit_id,\n" +
            "  view_id,\n" +
            "  view_name,\n" +
            "  schema_id,\n" +
            "  create_time,\n" +
            "  update_time,\n" +
            "  view_data,\n" +
            "  comment)\n" +
            "VALUES (\n" +
            "  :commitId,\n" +
            "  :tableId,\n" +
            "  :tableName,\n" +
            "  :schemaId,\n" +
            "  :createTime,\n" +
            "  :updateTime,\n" +
            "  :viewData,\n" +
            "  :comment)\n")
    void insertView(
            @Bind long commitId,
            @Bind long tableId,
            @Bind byte[] tableName,
            @Bind long schemaId,
            @Bind long createTime,
            @Bind long updateTime,
            @Bind byte[] viewData,
            @Bind byte[] comment);

    @SqlUpdate("UPDATE views SET end_commit_id = :commitId\n" +
            "WHERE end_commit_id IS NULL \n" +
            "  AND view_id = :viewId")
    int deleteView(
            @Bind long commitId,
            @Bind long viewId);

    @SqlUpdate("DELETE FROM views WHERE start_commit_id = :commitId")
    void rollbackCreatedViews(
            @Bind long commitId);

    @SqlUpdate("UPDATE views SET end_commit_id = NULL\n" +
            "WHERE end_commit_id = :commitId")
    void rollbackDeletedViews(
            @Bind long commitId);

    @SqlQuery("SELECT count(*)\n" +
            "FROM views\n" +
            "WHERE end_commit_id IS NULL\n" +
            "  AND view_id = :viewId")
    boolean viewIdExists(
            @Bind long viewId);

    @SqlQuery("SELECT count(*)\n" +
            "FROM views\n" +
            "WHERE end_commit_id IS NULL\n" +
            "  AND schema_id = :schemaId\n" +
            "  AND view_name = :viewName")
    boolean viewNameExists(
            @Bind long schemaId,
            @Bind byte[] viewName);
}
