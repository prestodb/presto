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
import org.jdbi.v3.sqlobject.customizer.BindList;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.statement.UseRowMapper;

import java.util.Collection;
import java.util.Set;

public interface MasterCommitCleanerDao
{
    // dropped tables

    @SqlQuery("SELECT table_id\n" +
            "FROM tables\n" +
            "GROUP BY table_id\n" +
            "HAVING count(end_commit_id) = count(*)\n" +
            "   AND max(end_commit_id) <= :activeCommitId")
    Set<Long> getDroppedTableIds(@Bind long activeCommitId);

    @SqlUpdate("DELETE FROM columns WHERE table_id IN (<tableIds>)")
    void cleanupDroppedTableColumns(@BindList Set<Long> tableIds);

    // dropped columns

    @SqlQuery("SELECT table_id, column_id\n" +
            "FROM columns\n" +
            "GROUP BY table_id, column_id\n" +
            "HAVING count(end_commit_id) = count(*)\n" +
            "   AND max(end_commit_id) <= :activeCommitId")
    @UseRowMapper(TableColumn.Mapper.class)
    Set<TableColumn> getDroppedColumns(@Bind long activeCommitId);

    // transactions

    @SqlQuery("SELECT transaction_id FROM transactions WHERE successful")
    Set<Long> getSuccessfulTransactionIds();

    @SqlUpdate("DELETE FROM transactions WHERE transaction_id IN (<transactionIds>)")
    void cleanupTransactions(@BindList Iterable<Long> transactionIds);

    @SqlQuery("SELECT transaction_id\n" +
            "FROM transactions\n" +
            "WHERE NOT successful\n" +
            "  AND end_time <= :maxEndTime")
    Set<Long> getFailedTransactions(@Bind long maxEndTime);

    // abort transactions

    default void abortTransactions(Collection<Long> excludedTransactionIds, long endTime)
    {
        if (excludedTransactionIds.isEmpty()) {
            doAbortTransactions(endTime);
        }
        else {
            doAbortTransactions(excludedTransactionIds, endTime);
        }
    }

    @SqlUpdate("UPDATE transactions SET successful = FALSE, end_time = :endTime\n" +
            "WHERE successful IS NULL")
    void doAbortTransactions(@Bind long endTime);

    @SqlUpdate("UPDATE transactions SET successful = FALSE, end_time = :endTime\n" +
            "WHERE successful IS NULL\n" +
            "  AND transaction_id NOT IN (<excludedTransactionIds>)")
    void doAbortTransactions(
            @BindList Iterable<Long> excludedTransactionIds,
            @Bind long endTime);

    // commit scoped data

    default void cleanup(long activeCommitId)
    {
        cleanupSchemas(activeCommitId);
        cleanupTables(activeCommitId);
        cleanupColumns(activeCommitId);
        cleanupViews(activeCommitId);
        cleanupCommits(activeCommitId);
    }

    @SqlUpdate("DELETE FROM schemata WHERE end_commit_id <= :activeCommitId")
    void cleanupSchemas(@Bind long activeCommitId);

    @SqlUpdate("DELETE FROM tables WHERE end_commit_id <= :activeCommitId")
    void cleanupTables(@Bind long activeCommitId);

    @SqlUpdate("DELETE FROM columns WHERE end_commit_id <= :activeCommitId")
    void cleanupColumns(@Bind long activeCommitId);

    @SqlUpdate("DELETE FROM views WHERE end_commit_id <= :activeCommitId")
    void cleanupViews(@Bind long activeCommitId);

    @SqlUpdate("DELETE FROM commits WHERE commit_id < :activeCommitId")
    void cleanupCommits(@Bind long activeCommitId);
}
