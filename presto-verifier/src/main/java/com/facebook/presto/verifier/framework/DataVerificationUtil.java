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
package com.facebook.presto.verifier.framework;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.verifier.checksum.ChecksumResult;
import com.facebook.presto.verifier.checksum.ChecksumValidator;
import com.facebook.presto.verifier.checksum.ColumnMatchResult;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.facebook.presto.verifier.prestoaction.QueryAction;
import com.facebook.presto.verifier.prestoaction.QueryActionStats;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;

import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.verifier.framework.DataMatchResult.MatchType.COLUMN_MISMATCH;
import static com.facebook.presto.verifier.framework.DataMatchResult.MatchType.MATCH;
import static com.facebook.presto.verifier.framework.DataMatchResult.MatchType.ROW_COUNT_MISMATCH;
import static com.facebook.presto.verifier.framework.DataMatchResult.MatchType.SCHEMA_MISMATCH;
import static com.facebook.presto.verifier.framework.QueryStage.DESCRIBE;
import static com.facebook.presto.verifier.framework.QueryStage.forTeardown;
import static com.facebook.presto.verifier.framework.VerifierUtil.runAndConsume;
import static java.util.Locale.ENGLISH;

public class DataVerificationUtil
{
    private static final Logger log = Logger.get(DataVerificationUtil.class);

    private DataVerificationUtil() {}

    public static void teardownSafely(QueryAction queryAction, Optional<? extends QueryBundle> bundle, Consumer<QueryActionStats> queryStatsConsumer)
    {
        if (!bundle.isPresent()) {
            return;
        }

        for (Statement teardownQuery : bundle.get().getTeardownQueries()) {
            try {
                runAndConsume(() -> queryAction.execute(teardownQuery, forTeardown(bundle.get().getCluster())), queryStatsConsumer);
            }
            catch (Throwable t) {
                log.warn("Failed to teardown %s: %s", bundle.get().getCluster().name().toLowerCase(ENGLISH), formatSql(teardownQuery, Optional.empty()));
            }
        }
    }

    public static List<Column> getColumns(PrestoAction prestoAction, TypeManager typeManager, QualifiedName tableName)
    {
        return prestoAction
                .execute(new ShowColumns(tableName), DESCRIBE, resultSet -> Optional.of(Column.fromResultSet(typeManager, resultSet)))
                .getResults();
    }

    public static DataMatchResult match(
            ChecksumValidator checksumValidator,
            List<Column> controlColumns,
            List<Column> testColumns,
            ChecksumResult controlChecksum,
            ChecksumResult testChecksum)
    {
        if (!controlColumns.equals(testColumns)) {
            return new DataMatchResult(
                    SCHEMA_MISMATCH,
                    Optional.empty(),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    ImmutableList.of());
        }

        OptionalLong controlRowCount = OptionalLong.of(controlChecksum.getRowCount());
        OptionalLong testRowCount = OptionalLong.of(testChecksum.getRowCount());

        DataMatchResult.MatchType matchType;
        List<ColumnMatchResult<?>> mismatchedColumns;
        if (controlChecksum.getRowCount() != testChecksum.getRowCount()) {
            mismatchedColumns = ImmutableList.of();
            matchType = ROW_COUNT_MISMATCH;
        }
        else {
            mismatchedColumns = checksumValidator.getMismatchedColumns(controlColumns, controlChecksum, testChecksum);
            matchType = mismatchedColumns.isEmpty() ? MATCH : COLUMN_MISMATCH;
        }
        return new DataMatchResult(
                matchType,
                Optional.of(controlChecksum),
                controlRowCount,
                testRowCount,
                mismatchedColumns);
    }
}
