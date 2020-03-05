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
import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.verifier.checksum.ChecksumResult;
import com.facebook.presto.verifier.checksum.ChecksumValidator;
import com.facebook.presto.verifier.checksum.ColumnMatchResult;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.facebook.presto.verifier.framework.MatchResult.MatchType.COLUMN_MISMATCH;
import static com.facebook.presto.verifier.framework.MatchResult.MatchType.MATCH;
import static com.facebook.presto.verifier.framework.MatchResult.MatchType.ROW_COUNT_MISMATCH;
import static com.facebook.presto.verifier.framework.MatchResult.MatchType.SCHEMA_MISMATCH;
import static com.facebook.presto.verifier.framework.QueryStage.DESCRIBE;
import static com.facebook.presto.verifier.framework.QueryStage.DETERMINISM_ANALYSIS;
import static com.facebook.presto.verifier.framework.QueryStage.forMain;
import static com.facebook.presto.verifier.framework.QueryStage.forSetup;
import static com.facebook.presto.verifier.framework.QueryStage.forTeardown;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Locale.ENGLISH;

public class DataVerificationUtil
{
    private static final Logger log = Logger.get(DataVerificationUtil.class);

    private DataVerificationUtil() {}

    public static QueryStats setupAndRun(PrestoAction prestoAction, QueryBundle bundle, boolean determinismAnalysis)
    {
        checkState(!determinismAnalysis || bundle.getCluster() == CONTROL, "Determinism analysis can only be run on control cluster");
        QueryStage setupStage = determinismAnalysis ? DETERMINISM_ANALYSIS : forSetup(bundle.getCluster());
        QueryStage mainStage = determinismAnalysis ? DETERMINISM_ANALYSIS : forMain(bundle.getCluster());

        for (Statement setupQuery : bundle.getSetupQueries()) {
            prestoAction.execute(setupQuery, setupStage);
        }
        return prestoAction.execute(bundle.getQuery(), mainStage);
    }

    public static void teardownSafely(PrestoAction prestoAction, Optional<QueryBundle> bundle)
    {
        if (!bundle.isPresent()) {
            return;
        }

        for (Statement teardownQuery : bundle.get().getTeardownQueries()) {
            try {
                prestoAction.execute(teardownQuery, forTeardown(bundle.get().getCluster()));
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

    public static MatchResult match(
            ChecksumValidator checksumValidator,
            List<Column> controlColumns,
            List<Column> testColumns,
            ChecksumResult controlChecksum,
            ChecksumResult testChecksum)
    {
        if (!controlColumns.equals(testColumns)) {
            return new MatchResult(
                    SCHEMA_MISMATCH,
                    Optional.empty(),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    ImmutableMap.of());
        }

        OptionalLong controlRowCount = OptionalLong.of(controlChecksum.getRowCount());
        OptionalLong testRowCount = OptionalLong.of(testChecksum.getRowCount());

        MatchResult.MatchType matchType;
        Map<Column, ColumnMatchResult> mismatchedColumns;
        if (controlChecksum.getRowCount() != testChecksum.getRowCount()) {
            mismatchedColumns = ImmutableMap.of();
            matchType = ROW_COUNT_MISMATCH;
        }
        else {
            mismatchedColumns = checksumValidator.getMismatchedColumns(controlColumns, controlChecksum, testChecksum);
            matchType = mismatchedColumns.isEmpty() ? MATCH : COLUMN_MISMATCH;
        }
        return new MatchResult(
                matchType,
                Optional.of(controlChecksum),
                controlRowCount,
                testRowCount,
                mismatchedColumns);
    }
}
