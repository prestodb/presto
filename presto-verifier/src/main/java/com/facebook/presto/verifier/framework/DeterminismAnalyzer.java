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

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.verifier.checksum.ChecksumResult;
import com.facebook.presto.verifier.checksum.ChecksumValidator;
import com.facebook.presto.verifier.event.DeterminismAnalysisDetails;
import com.facebook.presto.verifier.event.DeterminismAnalysisRun;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.facebook.presto.verifier.rewrite.QueryRewriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.facebook.presto.verifier.framework.DataVerificationUtil.getColumns;
import static com.facebook.presto.verifier.framework.DataVerificationUtil.match;
import static com.facebook.presto.verifier.framework.DataVerificationUtil.teardownSafely;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.ANALYSIS_FAILED_DATA_CHANGED;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.ANALYSIS_FAILED_INCONSISTENT_SCHEMA;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.ANALYSIS_FAILED_QUERY_FAILURE;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.DETERMINISTIC;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.NON_DETERMINISTIC_CATALOG;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.NON_DETERMINISTIC_COLUMNS;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.NON_DETERMINISTIC_LIMIT_CLAUSE;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.NON_DETERMINISTIC_ROW_COUNT;
import static com.facebook.presto.verifier.framework.QueryStage.DETERMINISM_ANALYSIS_CHECKSUM;
import static com.facebook.presto.verifier.framework.QueryStage.DETERMINISM_ANALYSIS_MAIN;
import static com.facebook.presto.verifier.framework.QueryStage.DETERMINISM_ANALYSIS_SETUP;
import static com.facebook.presto.verifier.framework.VerifierUtil.callAndConsume;
import static com.facebook.presto.verifier.framework.VerifierUtil.runAndConsume;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DeterminismAnalyzer
{
    private final SourceQuery sourceQuery;
    private final PrestoAction prestoAction;
    private final QueryRewriter queryRewriter;
    private final ChecksumValidator checksumValidator;
    private final TypeManager typeManager;

    private final boolean runTeardown;
    private final int maxAnalysisRuns;
    private final Set<String> nonDeterministicCatalogs;
    private final boolean handleLimitQuery;

    public DeterminismAnalyzer(
            SourceQuery sourceQuery,
            PrestoAction prestoAction,
            QueryRewriter queryRewriter,
            ChecksumValidator checksumValidator,
            TypeManager typeManager,
            DeterminismAnalyzerConfig config)
    {
        this.sourceQuery = requireNonNull(sourceQuery, "sourceQuery is null");
        this.prestoAction = requireNonNull(prestoAction, "prestoAction is null");
        this.queryRewriter = requireNonNull(queryRewriter, "queryRewriter is null");
        this.checksumValidator = requireNonNull(checksumValidator, "checksumValidator is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");

        this.runTeardown = config.isRunTeardown();
        this.maxAnalysisRuns = config.getMaxAnalysisRuns();
        this.nonDeterministicCatalogs = ImmutableSet.copyOf(config.getNonDeterministicCatalogs());
        this.handleLimitQuery = config.isHandleLimitQuery();
    }

    protected DeterminismAnalysisDetails analyze(QueryObjectBundle control, ChecksumResult controlChecksum)
    {
        DeterminismAnalysisDetails.Builder determinismAnalysisDetails = DeterminismAnalysisDetails.builder();
        DeterminismAnalysis analysis = analyze(control, controlChecksum, determinismAnalysisDetails);
        return determinismAnalysisDetails.build(analysis);
    }

    private DeterminismAnalysis analyze(QueryObjectBundle control, ChecksumResult controlChecksum, DeterminismAnalysisDetails.Builder determinismAnalysisDetails)
    {
        // Handle mutable catalogs
        if (isNonDeterministicCatalogReferenced(control.getQuery())) {
            return NON_DETERMINISTIC_CATALOG;
        }

        // Handle limit query
        LimitQueryDeterminismAnalysis limitQueryAnalysis = new LimitQueryDeterminismAnalyzer(
                prestoAction,
                handleLimitQuery,
                control.getQuery(),
                controlChecksum.getRowCount(),
                determinismAnalysisDetails).analyze();

        switch (limitQueryAnalysis) {
            case NOT_RUN:
            case FAILED_QUERY_FAILURE:
            case DETERMINISTIC:
                // try the next analysis
                break;
            case NON_DETERMINISTIC:
                return NON_DETERMINISTIC_LIMIT_CLAUSE;
            case FAILED_DATA_CHANGED:
                return ANALYSIS_FAILED_DATA_CHANGED;
            default:
                throw new IllegalArgumentException(format("Invalid limitQueryAnalysis: %s", limitQueryAnalysis));
        }

        // Rerun control query multiple times
        List<Column> columns = getColumns(prestoAction, typeManager, control.getObjectName());
        Map<QueryBundle, DeterminismAnalysisRun.Builder> queryRuns = new HashMap<>();
        try {
            for (int i = 0; i < maxAnalysisRuns; i++) {
                QueryObjectBundle queryBundle = queryRewriter.rewriteQuery(sourceQuery.getQuery(CONTROL), CONTROL);
                DeterminismAnalysisRun.Builder run = determinismAnalysisDetails.addRun().setTableName(queryBundle.getObjectName().toString());
                queryRuns.put(queryBundle, run);

                // Rerun setup and main query
                queryBundle.getSetupQueries().forEach(query -> runAndConsume(
                        () -> prestoAction.execute(query, DETERMINISM_ANALYSIS_SETUP),
                        stats -> stats.getQueryStats().map(QueryStats::getQueryId).ifPresent(run::addSetupQueryId)));
                runAndConsume(
                        () -> prestoAction.execute(queryBundle.getQuery(), DETERMINISM_ANALYSIS_MAIN),
                        stats -> stats.getQueryStats().map(QueryStats::getQueryId).ifPresent(run::setQueryId));

                // Run checksum query
                Query checksumQuery = checksumValidator.generateChecksumQuery(queryBundle.getObjectName(), columns);
                ChecksumResult testChecksum = getOnlyElement(callAndConsume(
                        () -> prestoAction.execute(checksumQuery, DETERMINISM_ANALYSIS_CHECKSUM, ChecksumResult::fromResultSet),
                        stats -> stats.getQueryStats().map(QueryStats::getQueryId).ifPresent(run::setChecksumQueryId)).getResults());

                DeterminismAnalysis analysis = matchResultToDeterminism(match(checksumValidator, columns, columns, controlChecksum, testChecksum));
                if (analysis != DETERMINISTIC) {
                    return analysis;
                }
            }

            return DETERMINISTIC;
        }
        catch (QueryException qe) {
            return ANALYSIS_FAILED_QUERY_FAILURE;
        }
        finally {
            if (runTeardown) {
                queryRuns.forEach((queryBundle, run) -> teardownSafely(
                        prestoAction,
                        Optional.of(queryBundle),
                        queryStats -> queryStats.getQueryStats().map(QueryStats::getQueryId).ifPresent(run::addTeardownQueryId)));
            }
        }
    }

    private DeterminismAnalysis matchResultToDeterminism(DataMatchResult matchResult)
    {
        switch (matchResult.getMatchType()) {
            case MATCH:
                return DETERMINISTIC;
            case SCHEMA_MISMATCH:
                return ANALYSIS_FAILED_INCONSISTENT_SCHEMA;
            case ROW_COUNT_MISMATCH:
                return NON_DETERMINISTIC_ROW_COUNT;
            case COLUMN_MISMATCH:
                return NON_DETERMINISTIC_COLUMNS;
            default:
                throw new IllegalArgumentException(format("Invalid MatchResult: %s", matchResult));
        }
    }

    @VisibleForTesting
    boolean isNonDeterministicCatalogReferenced(Statement statement)
    {
        if (nonDeterministicCatalogs.isEmpty()) {
            return false;
        }

        AtomicBoolean nonDeterministicCatalogReferenced = new AtomicBoolean();
        new NonDeterministicCatalogVisitor().process(statement, nonDeterministicCatalogReferenced);
        return nonDeterministicCatalogReferenced.get();
    }

    private class NonDeterministicCatalogVisitor
            extends AstVisitor<Void, AtomicBoolean>
    {
        protected Void visitNode(Node node, AtomicBoolean context)
        {
            node.getChildren().forEach(child -> process(child, context));
            return null;
        }

        protected Void visitTable(Table node, AtomicBoolean nonDeterministicCatalogReferenced)
        {
            if (node.getName().getParts().size() == 3 && nonDeterministicCatalogs.contains(node.getName().getParts().get(0))) {
                nonDeterministicCatalogReferenced.set(true);
            }
            return null;
        }
    }
}
