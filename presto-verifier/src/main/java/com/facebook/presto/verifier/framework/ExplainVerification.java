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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.Serialization;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.planPrinter.JsonRenderer.JsonRenderedNode;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.ExplainFormat;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.verifier.event.DeterminismAnalysisDetails;
import com.facebook.presto.verifier.event.QueryInfo;
import com.facebook.presto.verifier.prestoaction.PrestoAction.ResultSetConverter;
import com.facebook.presto.verifier.prestoaction.QueryActions;
import com.facebook.presto.verifier.prestoaction.SqlExceptionClassifier;
import com.facebook.presto.verifier.source.SnapshotQueryConsumer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.sql.tree.ExplainFormat.Type.JSON;
import static com.facebook.presto.testing.TestingEnvironment.FUNCTION_AND_TYPE_MANAGER;
import static com.facebook.presto.verifier.framework.ExplainMatchResult.MatchType;
import static com.facebook.presto.verifier.framework.ExplainMatchResult.MatchType.DETAILS_MISMATCH;
import static com.facebook.presto.verifier.framework.ExplainMatchResult.MatchType.MATCH;
import static com.facebook.presto.verifier.framework.ExplainMatchResult.MatchType.SNAPSHOT_DOES_NOT_EXIST;
import static com.facebook.presto.verifier.framework.ExplainMatchResult.MatchType.STRUCTURE_MISMATCH;
import static com.facebook.presto.verifier.framework.VerifierUtil.PARSING_OPTIONS;
import static com.facebook.presto.verifier.source.AbstractJdbiSnapshotQuerySupplier.VERIFIER_SNAPSHOT_KEY_PATTERN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ExplainVerification
        extends AbstractVerification<QueryBundle, ExplainMatchResult, String>
{
    private static final ResultSetConverter<String> QUERY_PLAN_RESULT_SET_CONVERTER = resultSet -> Optional.of(resultSet.getString("Query Plan"));
    private static JsonCodec<JsonRenderedNode> planCodec;
    private final SqlParser sqlParser;

    public ExplainVerification(
            QueryActions queryActions,
            SourceQuery sourceQuery,
            SqlExceptionClassifier exceptionClassifier,
            VerificationContext verificationContext,
            VerifierConfig verifierConfig,
            SqlParser sqlParser,
            ListeningExecutorService executor,
            SnapshotQueryConsumer snapshotQueryConsumer,
            Map<String, SnapshotQuery> snapshotQueries)
    {
        super(queryActions, sourceQuery, exceptionClassifier, verificationContext, Optional.of(QUERY_PLAN_RESULT_SET_CONVERTER), verifierConfig, executor, snapshotQueryConsumer, snapshotQueries);
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        JsonObjectMapperProvider provider = new JsonObjectMapperProvider();
        provider.setJsonSerializers(ImmutableMap.of(VariableReferenceExpression.class, new Serialization.VariableReferenceExpressionSerializer()));
        provider.setKeyDeserializers(ImmutableMap.of(VariableReferenceExpression.class, new Serialization.VariableReferenceExpressionDeserializer(FUNCTION_AND_TYPE_MANAGER)));
        JsonCodecFactory codecFactory = new JsonCodecFactory(provider, true);
        planCodec = codecFactory.jsonCodec(JsonRenderedNode.class);
    }

    @Override
    protected QueryBundle getQueryRewrite(ClusterType clusterType)
    {
        Statement statement = sqlParser.createStatement(getSourceQuery().getQuery(clusterType), PARSING_OPTIONS);
        Explain explain = new Explain(statement, false, false, ImmutableList.of(new ExplainFormat(JSON)));
        return new QueryBundle(ImmutableList.of(), explain, ImmutableList.of(), clusterType);
    }

    @Override
    protected ExplainMatchResult verify(
            QueryBundle control,
            QueryBundle test,
            Optional<QueryResult<String>> controlQueryResult,
            Optional<QueryResult<String>> testQueryResult,
            ChecksumQueryContext controlChecksumQueryContext,
            ChecksumQueryContext testChecksumQueryContext)
    {
        checkArgument(testQueryResult.isPresent(), "test query plan is missing");

        JsonRenderedNode controlPlan;
        if (isControlEnabled()) {
            checkArgument(controlQueryResult.isPresent(), "control query plan is missing");
            String result = getOnlyElement(controlQueryResult.get().getResults());
            if (saveSnapshot) {
                snapshotQueryConsumer.accept(new SnapshotQuery(getSourceQuery().getSuite(), getSourceQuery().getName(), isExplain, result));
                return new ExplainMatchResult(MATCH);
            }
            controlPlan = planCodec.fromJson(result);
        }
        else {
            String key = format(VERIFIER_SNAPSHOT_KEY_PATTERN, getSourceQuery().getSuite(), getSourceQuery().getName(), isExplain);
            SnapshotQuery snapshotQuery = snapshotQueries.get(key);
            if (snapshotQuery == null) {
                return new ExplainMatchResult(SNAPSHOT_DOES_NOT_EXIST);
            }
            String snapshotJson = snapshotQuery.getSnapshot();
            controlPlan = planCodec.fromJson(snapshotJson);
        }
        JsonRenderedNode testPlan = planCodec.fromJson(getOnlyElement(testQueryResult.get().getResults()));

        return new ExplainMatchResult(match(controlPlan, testPlan));
    }

    @Override
    protected DeterminismAnalysisDetails analyzeDeterminism(QueryBundle control, ExplainMatchResult matchResult)
    {
        throw new UnsupportedOperationException("analyzeDeterminism is not supported for ExplainVerification");
    }

    @Override
    protected Optional<String> resolveFailure(Optional<QueryBundle> control, Optional<QueryBundle> test, QueryContext controlQueryContext, Optional<ExplainMatchResult> matchResult, Optional<Throwable> throwable)
    {
        return Optional.empty();
    }

    @Override
    protected void updateQueryInfo(QueryInfo.Builder queryInfo, Optional<QueryResult<String>> queryResult)
    {
        queryResult.ifPresent(result -> queryInfo.setJsonPlan(getOnlyElement(result.getResults())));
    }

    private MatchType match(JsonRenderedNode controlPlan, JsonRenderedNode testPlan)
    {
        if (!Objects.equals(controlPlan.getName(), testPlan.getName())
                || !Objects.equals(controlPlan.getIdentifier(), testPlan.getIdentifier())
                || !Objects.equals(controlPlan.getRemoteSources(), testPlan.getRemoteSources())
                || controlPlan.getChildren().size() != testPlan.getChildren().size()) {
            return STRUCTURE_MISMATCH;
        }

        boolean detailsMismatched = !Objects.equals(controlPlan.getDetails(), testPlan.getDetails())
                || !Objects.equals(controlPlan.getEstimates(), testPlan.getEstimates());
        for (int i = 0; i < controlPlan.getChildren().size(); i++) {
            MatchType childMatchType = match(controlPlan.getChildren().get(i), testPlan.getChildren().get(i));
            if (childMatchType == STRUCTURE_MISMATCH) {
                return STRUCTURE_MISMATCH;
            }
            else if (childMatchType == DETAILS_MISMATCH) {
                detailsMismatched = true;
            }
        }
        return detailsMismatched ? DETAILS_MISMATCH : MATCH;
    }
}
