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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static com.facebook.presto.sql.tree.ExplainFormat.Type.JSON;
import static com.facebook.presto.testing.TestingEnvironment.FUNCTION_AND_TYPE_MANAGER;
import static com.facebook.presto.verifier.framework.ExplainMatchResult.MatchType.DETAILS_MISMATCH;
import static com.facebook.presto.verifier.framework.ExplainMatchResult.MatchType.MATCH;
import static com.facebook.presto.verifier.framework.ExplainMatchResult.MatchType.SNAPSHOT_DOES_NOT_EXIST;
import static com.facebook.presto.verifier.framework.ExplainMatchResult.MatchType.STRUCTURE_MISMATCH;
import static com.facebook.presto.verifier.framework.VerifierUtil.PARSING_OPTIONS;
import static com.facebook.presto.verifier.source.AbstractJdbiSnapshotQuerySupplier.VERIFIER_SNAPSHOT_KEY_PATTERN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

public class ExplainVerification
        extends AbstractVerification<QueryBundle, ExplainMatchResult, String>
{
    private static final ResultSetConverter<String> QUERY_PLAN_RESULT_SET_CONVERTER = resultSet -> Optional.of(resultSet.getString("Query Plan"));
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    // Caps the mismatch report so it stays readable and fits verifier result storage.
    private static final int MAX_REPORTED_DIFFERENCES = 10;
    private static final int MAX_REPORTED_VALUE_LENGTH = 200;
    private static JsonCodec<JsonRenderedNode> planCodec;
    private final SqlParser sqlParser;
    private final boolean passthroughExplain;

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
        // A source query that is already an EXPLAIN statement (e.g. sampled directly
        // from production, such as EXPLAIN (TYPE IO) ...) is verified verbatim rather
        // than re-wrapped in EXPLAIN (FORMAT JSON).
        this.passthroughExplain = sqlParser.createStatement(sourceQuery.getQuery(ClusterType.CONTROL), PARSING_OPTIONS) instanceof Explain;
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
        // Preserve an already-EXPLAIN source query verbatim so its explain type is
        // exercised as-is; otherwise wrap in EXPLAIN (FORMAT JSON) for plan-tree comparison.
        Statement explain = statement instanceof Explain
                ? statement
                : new Explain(statement, false, false, ImmutableList.of(new ExplainFormat(JSON)));
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

        String controlResult;
        if (isControlEnabled()) {
            checkArgument(controlQueryResult.isPresent(), "control query plan is missing");
            controlResult = getOnlyElement(controlQueryResult.get().getResults());
            if (saveSnapshot) {
                snapshotQueryConsumer.accept(new SnapshotQuery(getSourceQuery().getSuite(), getSourceQuery().getName(), isExplain, controlResult));
                return new ExplainMatchResult(MATCH);
            }
        }
        else {
            String key = format(VERIFIER_SNAPSHOT_KEY_PATTERN, getSourceQuery().getSuite(), getSourceQuery().getName(), isExplain);
            SnapshotQuery snapshotQuery = snapshotQueries.get(key);
            if (snapshotQuery == null) {
                return new ExplainMatchResult(SNAPSHOT_DOES_NOT_EXIST);
            }
            controlResult = snapshotQuery.getSnapshot();
        }
        String testResult = getOnlyElement(testQueryResult.get().getResults());

        if (passthroughExplain) {
            return matchRaw(controlResult, testResult);
        }
        return match(planCodec.fromJson(controlResult), planCodec.fromJson(testResult), ImmutableList.of());
    }

    @Override
    protected DeterminismAnalysisDetails analyzeDeterminism(QueryBundle controlObject, QueryBundle testObject, ExplainMatchResult matchResult)
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

    @VisibleForTesting
    static ExplainMatchResult matchRaw(String controlResult, String testResult)
    {
        // Non-plan-tree EXPLAIN output (e.g. EXPLAIN (TYPE IO)) is compared as JSON.
        // Object field order is already ignored by JsonNode equality; array element
        // order is normalized here because such arrays serialize unordered Sets (input
        // tables, column constraints, ranges). Fall back to exact string comparison for
        // non-JSON output.
        JsonNode controlNode;
        JsonNode testNode;
        try {
            controlNode = canonicalize(OBJECT_MAPPER.readTree(controlResult));
            testNode = canonicalize(OBJECT_MAPPER.readTree(testResult));
        }
        catch (IOException e) {
            return new ExplainMatchResult(controlResult.equals(testResult) ? MATCH : STRUCTURE_MISMATCH);
        }
        if (controlNode.equals(testNode)) {
            return new ExplainMatchResult(MATCH);
        }
        List<String> differences = new ArrayList<>();
        collectDifferences("", controlNode, testNode, differences);
        return new ExplainMatchResult(STRUCTURE_MISMATCH, Optional.of(formatDifferences(differences)));
    }

    /**
     * Lists every differing leaf as one "path: control -> test" line, so a whole run's
     * mismatches can be grouped by path from the error message. Array elements are
     * compared in canonical order and share one "[]" path segment.
     */
    private static void collectDifferences(String path, JsonNode control, JsonNode test, List<String> differences)
    {
        if (control.isObject() && test.isObject()) {
            Set<String> fields = new TreeSet<>();
            control.fieldNames().forEachRemaining(fields::add);
            test.fieldNames().forEachRemaining(fields::add);
            for (String field : fields) {
                String fieldPath = path.isEmpty() ? field : path + "." + field;
                JsonNode controlField = control.get(field);
                JsonNode testField = test.get(field);
                if (controlField == null || testField == null) {
                    differences.add(format("%s: %s -> %s", fieldPath, renderValue(controlField), renderValue(testField)));
                }
                else {
                    collectDifferences(fieldPath, controlField, testField, differences);
                }
            }
            return;
        }
        String reportedPath = path.isEmpty() ? "(root)" : path;
        if (control.isArray() && test.isArray()) {
            if (control.size() != test.size()) {
                differences.add(format("%s: %s elements -> %s elements", reportedPath, control.size(), test.size()));
                return;
            }
            for (int i = 0; i < control.size(); i++) {
                collectDifferences(path + "[]", control.get(i), test.get(i), differences);
            }
            return;
        }
        if (!control.equals(test)) {
            differences.add(format("%s: %s -> %s", reportedPath, renderValue(control), renderValue(test)));
        }
    }

    private static String renderValue(JsonNode node)
    {
        return node == null ? "(missing)" : truncate(node.toString());
    }

    private static String truncate(String value)
    {
        if (value == null) {
            return "(missing)";
        }
        return value.length() <= MAX_REPORTED_VALUE_LENGTH ? value : value.substring(0, MAX_REPORTED_VALUE_LENGTH) + "...";
    }

    private static String formatDifferences(List<String> differences)
    {
        StringBuilder report = new StringBuilder("Differing Fields:");
        differences.stream()
                .limit(MAX_REPORTED_DIFFERENCES)
                .forEach(difference -> report.append("\n  ").append(difference));
        if (differences.size() > MAX_REPORTED_DIFFERENCES) {
            report.append(format("\n  ... and %s more", differences.size() - MAX_REPORTED_DIFFERENCES));
        }
        return report.toString();
    }

    private static JsonNode canonicalize(JsonNode node)
    {
        if (node.isArray()) {
            List<JsonNode> children = new ArrayList<>();
            for (JsonNode child : node) {
                children.add(canonicalize(child));
            }
            children.sort(comparing(JsonNode::toString));
            ArrayNode sorted = OBJECT_MAPPER.createArrayNode();
            sorted.addAll(children);
            return sorted;
        }
        if (node.isObject()) {
            ObjectNode result = OBJECT_MAPPER.createObjectNode();
            node.fields().forEachRemaining(entry -> result.set(entry.getKey(), canonicalize(entry.getValue())));
            return result;
        }
        return node;
    }

    /**
     * Compares plan trees node by node. A structure mismatch reports the first node where
     * the plans diverge, prefixed with the names of its ancestors.
     */
    private static ExplainMatchResult match(JsonRenderedNode controlPlan, JsonRenderedNode testPlan, List<String> ancestors)
    {
        Optional<String> structureDifference = structureDifference(controlPlan, testPlan);
        if (structureDifference.isPresent()) {
            String location = ancestors.isEmpty() ? "" : String.join(" > ", ancestors) + ": ";
            return new ExplainMatchResult(
                    STRUCTURE_MISMATCH,
                    Optional.of("Differing Plan Node:\n  " + location + structureDifference.get()));
        }

        boolean detailsMismatched = !Objects.equals(controlPlan.getDetails(), testPlan.getDetails())
                || !Objects.equals(controlPlan.getEstimates(), testPlan.getEstimates());
        List<String> childAncestors = ImmutableList.<String>builder()
                .addAll(ancestors)
                .add(controlPlan.getName())
                .build();
        for (int i = 0; i < controlPlan.getChildren().size(); i++) {
            ExplainMatchResult childResult = match(controlPlan.getChildren().get(i), testPlan.getChildren().get(i), childAncestors);
            if (!childResult.isMatched()) {
                return childResult;
            }
            if (childResult.getMatchType() == DETAILS_MISMATCH) {
                detailsMismatched = true;
            }
        }
        return new ExplainMatchResult(detailsMismatched ? DETAILS_MISMATCH : MATCH);
    }

    private static Optional<String> structureDifference(JsonRenderedNode controlPlan, JsonRenderedNode testPlan)
    {
        if (!Objects.equals(controlPlan.getName(), testPlan.getName())) {
            return Optional.of(format("%s -> %s", controlPlan.getName(), testPlan.getName()));
        }
        if (!Objects.equals(controlPlan.getIdentifier(), testPlan.getIdentifier())) {
            return Optional.of(format(
                    "%s identifier %s -> %s",
                    controlPlan.getName(),
                    truncate(controlPlan.getIdentifier()),
                    truncate(testPlan.getIdentifier())));
        }
        if (!Objects.equals(controlPlan.getRemoteSources(), testPlan.getRemoteSources())) {
            return Optional.of(format("%s remote sources %s -> %s", controlPlan.getName(), controlPlan.getRemoteSources(), testPlan.getRemoteSources()));
        }
        if (controlPlan.getChildren().size() != testPlan.getChildren().size()) {
            return Optional.of(format(
                    "%s children %s -> %s",
                    controlPlan.getName(),
                    controlPlan.getChildren().size(),
                    testPlan.getChildren().size()));
        }
        return Optional.empty();
    }
}
