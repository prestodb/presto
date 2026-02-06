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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableMetadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getRustCascadeOptimizerUrl;
import static com.facebook.presto.SystemSessionProperties.isRustCascadeOptimizerEnabled;
import static java.util.Objects.requireNonNull;

/**
 * Plan optimizer that delegates join reordering to the external Rust Cascades optimizer.
 *
 * <p>This optimizer extracts a join graph (tables + equi-join conditions) from the current
 * plan tree, sends it to the Rust optimizer service via HTTP, and rebuilds the join tree
 * in the order determined by the Cascades search.
 *
 * <p>The optimizer is gated by the {@code use_rust_cascade_optimizer} session property
 * (default: false). When disabled or when the Rust service is unavailable, the optimizer
 * is a no-op and the original plan is returned unchanged.
 *
 * <h3>Placement in the optimizer pipeline</h3>
 * This optimizer runs after PhysicalCteOptimizer and before AddExchanges:
 * <ul>
 *   <li>All rule-based logical optimizations are already done</li>
 *   <li>Join distribution types are determined</li>
 *   <li>The plan is still in logical form (no exchanges yet)</li>
 *   <li>The Rust optimizer can reorder joins before exchanges are added</li>
 * </ul>
 *
 * <h3>Future work</h3>
 * <ul>
 *   <li>Replace the join-graph JSON protocol with full Substrait conversion for
 *       production use. This requires mapping Presto's TableHandle/ColumnHandle/
 *       RowExpression types to Substrait.</li>
 *   <li>Support non-equi join predicates (range joins, theta joins).</li>
 *   <li>Support outer join reordering with proper ordering constraints.</li>
 *   <li>Support multi-column equi-joins as single join edges.</li>
 *   <li>Connection pooling / reuse for the HTTP client.</li>
 *   <li>Metrics and logging for optimization latency and fallback rate.</li>
 * </ul>
 */
public class RustCascadeOptimizer
        implements PlanOptimizer
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    private final Metadata metadata;
    private boolean isEnabledForTesting;

    public RustCascadeOptimizer(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public void setEnabledForTesting(boolean isSet)
    {
        isEnabledForTesting = isSet;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isEnabledForTesting || isRustCascadeOptimizerEnabled(session);
    }

    @Override
    public boolean isCostBased(Session session)
    {
        return true;
    }

    @Override
    public PlanOptimizerResult optimize(
            PlanNode plan,
            Session session,
            TypeProvider types,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        if (!isEnabled(session)) {
            return PlanOptimizerResult.optimizerResult(plan, false);
        }

        // Step 1: Extract join graph from the plan tree.
        JoinGraphExtractor extractor = new JoinGraphExtractor(metadata, session);
        SimplePlanRewriter.rewriteWith(extractor, plan, null);

        if (extractor.getJoinEdges().isEmpty()) {
            // No joins to optimize.
            return PlanOptimizerResult.optimizerResult(plan, false);
        }

        // Step 2: Call Rust optimizer.
        String url = getRustCascadeOptimizerUrl(session);
        Optional<JsonNode> optimizedTree = RustOptimizerClient.optimize(url, extractor);
        if (!optimizedTree.isPresent()) {
            // Rust optimizer unavailable or returned error; fall back to original plan.
            return PlanOptimizerResult.optimizerResult(plan, false);
        }

        // Step 3: Rebuild the plan tree with the optimized join order.
        try {
            PlanNode newPlan = JoinTreeRebuilder.rebuild(
                    plan,
                    optimizedTree.get(),
                    extractor,
                    idAllocator);
            return PlanOptimizerResult.optimizerResult(newPlan, true);
        }
        catch (Exception e) {
            // If rebuilding fails for any reason, fall back to original plan.
            // TODO: Add logging/metrics for rebuild failures to diagnose issues.
            return PlanOptimizerResult.optimizerResult(plan, false);
        }
    }

    // -----------------------------------------------------------------------
    // JoinGraphExtractor: walks the plan tree to collect tables and join edges
    // -----------------------------------------------------------------------

    /**
     * Walks the plan tree and extracts a join graph suitable for the Rust optimizer.
     *
     * <p>Collects:
     * <ul>
     *   <li>Table scans with their statistics (row count, size, column NDVs)</li>
     *   <li>Equi-join edges between pairs of tables</li>
     * </ul>
     *
     * <h3>Limitations / Future Work</h3>
     * <ul>
     *   <li>Only considers the first join tree encountered; plans with multiple
     *       independent join trees (e.g., UNION of two join queries) will only
     *       optimize the first one.</li>
     *   <li>Only handles equi-join conditions (EquiJoinClause). Non-equi filters
     *       on JoinNode are ignored for join reordering purposes.</li>
     *   <li>Filter nodes above/below table scans are not communicated to the Rust
     *       optimizer. These could improve cardinality estimates.</li>
     * </ul>
     */
    static class JoinGraphExtractor
            extends SimplePlanRewriter<Void>
    {
        private final Metadata metadata;
        private final Session session;

        // Map from table ID (e.g., "t0") to the original TableScanNode.
        private final Map<String, TableScanNode> tableScanNodes = new LinkedHashMap<>();
        // Map from table ID to table metadata (schema + name).
        private final Map<String, SchemaTableName> tableNames = new LinkedHashMap<>();
        // Map from table ID to table statistics.
        private final Map<String, TableStatistics> tableStats = new LinkedHashMap<>();
        // Map from variable name to the table ID that produces it.
        private final Map<String, String> variableToTableId = new HashMap<>();
        // Map from variable name to column name (via ColumnHandle metadata lookup).
        private final Map<String, String> variableToColumnName = new HashMap<>();
        // List of equi-join edges.
        private final List<JoinEdgeInfo> joinEdges = new ArrayList<>();
        // The root JoinNode (topmost join in the tree).
        private PlanNode rootJoin;
        // The parent of the root join (e.g., AggregationNode) whose source we'll replace.
        private PlanNode rootJoinParent;

        private int tableCounter;

        JoinGraphExtractor(Metadata metadata, Session session)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = requireNonNull(session, "session is null");
        }

        Map<String, TableScanNode> getTableScanNodes()
        {
            return Collections.unmodifiableMap(tableScanNodes);
        }

        Map<String, SchemaTableName> getTableNames()
        {
            return Collections.unmodifiableMap(tableNames);
        }

        Map<String, TableStatistics> getTableStats()
        {
            return Collections.unmodifiableMap(tableStats);
        }

        List<JoinEdgeInfo> getJoinEdges()
        {
            return Collections.unmodifiableList(joinEdges);
        }

        String getVariableTableId(String variableName)
        {
            return variableToTableId.get(variableName);
        }

        String getVariableColumnName(String variableName)
        {
            return variableToColumnName.getOrDefault(variableName, variableName);
        }

        PlanNode getRootJoin()
        {
            return rootJoin;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            String tableId = "t" + tableCounter++;

            tableScanNodes.put(tableId, node);

            // Get table name from metadata.
            TableMetadata tableMetadata = metadata.getTableMetadata(session, node.getTable());
            SchemaTableName schemaTableName = tableMetadata.getTable();
            tableNames.put(tableId, schemaTableName);

            // Get table statistics.
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(node.getAssignments().values());
            TableStatistics stats = metadata.getTableStatistics(session, node.getTable(), columnHandles, Constraint.alwaysTrue());
            tableStats.put(tableId, stats);

            // Map each output variable to its table ID and column name.
            for (Map.Entry<VariableReferenceExpression, ColumnHandle> entry : node.getAssignments().entrySet()) {
                VariableReferenceExpression variable = entry.getKey();
                ColumnHandle columnHandle = entry.getValue();
                variableToTableId.put(variable.getName(), tableId);
                // Look up the actual column name from metadata.
                ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, node.getTable(), columnHandle);
                variableToColumnName.put(variable.getName(), columnMetadata.getName());
            }

            return node;
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            // Visit children first (bottom-up) to collect table scans before processing joins.
            context.defaultRewrite(node, context.get());

            // Record this as the root join (the last one visited in bottom-up order is the topmost).
            rootJoin = node;

            // Record equi-join edges.
            for (EquiJoinClause clause : node.getCriteria()) {
                String leftVarName = clause.getLeft().getName();
                String rightVarName = clause.getRight().getName();
                String leftTableId = variableToTableId.get(leftVarName);
                String rightTableId = variableToTableId.get(rightVarName);

                if (leftTableId != null && rightTableId != null) {
                    joinEdges.add(new JoinEdgeInfo(
                            leftTableId,
                            rightTableId,
                            node.getType(),
                            clause,
                            leftVarName,
                            rightVarName));
                }
            }

            return node;
        }
    }

    /**
     * Holds information about an equi-join edge between two tables.
     */
    static class JoinEdgeInfo
    {
        private final String leftTableId;
        private final String rightTableId;
        private final JoinType joinType;
        private final EquiJoinClause clause;
        private final String leftVariableName;
        private final String rightVariableName;

        JoinEdgeInfo(
                String leftTableId,
                String rightTableId,
                JoinType joinType,
                EquiJoinClause clause,
                String leftVariableName,
                String rightVariableName)
        {
            this.leftTableId = leftTableId;
            this.rightTableId = rightTableId;
            this.joinType = joinType;
            this.clause = clause;
            this.leftVariableName = leftVariableName;
            this.rightVariableName = rightVariableName;
        }
    }

    // -----------------------------------------------------------------------
    // RustOptimizerClient: sends join graph to Rust service, gets optimized tree
    // -----------------------------------------------------------------------

    /**
     * HTTP client that sends the join graph to the Rust Cascades optimizer and returns
     * the optimized join tree.
     *
     * <p>On any failure (connection refused, timeout, HTTP error, parse error), returns
     * {@code Optional.empty()} so the optimizer gracefully falls back to the original plan.
     *
     * <h3>Future Work</h3>
     * <ul>
     *   <li>Connection pooling for the HTTP client (currently creates a new request each time).</li>
     *   <li>Retry logic with backoff for transient failures.</li>
     *   <li>Metrics for request latency, success rate, and fallback rate.</li>
     * </ul>
     */
    static class RustOptimizerClient
    {
        private RustOptimizerClient() {}

        static Optional<JsonNode> optimize(String baseUrl, JoinGraphExtractor extractor)
        {
            try {
                String jsonBody = buildRequestJson(extractor);

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + "/optimize/join-graph"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                        .timeout(Duration.ofSeconds(10))
                        .build();

                HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() != 200) {
                    return Optional.empty();
                }

                JsonNode responseJson = MAPPER.readTree(response.body());
                JsonNode tree = responseJson.get("tree");
                if (tree == null) {
                    return Optional.empty();
                }

                return Optional.of(tree);
            }
            catch (Exception e) {
                // Any failure → graceful fallback.
                // TODO: Log the exception at DEBUG level for troubleshooting.
                return Optional.empty();
            }
        }

        /**
         * Builds the JSON request body for the /optimize/join-graph endpoint.
         *
         * <p>Converts Presto's internal table statistics (row count, total size,
         * per-column NDV) into the simplified JSON protocol expected by the Rust optimizer.
         *
         * <h3>Future Work</h3>
         * <ul>
         *   <li>Include filter predicates for better cardinality estimation.</li>
         *   <li>Handle multi-column equi-joins as single edges with multiple column pairs.</li>
         * </ul>
         */
        static String buildRequestJson(JoinGraphExtractor extractor)
                throws Exception
        {
            ObjectNode root = MAPPER.createObjectNode();

            // Tables
            ArrayNode tablesArray = MAPPER.createArrayNode();
            for (Map.Entry<String, TableScanNode> entry : extractor.getTableScanNodes().entrySet()) {
                String tableId = entry.getKey();
                SchemaTableName schemaTableName = extractor.getTableNames().get(tableId);
                TableStatistics stats = extractor.getTableStats().get(tableId);

                ObjectNode tableNode = MAPPER.createObjectNode();
                tableNode.put("id", tableId);
                tableNode.put("schema", schemaTableName.getSchemaName());
                tableNode.put("name", schemaTableName.getTableName());
                tableNode.put("rowCount", estimateValue(stats.getRowCount(), 1000.0));
                tableNode.put("sizeBytes", estimateValue(stats.getTotalSize(), 100000.0));

                // Column statistics
                ArrayNode columnsArray = MAPPER.createArrayNode();
                TableScanNode scanNode = entry.getValue();
                for (Map.Entry<VariableReferenceExpression, ColumnHandle> colEntry : scanNode.getAssignments().entrySet()) {
                    ColumnHandle columnHandle = colEntry.getValue();
                    String variableName = colEntry.getKey().getName();
                    String columnName = extractor.getVariableColumnName(variableName);

                    ObjectNode colNode = MAPPER.createObjectNode();
                    colNode.put("name", columnName);

                    // Get column-level statistics if available.
                    ColumnStatistics colStats = stats.getColumnStatistics().get(columnHandle);
                    if (colStats != null) {
                        colNode.put("ndv", estimateValue(colStats.getDistinctValuesCount(), estimateValue(stats.getRowCount(), 1000.0)));
                        colNode.put("nullFraction", estimateValue(colStats.getNullsFraction(), 0.0));
                        colNode.put("avgSize", estimateValue(colStats.getDataSize(), 8.0));
                    }
                    else {
                        // Default NDV: assume all values are distinct (conservative estimate).
                        // TODO: Use a smarter default based on data type and table size.
                        colNode.put("ndv", estimateValue(stats.getRowCount(), 1000.0));
                        colNode.put("nullFraction", 0.0);
                        colNode.put("avgSize", 8.0);
                    }

                    columnsArray.add(colNode);
                }
                tableNode.set("columns", columnsArray);
                tablesArray.add(tableNode);
            }
            root.set("tables", tablesArray);

            // Joins
            ArrayNode joinsArray = MAPPER.createArrayNode();
            for (JoinEdgeInfo edge : extractor.getJoinEdges()) {
                ObjectNode joinNode = MAPPER.createObjectNode();
                joinNode.put("leftTableId", edge.leftTableId);
                joinNode.put("rightTableId", edge.rightTableId);
                joinNode.put("joinType", edge.joinType.name());
                joinNode.put("leftColumn", extractor.getVariableColumnName(edge.leftVariableName));
                joinNode.put("rightColumn", extractor.getVariableColumnName(edge.rightVariableName));
                joinsArray.add(joinNode);
            }
            root.set("joins", joinsArray);

            return MAPPER.writeValueAsString(root);
        }

        private static double estimateValue(Estimate estimate, double defaultValue)
        {
            return estimate.isUnknown() ? defaultValue : estimate.getValue();
        }
    }

    // -----------------------------------------------------------------------
    // JoinTreeRebuilder: reconstructs PlanNode tree from optimized join order
    // -----------------------------------------------------------------------

    /**
     * Reconstructs the Presto {@link PlanNode} join tree from the Rust optimizer's
     * response tree.
     *
     * <p>Walks the JSON response tree recursively:
     * <ul>
     *   <li>Leaf nodes (with "tableId") are mapped back to the original {@link TableScanNode}.</li>
     *   <li>Join nodes are converted to new {@link JoinNode} instances with the original
     *       {@link EquiJoinClause} looked up from the extractor's join edge list.</li>
     * </ul>
     *
     * <p>The rebuilt join tree is grafted onto the original plan by replacing the subtree
     * rooted at the topmost JoinNode. Non-join operators above the root join (e.g.,
     * AggregationNode, ProjectNode) are preserved.
     *
     * <h3>Future Work</h3>
     * <ul>
     *   <li>Handle non-equi join filters: currently only equi-join criteria are
     *       preserved. If the original JoinNode had a non-equi filter, it is dropped.</li>
     *   <li>Preserve JoinDistributionType from the original plan.</li>
     *   <li>Preserve dynamic filters from the original join nodes.</li>
     * </ul>
     */
    static class JoinTreeRebuilder
    {
        private JoinTreeRebuilder() {}

        /**
         * Rebuilds the plan by replacing the join subtree with the optimized order.
         *
         * @param originalPlan The complete original plan (may have non-join operators above joins).
         * @param optimizedTree The JSON tree from the Rust optimizer.
         * @param extractor The extractor containing table scan mappings and join edges.
         * @param idAllocator For generating new PlanNodeIds.
         * @return The plan with joins reordered according to the optimizer's output.
         */
        static PlanNode rebuild(
                PlanNode originalPlan,
                JsonNode optimizedTree,
                JoinGraphExtractor extractor,
                PlanNodeIdAllocator idAllocator)
        {
            // Build the new join subtree from the optimized order.
            PlanNode newJoinTree = buildFromTree(optimizedTree, extractor, idAllocator);

            // Replace the old join tree in the original plan.
            PlanNode rootJoin = extractor.getRootJoin();
            if (rootJoin == null) {
                return originalPlan;
            }

            // Use a rewriter to find and replace the root join node.
            return SimplePlanRewriter.rewriteWith(new JoinReplacer(rootJoin.getId(), newJoinTree), originalPlan, null);
        }

        /**
         * Recursively builds a PlanNode tree from the Rust optimizer's JSON response.
         */
        private static PlanNode buildFromTree(
                JsonNode node,
                JoinGraphExtractor extractor,
                PlanNodeIdAllocator idAllocator)
        {
            // Leaf node: reference to a table scan.
            if (node.has("tableId")) {
                String tableId = node.get("tableId").asText();
                TableScanNode scanNode = extractor.getTableScanNodes().get(tableId);
                if (scanNode == null) {
                    throw new IllegalStateException("Unknown table ID from optimizer: " + tableId);
                }
                return scanNode;
            }

            // Join node: has left, right, joinType, leftColumn, rightColumn.
            PlanNode left = buildFromTree(node.get("left"), extractor, idAllocator);
            PlanNode right = buildFromTree(node.get("right"), extractor, idAllocator);

            String leftColumnName = node.has("leftColumn") ? node.get("leftColumn").asText() : "";
            String rightColumnName = node.has("rightColumn") ? node.get("rightColumn").asText() : "";
            String joinTypeStr = node.has("joinType") ? node.get("joinType").asText() : "INNER";
            JoinType joinType = parseJoinType(joinTypeStr);

            // Find the matching EquiJoinClause from the original plan.
            EquiJoinClause matchedClause = findMatchingClause(
                    leftColumnName, rightColumnName, left, right, extractor);

            // Build output variables: union of both sides' outputs.
            List<VariableReferenceExpression> outputVariables = ImmutableList.<VariableReferenceExpression>builder()
                    .addAll(left.getOutputVariables())
                    .addAll(right.getOutputVariables())
                    .build();

            List<EquiJoinClause> criteria = matchedClause != null
                    ? ImmutableList.of(matchedClause)
                    : ImmutableList.of();

            return new JoinNode(
                    Optional.empty(),
                    idAllocator.getNextId(),
                    Optional.empty(),
                    joinType,
                    left,
                    right,
                    criteria,
                    outputVariables,
                    Optional.empty(),    // filter
                    Optional.empty(),    // leftHashVariable
                    Optional.empty(),    // rightHashVariable
                    Optional.empty(),    // distributionType
                    ImmutableMap.of());  // dynamicFilters
        }

        /**
         * Finds the EquiJoinClause that matches the given column names, accounting for
         * the fact that the optimizer may have swapped left/right sides.
         *
         * <p>The matcher looks through the original join edges to find the clause whose
         * left/right variable column names match the requested columns, checking both
         * orientations (original and flipped).
         *
         * <h3>Future Work</h3>
         * <ul>
         *   <li>Support matching by VariableReferenceExpression directly when the column
         *       name lookup is ambiguous (e.g., self-joins where the same column name
         *       appears on both sides).</li>
         * </ul>
         */
        private static EquiJoinClause findMatchingClause(
                String leftColumn,
                String rightColumn,
                PlanNode leftChild,
                PlanNode rightChild,
                JoinGraphExtractor extractor)
        {
            // Build sets of variable names available from left and right children.
            java.util.Set<String> leftVars = new java.util.HashSet<>();
            for (VariableReferenceExpression v : leftChild.getOutputVariables()) {
                leftVars.add(v.getName());
            }
            java.util.Set<String> rightVars = new java.util.HashSet<>();
            for (VariableReferenceExpression v : rightChild.getOutputVariables()) {
                rightVars.add(v.getName());
            }

            // Search through all recorded join edges for a matching clause.
            for (JoinEdgeInfo edge : extractor.getJoinEdges()) {
                String edgeLeftCol = extractor.getVariableColumnName(edge.leftVariableName);
                String edgeRightCol = extractor.getVariableColumnName(edge.rightVariableName);

                // Check if this edge matches the requested columns in original orientation.
                if (edgeLeftCol.equals(leftColumn) && edgeRightCol.equals(rightColumn)) {
                    // Check variables are on the correct sides.
                    if (leftVars.contains(edge.leftVariableName) && rightVars.contains(edge.rightVariableName)) {
                        return edge.clause;
                    }
                    // Variables are swapped — flip the clause.
                    if (rightVars.contains(edge.leftVariableName) && leftVars.contains(edge.rightVariableName)) {
                        return edge.clause.flip();
                    }
                }

                // Check flipped orientation (optimizer may have swapped the join sides).
                if (edgeLeftCol.equals(rightColumn) && edgeRightCol.equals(leftColumn)) {
                    if (leftVars.contains(edge.rightVariableName) && rightVars.contains(edge.leftVariableName)) {
                        return edge.clause.flip();
                    }
                    if (rightVars.contains(edge.rightVariableName) && leftVars.contains(edge.leftVariableName)) {
                        return edge.clause;
                    }
                }
            }

            // No matching clause found.
            // TODO: Log a warning when no matching clause is found — this indicates a
            // mismatch between the Rust optimizer's output and the original join graph.
            return null;
        }

        private static JoinType parseJoinType(String joinTypeStr)
        {
            switch (joinTypeStr.toUpperCase()) {
                case "INNER": return JoinType.INNER;
                case "LEFT": return JoinType.LEFT;
                case "RIGHT": return JoinType.RIGHT;
                case "FULL": return JoinType.FULL;
                default: return JoinType.INNER;
            }
        }
    }

    /**
     * SimplePlanRewriter that replaces a specific JoinNode (identified by PlanNodeId)
     * with a new subtree. Used to graft the optimized join tree onto the original plan.
     */
    private static class JoinReplacer
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeId targetId;
        private final PlanNode replacement;

        JoinReplacer(PlanNodeId targetId, PlanNode replacement)
        {
            this.targetId = requireNonNull(targetId, "targetId is null");
            this.replacement = requireNonNull(replacement, "replacement is null");
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            if (node.getId().equals(targetId)) {
                return replacement;
            }
            return super.visitJoin(node, context);
        }
    }
}
