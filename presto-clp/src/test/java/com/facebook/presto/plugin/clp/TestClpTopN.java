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
package com.facebook.presto.plugin.clp;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.plugin.clp.optimization.ClpComputePushDown;
import com.facebook.presto.plugin.clp.optimization.ClpTopNSpec;
import com.facebook.presto.plugin.clp.optimization.ClpTopNSpec.Order;
import com.facebook.presto.plugin.clp.split.ClpSplitProvider;
import com.facebook.presto.plugin.clp.split.filter.ClpMySqlSplitFilterProvider;
import com.facebook.presto.plugin.clp.split.filter.ClpSplitFilterProvider;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.math3.util.Pair;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.Utils.checkState;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.ARCHIVES_STORAGE_DIRECTORY_BASE;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.METADATA_DB_PASSWORD;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.METADATA_DB_TABLE_PREFIX;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.METADATA_DB_URL_TEMPLATE;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.METADATA_DB_USER;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.getDbHandle;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.setupMetadata;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.setupSplit;
import static com.facebook.presto.plugin.clp.ClpSplit.SplitType.ARCHIVE;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.Boolean;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.ClpString;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.Float;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.Integer;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.VarString;
import static com.facebook.presto.plugin.clp.optimization.ClpTopNSpec.Order.ASC;
import static com.facebook.presto.plugin.clp.optimization.ClpTopNSpec.Order.DESC;
import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestClpTopN
        extends TestClpQueryBase
{
    private final Session defaultSession = testSessionBuilder()
            .setCatalog("clp")
            .setSchema(ClpMetadata.DEFAULT_SCHEMA_NAME)
            .build();

    private ClpMetadataDbSetUp.DbHandle dbHandle;
    ClpTableHandle table;

    private static final Logger log = Logger.get(TestClpTopN.class);

    private LocalQueryRunner localQueryRunner;
    private FunctionAndTypeManager functionAndTypeManager;
    private FunctionResolution functionResolution;
    private ClpSplitProvider splitProvider;
    private ClpSplitFilterProvider splitFilterProvider;
    private PlanNodeIdAllocator planNodeIdAllocator;
    private VariableAllocator variableAllocator;

    @BeforeMethod
    public void setUp()
    {
        dbHandle = getDbHandle("topn_query_testdb");
        final String tableName = "test";
        final String tablePath = ARCHIVES_STORAGE_DIRECTORY_BASE + tableName;
        table = new ClpTableHandle(new SchemaTableName("default", tableName), tablePath);

        setupMetadata(dbHandle,
                ImmutableMap.of(
                        tableName,
                        ImmutableList.of(
                                new Pair<>("msg.timestamp", Integer),
                                new Pair<>("city.Name", ClpString),
                                new Pair<>("city.Region.Id", Integer),
                                new Pair<>("city.Region.Name", VarString),
                                new Pair<>("fare", Float),
                                new Pair<>("isHoliday", Boolean))));

        splitProvider = setupSplit(dbHandle,
                ImmutableMap.of(
                        tableName,
                        ImmutableList.of(
                                new ClpMetadataDbSetUp.ArchivesTableRow("0", 100, 0, 100),
                                new ClpMetadataDbSetUp.ArchivesTableRow("1", 100, 50, 150),
                                new ClpMetadataDbSetUp.ArchivesTableRow("2", 100, 100, 200),
                                new ClpMetadataDbSetUp.ArchivesTableRow("3", 100, 201, 300),
                                new ClpMetadataDbSetUp.ArchivesTableRow("4", 100, 301, 400))));

        URL resource = getClass().getClassLoader().getResource("test-topn-split-filter.json");
        if (resource == null) {
            log.error("test-topn-split-filter.json not found in resources");
            return;
        }

        String filterConfigPath;
        try {
            filterConfigPath = Paths.get(resource.toURI()).toAbsolutePath().toString();
        }
        catch (URISyntaxException e) {
            log.error("test-topn-split-filter.json not found in resources");
            return;
        }

        localQueryRunner = new LocalQueryRunner(defaultSession);
        localQueryRunner.createCatalog("clp", new ClpConnectorFactory(), ImmutableMap.of(
                "clp.metadata-db-url", format(METADATA_DB_URL_TEMPLATE, dbHandle.getDbPath()),
                "clp.metadata-db-user", METADATA_DB_USER,
                "clp.metadata-db-password", METADATA_DB_PASSWORD,
                "clp.metadata-table-prefix", METADATA_DB_TABLE_PREFIX));
        localQueryRunner.getMetadata().registerBuiltInFunctions(extractFunctions(new ClpPlugin().getFunctions()));
        functionAndTypeManager = localQueryRunner.getMetadata().getFunctionAndTypeManager();
        functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
        splitFilterProvider = new ClpMySqlSplitFilterProvider(new ClpConfig().setSplitFilterConfig(filterConfigPath));
        planNodeIdAllocator = new PlanNodeIdAllocator();
        variableAllocator = new VariableAllocator();
    }

    @AfterMethod
    public void tearDown()
    {
        localQueryRunner.close();
        ClpMetadataDbSetUp.tearDown(dbHandle);
    }

    @Test
    public void test()
    {
        testTopNQueryPlanAndSplits(
                "SELECT * FROM test WHERE msg.timestamp > 120 AND msg.timestamp < 240 ORDER BY msg.timestamp DESC LIMIT 100",
                "(msg.timestamp > 120 AND msg.timestamp < 240)",
                "(end_timestamp > 120 AND begin_timestamp < 240)",
                100,
                DESC,
                ImmutableSet.of("1", "2", "3"));

        testTopNQueryPlanAndSplits(
                "SELECT * FROM test WHERE msg.timestamp > 120 AND msg.timestamp < 240 ORDER BY msg.timestamp ASC LIMIT 50",
                "(msg.timestamp > 120 AND msg.timestamp < 240)",
                "(end_timestamp > 120 AND begin_timestamp < 240)",
                50,
                ASC,
                ImmutableSet.of("1", "2", "3"));

        testTopNQueryPlanAndSplits(
                "SELECT * FROM test WHERE msg.timestamp >= 180 AND msg.timestamp <= 260 ORDER BY msg.timestamp DESC LIMIT 100",
                "(msg.timestamp >= 180 AND msg.timestamp <= 260)",
                "(end_timestamp >= 180 AND begin_timestamp <= 260)",
                100,
                DESC,
                ImmutableSet.of("2", "3"));

        testTopNQueryPlanAndSplits(
                "SELECT * FROM test WHERE msg.timestamp > 250 AND msg.timestamp < 290 ORDER BY msg.timestamp DESC LIMIT 10",
                "(msg.timestamp > 250 AND msg.timestamp < 290)",
                "(end_timestamp > 250 AND begin_timestamp < 290)",
                10,
                DESC,
                ImmutableSet.of("3"));

        testTopNQueryPlanAndSplits(
                "SELECT * FROM test WHERE msg.timestamp > 1000 AND msg.timestamp < 1100 ORDER BY msg.timestamp DESC LIMIT 10",
                "(msg.timestamp > 1000 AND msg.timestamp < 1100)",
                "(end_timestamp > 1000 AND begin_timestamp < 1100)",
                10,
                DESC,
                ImmutableSet.of());

        testTopNQueryPlanAndSplits(
                "SELECT * FROM test WHERE msg.timestamp <= 300 ORDER BY msg.timestamp DESC LIMIT 1000",
                "msg.timestamp <= 300",
                "begin_timestamp <= 300",
                1000,
                DESC,
                ImmutableSet.of("0", "1", "2", "3"));

        testTopNQueryPlanAndSplits(
                "SELECT * FROM test WHERE msg.timestamp <= 400 ORDER BY msg.timestamp DESC LIMIT 100",
                "msg.timestamp <= 400",
                "begin_timestamp <= 400",
                100,
                DESC,
                ImmutableSet.of("3", "4"));
    }

    private void testTopNQueryPlanAndSplits(String sql, String kql, String metadataSql, long limit, Order order, Set<String> splitIds)
    {
        TransactionId transactionId = localQueryRunner.getTransactionManager().beginTransaction(false);
        Session session = testSessionBuilder().setCatalog("clp").setSchema("default").setTransactionId(transactionId).build();

        Plan plan = localQueryRunner.createPlan(
                session,
                sql,
                WarningCollector.NOOP);
        ClpComputePushDown optimizer = new ClpComputePushDown(functionAndTypeManager, functionResolution, splitFilterProvider);
        PlanNode optimizedPlan = optimizer.optimize(plan.getRoot(), session.toConnectorSession(), variableAllocator, planNodeIdAllocator);
        PlanNode optimizedPlanWithUniqueId = freshenIds(optimizedPlan, new PlanNodeIdAllocator());

        ClpTableLayoutHandle clpTableLayoutHandle = new ClpTableLayoutHandle(
                table,
                Optional.of(kql),
                Optional.of(metadataSql),
                true,
                Optional.of(new ClpTopNSpec(
                        limit,
                        ImmutableList.of(new ClpTopNSpec.Ordering(ImmutableList.of("begin_timestamp", "end_timestamp"), order)))));

        PlanAssert.assertPlan(
                session,
                localQueryRunner.getMetadata(),
                (node, sourceStats, lookup, s, types) -> PlanNodeStatsEstimate.unknown(),
                new Plan(optimizedPlanWithUniqueId, plan.getTypes(), StatsAndCosts.empty()),
                anyTree(
                        ClpTableScanMatcher.clpTableScanPattern(
                                clpTableLayoutHandle,
                                ImmutableSet.of(
                                        city,
                                        fare,
                                        isHoliday,
                                        new ClpColumnHandle(
                                                "msg",
                                                RowType.from(ImmutableList.of(new RowType.Field(Optional.of("timestamp"), BIGINT))))))));

        assertEquals(
                ImmutableSet.copyOf(splitProvider.listSplits(clpTableLayoutHandle)),
                splitIds.stream()
                        .map(id -> new ClpSplit("/tmp/archives/test/" + id, ARCHIVE, Optional.of(kql)))
                        .collect(ImmutableSet.toImmutableSet()));
    }

    /**
     * Recursively rebuilds a query plan tree so that every {@link PlanNode} has a fresh, unique ID.
     * <p></p>
     * This utility is mainly for testing, to avoid ID collisions that can occur when
     * <code>localQueryRunner.createPlan()</code> and a custom optimizer each use separate
     * {@link PlanNodeIdAllocator}s that start at the same seed, producing duplicate IDs.
     *
     * @param root the root of the plan
     * @param idAlloc the plan node ID allocator
     * @return the plan with a fresh, unique IDs.
     */
    private static PlanNode freshenIds(PlanNode root, PlanNodeIdAllocator idAlloc)
    {
        return SimplePlanRewriter.rewriteWith(new SimplePlanRewriter<Void>() {
            @Override
            public PlanNode visitOutput(OutputNode node, RewriteContext<Void> ctx)
            {
                PlanNode src = ctx.rewrite(node.getSource(), null);
                return new OutputNode(
                        node.getSourceLocation(),
                        idAlloc.getNextId(),
                        src,
                        node.getColumnNames(),
                        node.getOutputVariables());
            }

            @Override
            public PlanNode visitExchange(ExchangeNode node, RewriteContext<Void> ctx)
            {
                List<PlanNode> newSources = node.getSources().stream()
                        .map(s -> ctx.rewrite(s, null))
                        .collect(com.google.common.collect.ImmutableList.toImmutableList());

                return new ExchangeNode(
                        node.getSourceLocation(),
                        idAlloc.getNextId(),
                        node.getType(),
                        node.getScope(),
                        node.getPartitioningScheme(),
                        newSources,
                        node.getInputs(),
                        node.isEnsureSourceOrdering(),
                        node.getOrderingScheme());
            }

            @Override
            public PlanNode visitProject(ProjectNode node, RewriteContext<Void> ctx)
            {
                PlanNode src = ctx.rewrite(node.getSource(), null);
                return new ProjectNode(idAlloc.getNextId(), src, node.getAssignments());
            }

            @Override
            public PlanNode visitFilter(FilterNode node, RewriteContext<Void> ctx)
            {
                PlanNode src = ctx.rewrite(node.getSource(), null);
                return new FilterNode(node.getSourceLocation(), idAlloc.getNextId(), src, node.getPredicate());
            }

            @Override
            public PlanNode visitTopN(TopNNode node, RewriteContext<Void> ctx)
            {
                PlanNode src = ctx.rewrite(node.getSource(), null);
                return new TopNNode(
                        node.getSourceLocation(),
                        idAlloc.getNextId(),
                        src,
                        node.getCount(),
                        node.getOrderingScheme(),
                        node.getStep());
            }

            @Override
            public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> ctx)
            {
                return new TableScanNode(
                        node.getSourceLocation(),
                        idAlloc.getNextId(),
                        node.getTable(),
                        node.getOutputVariables(),
                        node.getAssignments());
            }

            @Override
            public PlanNode visitPlan(PlanNode node, RewriteContext<Void> ctx)
            {
                List<PlanNode> newChildren = node.getSources().stream()
                        .map(ch -> ctx.rewrite(ch, null))
                        .collect(com.google.common.collect.ImmutableList.toImmutableList());
                return node.replaceChildren(newChildren);
            }
        }, root, null);
    }

    private static final class ClpTableScanMatcher
            implements Matcher
    {
        private final ClpTableLayoutHandle expectedLayoutHandle;
        private final Set<ColumnHandle> expectedColumns;

        private ClpTableScanMatcher(ClpTableLayoutHandle expectedLayoutHandle, Set<ColumnHandle> expectedColumns)
        {
            this.expectedLayoutHandle = expectedLayoutHandle;
            this.expectedColumns = expectedColumns;
        }

        static PlanMatchPattern clpTableScanPattern(ClpTableLayoutHandle layoutHandle, Set<ColumnHandle> columns)
        {
            return node(TableScanNode.class).with(new ClpTableScanMatcher(layoutHandle, columns));
        }

        @Override
        public boolean shapeMatches(PlanNode node)
        {
            return node instanceof TableScanNode;
        }

        @Override
        public MatchResult detailMatches(
                PlanNode node,
                StatsProvider stats,
                Session session,
                Metadata metadata,
                SymbolAliases symbolAliases)
        {
            checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false");
            TableScanNode tableScanNode = (TableScanNode) node;
            ClpTableLayoutHandle actualLayoutHandle = (ClpTableLayoutHandle) tableScanNode.getTable().getLayout().get();

            // Check layout handle
            if (!expectedLayoutHandle.equals(actualLayoutHandle)) {
                return NO_MATCH;
            }

            // Check assignments contain expected columns
            Map<VariableReferenceExpression, ColumnHandle> actualAssignments = tableScanNode.getAssignments();
            Set<ColumnHandle> actualColumns = new HashSet<>(actualAssignments.values());

            if (!expectedColumns.equals(actualColumns)) {
                return NO_MATCH;
            }

            SymbolAliases.Builder aliasesBuilder = SymbolAliases.builder();
            for (VariableReferenceExpression variable : tableScanNode.getOutputVariables()) {
                aliasesBuilder.put(variable.getName(), new SymbolReference(variable.getName()));
            }

            return match(aliasesBuilder.build());
        }
    }
}
