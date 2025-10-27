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

import com.facebook.presto.Session;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.plugin.clp.optimization.ClpComputePushDown;
import com.facebook.presto.plugin.clp.optimization.ClpUdfRewriter;
import com.facebook.presto.plugin.clp.split.filter.ClpMySqlSplitFilterProvider;
import com.facebook.presto.plugin.clp.split.filter.ClpSplitFilterProvider;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
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

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.Utils.checkState;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.ARCHIVES_STORAGE_DIRECTORY_BASE;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.METADATA_DB_PASSWORD;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.METADATA_DB_TABLE_PREFIX;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.METADATA_DB_URL_TEMPLATE;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.METADATA_DB_USER;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.getDbHandle;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.setupMetadata;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.Boolean;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.ClpString;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.Float;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.Integer;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.VarString;
import static com.facebook.presto.plugin.clp.optimization.ClpUdfRewriter.JSON_STRING_PLACEHOLDER;
import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

@Test(singleThreaded = true)
public class TestClpUdfRewriter
        extends TestClpQueryBase
{
    private final Session defaultSession = testSessionBuilder()
            .setCatalog("clp")
            .setSchema(ClpMetadata.DEFAULT_SCHEMA_NAME)
            .build();

    private ClpMetadataDbSetUp.DbHandle dbHandle;
    ClpTableHandle table;

    private LocalQueryRunner localQueryRunner;
    private FunctionAndTypeManager functionAndTypeManager;
    private FunctionResolution functionResolution;
    private ClpSplitFilterProvider splitFilterProvider;
    private PlanNodeIdAllocator planNodeIdAllocator;
    private VariableAllocator variableAllocator;

    @BeforeMethod
    public void setUp()
    {
        dbHandle = getDbHandle("metadata_query_testdb");
        final String tableName = "test";
        final String tablePath = ARCHIVES_STORAGE_DIRECTORY_BASE + tableName;
        table = new ClpTableHandle(new SchemaTableName("default", tableName), tablePath);

        setupMetadata(dbHandle,
                ImmutableMap.of(
                        tableName,
                        ImmutableList.of(
                                new Pair<>("city.Name", ClpString),
                                new Pair<>("city.Region.Id", Integer),
                                new Pair<>("city.Region.Name", VarString),
                                new Pair<>("fare", Float),
                                new Pair<>("isHoliday", Boolean))));

        localQueryRunner = new LocalQueryRunner(defaultSession);
        localQueryRunner.createCatalog("clp", new ClpConnectorFactory(), ImmutableMap.of(
                "clp.metadata-db-url", format(METADATA_DB_URL_TEMPLATE, dbHandle.getDbPath()),
                "clp.metadata-db-user", METADATA_DB_USER,
                "clp.metadata-db-password", METADATA_DB_PASSWORD,
                "clp.metadata-table-prefix", METADATA_DB_TABLE_PREFIX));
        localQueryRunner.getMetadata().registerBuiltInFunctions(extractFunctions(new ClpPlugin().getFunctions()));
        functionAndTypeManager = localQueryRunner.getMetadata().getFunctionAndTypeManager();
        functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
        splitFilterProvider = new ClpMySqlSplitFilterProvider(new ClpConfig());
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
    public void testClpGetScanFilter()
    {
        TransactionId transactionId = localQueryRunner.getTransactionManager().beginTransaction(false);
        Session session = testSessionBuilder().setCatalog("clp").setSchema("default").setTransactionId(transactionId).build();

        Plan plan = localQueryRunner.createPlan(
                session,
                "SELECT * FROM test WHERE CLP_GET_BIGINT('user_id') = 0 AND CLP_GET_DOUBLE('fare') < 50.0 AND CLP_GET_STRING('city') = 'SF' AND " +
                        "CLP_GET_BOOL('isHoliday') = true AND cardinality(CLP_GET_STRING_ARRAY('tags')) > 0 AND LOWER(city.Name) = 'beijing'",
                WarningCollector.NOOP);
        ClpUdfRewriter udfRewriter = new ClpUdfRewriter(functionAndTypeManager);
        PlanNode optimizedPlan = udfRewriter.optimize(plan.getRoot(), session.toConnectorSession(), variableAllocator, planNodeIdAllocator);
        ClpComputePushDown optimizer = new ClpComputePushDown(functionAndTypeManager, functionResolution, splitFilterProvider);
        optimizedPlan = optimizer.optimize(optimizedPlan, session.toConnectorSession(), variableAllocator, planNodeIdAllocator);

        PlanAssert.assertPlan(
                session,
                localQueryRunner.getMetadata(),
                (node, sourceStats, lookup, s, types) -> PlanNodeStatsEstimate.unknown(),
                new Plan(optimizedPlan, plan.getTypes(), StatsAndCosts.empty()),
                anyTree(
                        filter(
                                expression("lower(city.Name) = 'beijing' AND cardinality(tags) > 0"),
                                ClpTableScanMatcher.clpTableScanPattern(
                                        new ClpTableLayoutHandle(
                                                table,
                                                Optional.of(
                                                        "(((user_id: 0 AND fare < 50.0) AND (city: \"SF\" AND isHoliday: true)))"),
                                                Optional.empty()),
                                        ImmutableSet.of(
                                                city,
                                                fare,
                                                isHoliday,
                                                new ClpColumnHandle("user_id", BIGINT),
                                                new ClpColumnHandle("city", VARCHAR),
                                                new ClpColumnHandle("tags", new ArrayType(VARCHAR)))))));
    }

    @Test
    public void testClpGetScanProject()
    {
        TransactionId transactionId = localQueryRunner.getTransactionManager().beginTransaction(false);
        Session session = testSessionBuilder().setCatalog("clp").setSchema("default").setTransactionId(transactionId).build();

        Plan plan = localQueryRunner.createPlan(
                session,
                "SELECT CLP_GET_BIGINT('user_id'), CLP_GET_DOUBLE('fare'), CLP_GET_STRING('user'), " +
                        "CLP_GET_BOOL('isHoliday'), CLP_GET_STRING_ARRAY('tags'), city.Name FROM test",
                WarningCollector.NOOP);
        ClpUdfRewriter udfRewriter = new ClpUdfRewriter(functionAndTypeManager);
        PlanNode optimizedPlan = udfRewriter.optimize(plan.getRoot(), session.toConnectorSession(), variableAllocator, planNodeIdAllocator);
        ClpComputePushDown optimizer = new ClpComputePushDown(functionAndTypeManager, functionResolution, splitFilterProvider);
        optimizedPlan = optimizer.optimize(optimizedPlan, session.toConnectorSession(), variableAllocator, planNodeIdAllocator);

        PlanAssert.assertPlan(
                session,
                localQueryRunner.getMetadata(),
                (node, sourceStats, lookup, s, types) -> PlanNodeStatsEstimate.unknown(),
                new Plan(optimizedPlan, plan.getTypes(), StatsAndCosts.empty()),
                anyTree(
                        project(
                                ImmutableMap.of(
                                        "clp_get_bigint",
                                        PlanMatchPattern.expression("user_und_id"),
                                        "clp_get_double",
                                        PlanMatchPattern.expression("fare"),
                                        "clp_get_string",
                                        PlanMatchPattern.expression("user"),
                                        "clp_get_bool",
                                        PlanMatchPattern.expression("is_uxholiday"),
                                        "clp_get_string_array",
                                        PlanMatchPattern.expression("tags"),
                                        "expr",
                                        PlanMatchPattern.expression("city.Name")),
                                ClpTableScanMatcher.clpTableScanPattern(
                                        new ClpTableLayoutHandle(
                                                table,
                                                Optional.empty(),
                                                Optional.empty()),
                                        ImmutableSet.of(
                                                new ClpColumnHandle("user_id", BIGINT),
                                                new ClpColumnHandle("fare", DOUBLE),
                                                new ClpColumnHandle("user", VARCHAR),
                                                isHoliday,
                                                new ClpColumnHandle("tags", new ArrayType(VARCHAR)),
                                                city)))));
    }

    @Test
    public void testClpGetScanProjectFilter()
    {
        TransactionId transactionId = localQueryRunner.getTransactionManager().beginTransaction(false);
        Session session = testSessionBuilder().setCatalog("clp").setSchema("default").setTransactionId(transactionId).build();

        Plan plan = localQueryRunner.createPlan(
                session,
                "SELECT LOWER(city.Name), LOWER(CLP_GET_STRING('city.Name')) from test WHERE CLP_GET_BIGINT('user_id') = 0 AND LOWER(city.Name) = 'beijing'",
                WarningCollector.NOOP);
        ClpUdfRewriter udfRewriter = new ClpUdfRewriter(functionAndTypeManager);
        PlanNode optimizedPlan = udfRewriter.optimize(plan.getRoot(), session.toConnectorSession(), variableAllocator, planNodeIdAllocator);
        ClpComputePushDown optimizer = new ClpComputePushDown(functionAndTypeManager, functionResolution, splitFilterProvider);
        optimizedPlan = optimizer.optimize(optimizedPlan, session.toConnectorSession(), variableAllocator, planNodeIdAllocator);

        PlanAssert.assertPlan(
                session,
                localQueryRunner.getMetadata(),
                (node, sourceStats, lookup, s, types) -> PlanNodeStatsEstimate.unknown(),
                new Plan(optimizedPlan, plan.getTypes(), StatsAndCosts.empty()),
                anyTree(
                        project(
                                ImmutableMap.of(
                                        "lower",
                                        PlanMatchPattern.expression("lower(city.Name)"),
                                        "lower_0",
                                        PlanMatchPattern.expression("lower(city_dot__uxname)")),
                                filter(
                                        expression("lower(city.Name) = 'beijing'"),
                                        ClpTableScanMatcher.clpTableScanPattern(
                                                new ClpTableLayoutHandle(table, Optional.of("(user_id: 0)"), Optional.empty()),
                                                ImmutableSet.of(
                                                        new ClpColumnHandle("city.Name", VARCHAR),
                                                        new ClpColumnHandle("user_id", BIGINT),
                                                        city))))));
    }

    @Test
    public void testClpGetJsonString()
    {
        TransactionId transactionId = localQueryRunner.getTransactionManager().beginTransaction(false);
        Session session = testSessionBuilder().setCatalog("clp").setSchema("default").setTransactionId(transactionId).build();

        Plan plan = localQueryRunner.createPlan(
                session,
                "SELECT CLP_GET_JSON_STRING() from test WHERE CLP_GET_BIGINT('user_id') = 0",
                WarningCollector.NOOP);
        ClpUdfRewriter udfRewriter = new ClpUdfRewriter(functionAndTypeManager);
        PlanNode optimizedPlan = udfRewriter.optimize(plan.getRoot(), session.toConnectorSession(), variableAllocator, planNodeIdAllocator);
        ClpComputePushDown optimizer = new ClpComputePushDown(functionAndTypeManager, functionResolution, splitFilterProvider);
        optimizedPlan = optimizer.optimize(optimizedPlan, session.toConnectorSession(), variableAllocator, planNodeIdAllocator);

        PlanAssert.assertPlan(
                session,
                localQueryRunner.getMetadata(),
                (node, sourceStats, lookup, s, types) -> PlanNodeStatsEstimate.unknown(),
                new Plan(optimizedPlan, plan.getTypes(), StatsAndCosts.empty()),
                anyTree(
                        project(
                                ImmutableMap.of(
                                        "clp_get_json_string",
                                        PlanMatchPattern.expression(JSON_STRING_PLACEHOLDER)),
                                ClpTableScanMatcher.clpTableScanPattern(
                                        new ClpTableLayoutHandle(table, Optional.of("user_id: 0"), Optional.empty()),
                                        ImmutableSet.of(
                                                new ClpColumnHandle("user_id", BIGINT),
                                                new ClpColumnHandle(JSON_STRING_PLACEHOLDER, VARCHAR))))));
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
