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

package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.JoinDistributionType;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.tpcds.TpcdsTableHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.base.Strings;
import com.google.common.base.VerifyException;
import com.google.common.io.Resources;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.ENABLE_SCALAR_FUNCTION_STATS_PROPAGATION;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZER_USE_HISTOGRAMS;
import static com.facebook.presto.spi.plan.JoinDistributionType.REPLICATED;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED;
import static com.facebook.presto.testing.TestngUtils.toDataProvider;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.io.Files.createParentDirs;
import static com.google.common.io.Files.write;
import static com.google.common.io.Resources.getResource;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.isDirectory;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;
import static org.testng.Assert.assertEquals;

public abstract class AbstractCostBasedPlanTest
        extends BasePlanTest
{
    public AbstractCostBasedPlanTest(LocalQueryRunnerSupplier supplier)
    {
        super(supplier);
    }

    protected abstract Stream<String> getQueryResourcePaths();

    @DataProvider
    public Object[][] getQueriesDataProvider()
    {
        return getQueryResourcePaths()
                .collect(toDataProvider());
    }

    @Test(dataProvider = "getQueriesDataProvider")
    public void test(String queryResourcePath)
    {
        assertEquals(generateQueryPlan(read(queryResourcePath)), read(getQueryPlanResourcePath(queryResourcePath)));
    }

    @Test(dataProvider = "getQueriesDataProvider")
    public void scalarFunctionStatsPropagatePlansMatch(String queryResourcePath)
    {
        String sql = read(queryResourcePath);
        Session scalarStatsPropagateSession = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(ENABLE_SCALAR_FUNCTION_STATS_PROPAGATION, "true")
                .build();
        Session baselineSession = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(ENABLE_SCALAR_FUNCTION_STATS_PROPAGATION, "false")
                .build();
        String regularPlan = generateQueryPlan(sql, baselineSession);
        String scalarStatsPropagatePlan = generateQueryPlan(sql, scalarStatsPropagateSession);
        if (!regularPlan.equals(scalarStatsPropagatePlan)) {
            assertEquals(scalarStatsPropagatePlan, read(getScalarFunctionStatsPlanResourcePath(getQueryPlanResourcePath(queryResourcePath))));
        }
    }

    @Test(dataProvider = "getQueriesDataProvider")
    public void histogramsPlansMatch(String queryResourcePath)
    {
        String sql = read(queryResourcePath);
        Session histogramSession = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(OPTIMIZER_USE_HISTOGRAMS, "true")
                .build();
        Session noHistogramSession = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(OPTIMIZER_USE_HISTOGRAMS, "false")
                .build();
        String regularPlan = generateQueryPlan(sql, noHistogramSession);
        String histogramPlan = generateQueryPlan(sql, histogramSession);
        if (!regularPlan.equals(histogramPlan)) {
            assertEquals(histogramPlan, read(getHistogramPlanResourcePath(getQueryPlanResourcePath(queryResourcePath))));
        }
    }

    private String getQueryPlanResourcePath(String queryResourcePath)
    {
        return queryResourcePath.replaceAll("\\.sql$", ".plan.txt");
    }

    private String getHistogramPlanResourcePath(String regularPlanResourcePath)
    {
        Path root = Paths.get(regularPlanResourcePath);
        return root.getParent().resolve("histogram/" + root.getFileName()).toString();
    }

    private String getScalarFunctionStatsPlanResourcePath(String regularPlanResourcePath)
    {
        Path root = Paths.get(regularPlanResourcePath);
        return root.getParent().resolve("scalar_func_stats_propagate/" + root.getFileName()).toString();
    }

    private Path getResourceWritePath(String queryResourcePath)
    {
        return Paths.get(
                getSourcePath().toString(),
                "src/test/resources",
                getQueryPlanResourcePath(queryResourcePath));
    }

    public void generate()
            throws Exception
    {
        initPlanTest();
        try {
            getQueryResourcePaths()
                    .parallel()
                    .forEach(queryResourcePath -> {
                        try {
                            Path queryPlanWritePath = getResourceWritePath(queryResourcePath);
                            createParentDirs(queryPlanWritePath.toFile());
                            Session histogramSession = Session.builder(getQueryRunner().getDefaultSession())
                                    .setSystemProperty(OPTIMIZER_USE_HISTOGRAMS, "true")
                                    .build();
                            Session baselineSession = Session.builder(getQueryRunner().getDefaultSession())
                                    .setSystemProperty(OPTIMIZER_USE_HISTOGRAMS, "false")
                                    .setSystemProperty(ENABLE_SCALAR_FUNCTION_STATS_PROPAGATION, "false")
                                    .build();
                            Session scalarStatsPropagateSession = Session.builder(getQueryRunner().getDefaultSession())
                                    .setSystemProperty(ENABLE_SCALAR_FUNCTION_STATS_PROPAGATION, "true")
                                    .build();
                            String sql = read(queryResourcePath);
                            String regularPlan = generateQueryPlan(sql, baselineSession);
                            String histogramPlan = generateQueryPlan(sql, histogramSession);
                            String scalarStatsPropagatePlan = generateQueryPlan(sql, scalarStatsPropagateSession);
                            write(regularPlan.getBytes(UTF_8), queryPlanWritePath.toFile());
                            // write out the histogram plan if it differs
                            if (!regularPlan.equals(histogramPlan)) {
                                Path histogramPlanWritePath = getResourceWritePath(getHistogramPlanResourcePath(queryResourcePath));
                                createParentDirs(histogramPlanWritePath.toFile());
                                write(histogramPlan.getBytes(UTF_8), histogramPlanWritePath.toFile());
                            }
                            // write out the scalar function stats propagate plan if it differs
                            if (!regularPlan.equals(scalarStatsPropagatePlan)) {
                                Path writePath = getResourceWritePath(getScalarFunctionStatsPlanResourcePath(queryResourcePath));
                                createParentDirs(writePath.toFile());
                                write(scalarStatsPropagatePlan.getBytes(UTF_8), writePath.toFile());
                            }

                            System.out.println("Generated expected plan for query: " + queryResourcePath);
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }
        finally {
            destroyPlanTest();
        }
    }

    private static String read(String resource)
    {
        try {
            return Resources.toString(getResource(AbstractCostBasedPlanTest.class, resource), UTF_8);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String generateQueryPlan(String query)
    {
        return generateQueryPlan(query, getQueryRunner().getDefaultSession());
    }

    private String generateQueryPlan(String query, Session session)
    {
        String sql = query.replaceAll("\\s+;\\s+$", "")
                .replace("${database}.${schema}.", "")
                .replace("\"${database}\".\"${schema}\".\"${prefix}", "\"");
        Plan plan = plan(session, sql, OPTIMIZED_AND_VALIDATED, false);

        JoinOrderPrinter joinOrderPrinter = new JoinOrderPrinter();
        plan.getRoot().accept(joinOrderPrinter, 0);
        return joinOrderPrinter.result();
    }

    private static Path getSourcePath()
    {
        Path workingDir = Paths.get(System.getProperty("user.dir"));
        verify(isDirectory(workingDir), "Working directory is not a directory");
        String topDirectoryName = workingDir.getFileName().toString();
        switch (topDirectoryName) {
            case "presto-benchto-benchmarks":
                return workingDir;
            case "presto":
                return workingDir.resolve("presto-benchto-benchmarks");
            default:
                throw new IllegalStateException("This class must be executed from presto-benchto-benchmarks or presto source directory");
        }
    }

    private static class JoinOrderPrinter
            extends SimplePlanVisitor<Integer>
    {
        private final StringBuilder result = new StringBuilder();

        public String result()
        {
            return result.toString();
        }

        @Override
        public Void visitJoin(JoinNode node, Integer indent)
        {
            JoinDistributionType distributionType = node.getDistributionType()
                    .orElseThrow(() -> new VerifyException("Expected distribution type to be set"));
            if (node.isCrossJoin()) {
                checkState(node.getType() == INNER && distributionType == REPLICATED, "Expected CROSS JOIN to be INNER REPLICATED");
                output(indent, "cross join:");
            }
            else {
                output(indent, "join (%s, %s):", node.getType(), distributionType);
            }

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitExchange(ExchangeNode node, Integer indent)
        {
            Partitioning partitioning = node.getPartitioningScheme().getPartitioning();
            output(
                    indent,
                    "%s exchange (%s, %s, %s)",
                    node.getScope().isRemote() ? "remote" : "local",
                    node.getType(),
                    partitioning.getHandle(),
                    partitioning.getArguments().stream()
                            .map(Object::toString)
                            .sorted() // Currently, order of hash columns is not deterministic
                            .collect(joining(", ", "[", "]")));

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitAggregation(AggregationNode node, Integer indent)
        {
            output(
                    indent,
                    "%s aggregation over (%s)",
                    node.getStep().name().toLowerCase(ENGLISH),
                    node.getGroupingKeys().stream()
                            .map(Object::toString)
                            .sorted()
                            .collect(joining(", ")));

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitTableScan(TableScanNode node, Integer indent)
        {
            ConnectorTableHandle connectorTableHandle = node.getTable().getConnectorHandle();
            if (connectorTableHandle instanceof TpcdsTableHandle) {
                output(indent, "scan %s", ((TpcdsTableHandle) connectorTableHandle).getTableName());
            }
            else if (connectorTableHandle instanceof TpchTableHandle) {
                output(indent, "scan %s", ((TpchTableHandle) connectorTableHandle).getTableName());
            }
            else {
                throw new IllegalStateException(format("Unexpected ConnectorTableHandle: %s", connectorTableHandle.getClass()));
            }

            return null;
        }

        @Override
        public Void visitSemiJoin(final SemiJoinNode node, Integer indent)
        {
            output(indent, "semijoin (%s):", node.getDistributionType().get());

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitValues(ValuesNode node, Integer indent)
        {
            output(indent, "values (%s rows)", node.getRows().size());

            return null;
        }

        private void output(int indent, String message, Object... args)
        {
            String formattedMessage = format(message, args);
            result.append(format("%s%s%n", Strings.repeat("    ", indent), formattedMessage));
        }
    }
}
