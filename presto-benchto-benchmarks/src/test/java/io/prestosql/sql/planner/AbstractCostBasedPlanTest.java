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

package io.prestosql.sql.planner;

import com.google.common.base.Strings;
import com.google.common.base.VerifyException;
import com.google.common.io.Resources;
import io.prestosql.plugin.tpcds.TpcdsTableHandle;
import io.prestosql.plugin.tpch.TpchTableHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.planner.plan.ValuesNode;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.io.Files.createParentDirs;
import static com.google.common.io.Files.write;
import static com.google.common.io.Resources.getResource;
import static io.prestosql.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
import static io.prestosql.testing.TestngUtils.toDataProvider;
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

    private String getQueryPlanResourcePath(String queryResourcePath)
    {
        return queryResourcePath.replaceAll("\\.sql$", ".plan.txt");
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
                            Path queryPlanWritePath = Paths.get(
                                    getSourcePath().toString(),
                                    "src/test/resources",
                                    getQueryPlanResourcePath(queryResourcePath));
                            createParentDirs(queryPlanWritePath.toFile());
                            write(generateQueryPlan(read(queryResourcePath)).getBytes(UTF_8), queryPlanWritePath.toFile());
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
        String sql = query.replaceAll("\\s+;\\s+$", "")
                .replace("${database}.${schema}.", "")
                .replace("\"${database}\".\"${schema}\".\"${prefix}", "\"");
        Plan plan = plan(sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, false);

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
            JoinNode.DistributionType distributionType = node.getDistributionType()
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
                    node.getScope().name().toLowerCase(ENGLISH),
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
            result.append(format("%s%s\n", Strings.repeat("    ", indent), formattedMessage));
        }
    }
}
