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

package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TestingColumnHandle;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.DoubleRange;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestConnectorFilterStatsCalculatorService
{
    private Session session;
    private ConnectorFilterStatsCalculatorService statsCalculatorService;
    private ColumnHandle xColumn = new TestingColumnHandle("x");
    private ColumnStatistics xStats;
    private TableStatistics originalTableStatistics;
    private TableStatistics originalTableStatisticsWithoutTotalSize;
    private TableStatistics zeroTableStatistics;
    private TypeProvider standardTypes;
    private TestingRowExpressionTranslator translator;

    @BeforeClass
    public void setUp()
    {
        session = testSessionBuilder().build();
        MetadataManager metadata = MetadataManager.createTestMetadataManager();
        FilterStatsCalculator statsCalculator = new FilterStatsCalculator(metadata, new ScalarStatsCalculator(metadata), new StatsNormalizer());
        statsCalculatorService = new ConnectorFilterStatsCalculatorService(statsCalculator);
        xStats = ColumnStatistics.builder()
                .setDistinctValuesCount(Estimate.of(40))
                .setRange(new DoubleRange(-10, 10))
                .setNullsFraction(Estimate.of(0.25))
                .build();
        zeroTableStatistics = TableStatistics.builder()
                .setRowCount(Estimate.zero())
                .setTotalSize(Estimate.zero())
                .build();
        originalTableStatistics = TableStatistics.builder()
                .setRowCount(Estimate.of(100))
                .setTotalSize(Estimate.of(800))
                .setColumnStatistics(xColumn, xStats)
                .build();
        originalTableStatisticsWithoutTotalSize = TableStatistics.builder()
                .setRowCount(Estimate.of(100))
                .setColumnStatistics(xColumn, xStats)
                .build();
        standardTypes = TypeProvider.fromVariables(ImmutableList.<VariableReferenceExpression>builder()
                .add(new VariableReferenceExpression("x", DOUBLE))
                .build());
        translator = new TestingRowExpressionTranslator(MetadataManager.createTestMetadataManager());
    }

    @Test
    public void testTableStatisticsAfterFilter()
    {
        // totalSize always be zero
        assertPredicate("true", zeroTableStatistics, zeroTableStatistics);
        assertPredicate("x < 3e0", zeroTableStatistics, zeroTableStatistics);
        assertPredicate("false", zeroTableStatistics, zeroTableStatistics);

        // rowCount and totalSize all NaN
        assertPredicate("true", TableStatistics.empty(), TableStatistics.empty());
        // rowCount and totalSize from NaN to 0.0
        assertPredicate("false", TableStatistics.empty(), TableStatistics.builder().setRowCount(Estimate.zero()).setTotalSize(Estimate.zero()).build());

        TableStatistics filteredToZeroStatistics = TableStatistics.builder()
                .setRowCount(Estimate.zero())
                .setTotalSize(Estimate.zero())
                .setColumnStatistics(xColumn, new ColumnStatistics(Estimate.of(1.0), Estimate.zero(), Estimate.zero(), Optional.empty()))
                .build();
        assertPredicate("false", originalTableStatistics, filteredToZeroStatistics);

        TableStatistics filteredStatistics = TableStatistics.builder()
                .setRowCount(Estimate.of(37.5))
                .setTotalSize(Estimate.of(300))
                .setColumnStatistics(xColumn, new ColumnStatistics(Estimate.zero(), Estimate.of(20), Estimate.unknown(), Optional.of(new DoubleRange(-10, 0))))
                .build();
        assertPredicate("x < 0", originalTableStatistics, filteredStatistics);

        TableStatistics filteredStatisticsWithoutTotalSize = TableStatistics.builder()
                .setRowCount(Estimate.of(37.5))
                .setColumnStatistics(xColumn, new ColumnStatistics(Estimate.zero(), Estimate.of(20), Estimate.unknown(), Optional.of(new DoubleRange(-10, 0))))
                .build();
        assertPredicate("x < 0", originalTableStatisticsWithoutTotalSize, filteredStatisticsWithoutTotalSize);
    }

    private void assertPredicate(String filterExpression, TableStatistics tableStatistics, TableStatistics expectedStatistics)
    {
        assertPredicate(expression(filterExpression), tableStatistics, expectedStatistics);
    }

    private void assertPredicate(Expression filterExpression, TableStatistics tableStatistics, TableStatistics expectedStatistics)
    {
        RowExpression predicate = translator.translateAndOptimize(filterExpression, standardTypes);
        TableStatistics filteredStatistics = statsCalculatorService.filterStats(tableStatistics, predicate, session.toConnectorSession(),
                ImmutableMap.of(xColumn, "x"), ImmutableMap.of("x", DOUBLE));
        assertEquals(filteredStatistics, expectedStatistics);
    }
}
