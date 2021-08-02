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
package com.facebook.presto.sql.planner.planPrinter;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.operator.StageExecutionDescriptor;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.testing.TestingHandle;
import com.facebook.presto.testing.TestingMetadata;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.AbstractMockMetadata.dummyMetadata;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertTrue;

public class TestPlanPrinter
{
    private static final PlanBuilder PLAN_BUILDER = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), dummyMetadata());
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = createTestFunctionAndTypeManager();
    private static final VariableReferenceExpression COLUMN_VARIABLE = new VariableReferenceExpression("column", VARCHAR);
    private static final ColumnHandle COLUMN_HANDLE = new TestingMetadata.TestingColumnHandle("column");
    private static final TableHandle TABLE_HANDLE_WITH_LAYOUT = new TableHandle(
            new ConnectorId("testConnector"),
            new TestingMetadata.TestingTableHandle(),
            TestingTransactionHandle.create(),
            Optional.of(TestingHandle.INSTANCE));

    // Creates a trivial TableScan with the given domain on some column
    private String domainToPrintedScan(VariableReferenceExpression variable, ColumnHandle colHandle, Domain domain)
    {
        TupleDomain<ColumnHandle> tupleDomain = withColumnDomains(ImmutableMap.<ColumnHandle, Domain>builder().put(colHandle, domain).build());

        TableScanNode scanNode = PLAN_BUILDER.tableScan(
                TABLE_HANDLE_WITH_LAYOUT,
                ImmutableList.of(variable),
                ImmutableMap.of(variable, colHandle),
                tupleDomain,
                tupleDomain);

        PlanFragment testFragment = new PlanFragment(
                new PlanFragmentId(0),
                scanNode,
                ImmutableSet.of(variable),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(scanNode.getId()),
                new PartitioningScheme(Partitioning.create(SOURCE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(variable)),
                StageExecutionDescriptor.ungroupedExecution(),
                false,
                StatsAndCosts.empty(),
                Optional.empty());

        return PlanPrinter.textPlanFragment(testFragment, FUNCTION_AND_TYPE_MANAGER, TEST_SESSION, false);
    }

    // Asserts that the PlanPrinter output for this domain is what was expected
    private void assertDomainFormat(VariableReferenceExpression variable, ColumnHandle colHandle, Domain domain, String expected)
    {
        String printed = domainToPrintedScan(variable, colHandle, domain);
        assertTrue(printed.contains(":: " + expected));
    }

    private void assertDomainFormat(Domain domain, String expected)
    {
        assertDomainFormat(COLUMN_VARIABLE, COLUMN_HANDLE, domain, expected);
    }

    @Test
    public void testDomainTextFormatting()
    {
        assertDomainFormat(
                Domain.create(
                        ValueSet.all(VARCHAR),
                        false),
                "[(<min>, <max>)]");

        assertDomainFormat(
                Domain.create(
                        ValueSet.of(VARCHAR, utf8Slice("some string")),
                        false),
                "[[\"some string\"]]");

        assertDomainFormat(
                Domain.create(
                        ValueSet.of(VARCHAR, utf8Slice("here's a quote: \"")),
                        false),
                "[[\"here's a quote: \\\"\"]]");

        assertDomainFormat(
                Domain.create(
                        ValueSet.of(VARCHAR, utf8Slice("")),
                        false),
                "[[\"\"]]");

        assertDomainFormat(
                Domain.create(
                        ValueSet.ofRanges(
                                Range.greaterThanOrEqual(VARCHAR, utf8Slice("string with \"quotes\" inside"))
                                        .intersect(Range.lessThanOrEqual(VARCHAR, utf8Slice("this string's quote is here -> \"")))),
                        false),
                "[[\"string with \\\"quotes\\\" inside\", \"this string's quote is here -> \\\"\"]]");

        assertDomainFormat(
                Domain.create(
                        ValueSet.ofRanges(
                                Range.greaterThan(VARCHAR, utf8Slice("string with \"quotes\" inside"))
                                        .intersect(Range.lessThan(VARCHAR, utf8Slice("this string's quote is here -> \"")))),
                        false),
                "[(\"string with \\\"quotes\\\" inside\", \"this string's quote is here -> \\\"\")]");

        assertDomainFormat(
                Domain.create(
                        ValueSet.of(VARCHAR, utf8Slice("<min>")),
                        false),
                "[[\"<min>\"]]");

        assertDomainFormat(
                Domain.create(
                        ValueSet.of(VARCHAR, utf8Slice("<max>")),
                        false),
                "[[\"<max>\"]]");

        assertDomainFormat(
                Domain.create(
                        ValueSet.of(VARCHAR, utf8Slice("<min>, <max>")),
                        false),
                "[[\"<min>, <max>\"]]");

        assertDomainFormat(
                Domain.create(
                        ValueSet.ofRanges(
                                Range.greaterThanOrEqual(VARCHAR, utf8Slice("a"))
                                        .intersect(Range.lessThanOrEqual(VARCHAR, utf8Slice("b")))),
                        false),
                "[[\"a\", \"b\"]]");

        assertDomainFormat(
                Domain.create(
                        ValueSet.of(VARCHAR, utf8Slice("a, b")),
                        false),
                "[[\"a, b\"]]");

        assertDomainFormat(
                Domain.create(
                        ValueSet.of(VARCHAR, utf8Slice("xyz")),
                        true),
                "[NULL, [\"xyz\"]]");

        assertDomainFormat(
                Domain.onlyNull(VARCHAR),
                "[NULL]");
    }
}
