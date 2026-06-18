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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.sanity.TypeValidator;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.plan.AggregationNode.Step.INTERMEDIATE;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.spi.plan.WindowNode.Frame.BoundType.UNBOUNDED_FOLLOWING;
import static com.facebook.presto.spi.plan.WindowNode.Frame.BoundType.UNBOUNDED_PRECEDING;
import static com.facebook.presto.spi.plan.WindowNode.Frame.WindowType.RANGE;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.PlannerUtils.newVariable;
import static com.facebook.presto.sql.relational.Expressions.call;

public class TestTypeValidator
{
    private static final TableHandle TEST_TABLE_HANDLE = new TableHandle(
            new ConnectorId("test"),
            new TestingTableHandle(),
            TestingTransactionHandle.create(),
            Optional.empty());
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final TypeValidator TYPE_VALIDATOR = new TypeValidator();
    private static final FunctionAndTypeManager FUNCTION_MANAGER = createTestMetadataManager().getFunctionAndTypeManager();
    private static final FunctionHandle SUM = FUNCTION_MANAGER.lookupFunction("sum", fromTypes(DOUBLE));
    private static final FunctionHandle APPROX_PERCENTILE = FUNCTION_MANAGER.lookupFunction("approx_percentile", fromTypes(DOUBLE, DOUBLE));
    private static final TestingRowExpressionTranslator translator = new TestingRowExpressionTranslator();

    private VariableAllocator variableAllocator;
    private TableScanNode baseTableScan;
    private VariableReferenceExpression variableA;
    private VariableReferenceExpression variableB;
    private VariableReferenceExpression variableC;
    private VariableReferenceExpression variableD;
    private VariableReferenceExpression variableE;
    private VariableReferenceExpression variableC5;

    @BeforeClass
    public void setUp()
    {
        variableAllocator = new VariableAllocator();
        variableA = variableAllocator.newVariable("a", BIGINT);
        variableB = variableAllocator.newVariable("b", INTEGER);
        variableC = variableAllocator.newVariable("c", DOUBLE);
        variableD = variableAllocator.newVariable("d", DATE);
        variableE = variableAllocator.newVariable("e", VarcharType.createVarcharType(3));  // varchar(3), to test type only coercion
        variableC5 = variableAllocator.newVariable("c_5", VARBINARY);

        Map<VariableReferenceExpression, ColumnHandle> assignments = ImmutableMap.<VariableReferenceExpression, ColumnHandle>builder()
                .put(variableA, new TestingColumnHandle("a"))
                .put(variableB, new TestingColumnHandle("b"))
                .put(variableC, new TestingColumnHandle("c"))
                .put(variableD, new TestingColumnHandle("d"))
                .put(variableE, new TestingColumnHandle("e"))
                .build();

        baseTableScan = new TableScanNode(
                Optional.empty(),
                newId(),
                TEST_TABLE_HANDLE,
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.all(),
                TupleDomain.all(), Optional.empty());
    }

    @Test
    public void testValidProject()
    {
        Expression expression1 = new Cast(new SymbolReference(variableB.getName()), StandardTypes.BIGINT);
        Expression expression2 = new Cast(new SymbolReference(variableC.getName()), StandardTypes.BIGINT);
        Assignments assignments = Assignments.builder()
                .put(newVariable(variableAllocator, expression1, BIGINT), translator.translate(expression1, TypeProvider.fromVariables(ImmutableList.of(variableB))))
                .put(newVariable(variableAllocator, expression2, BIGINT), translator.translate(expression2, TypeProvider.fromVariables(ImmutableList.of(variableC))))
                .build();
        PlanNode node = new ProjectNode(
                newId(),
                baseTableScan,
                assignments);

        assertTypesValid(node);
    }

    @Test
    public void testValidUnion()
    {
        VariableReferenceExpression output = variableAllocator.newVariable("output", DATE);

        PlanNode node = new UnionNode(
                Optional.empty(),
                newId(),
                ImmutableList.of(baseTableScan, baseTableScan),
                ImmutableList.of(output),
                ImmutableMap.of(output, ImmutableList.of(variableD, variableD)));

        assertTypesValid(node);
    }

    @Test
    public void testValidWindow()
    {
        VariableReferenceExpression windowVariable = variableAllocator.newVariable("sum", DOUBLE);
        FunctionHandle functionHandle = FUNCTION_MANAGER.lookupFunction("sum", fromTypes(DOUBLE));

        WindowNode.Frame frame = new WindowNode.Frame(
                RANGE,
                UNBOUNDED_PRECEDING,
                Optional.empty(),
                Optional.empty(),
                UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        WindowNode.Function function = new WindowNode.Function(call("sum", functionHandle, DOUBLE, variableC), frame, false);

        DataOrganizationSpecification specification = new DataOrganizationSpecification(ImmutableList.of(), Optional.empty());

        PlanNode node = new WindowNode(
                Optional.empty(),
                newId(),
                baseTableScan,
                specification,
                ImmutableMap.of(windowVariable, function),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        assertTypesValid(node);
    }

    @Test
    public void testValidAggregation()
    {
        VariableReferenceExpression aggregationVariable = variableAllocator.newVariable("sum", DOUBLE);

        PlanNode node = new AggregationNode(
                Optional.empty(),
                newId(),
                baseTableScan,
                ImmutableMap.of(aggregationVariable, new Aggregation(
                        new CallExpression("sum",
                                SUM,
                                DOUBLE,
                                ImmutableList.of(variableC)),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(variableA, variableB)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        assertTypesValid(node);
    }

    @Test
    public void testValidIntermediateAggregation()
    {
        VariableReferenceExpression aggregationVariable = variableAllocator.newVariable("approx_percentile", VARBINARY);

        PlanNode node = new AggregationNode(
                Optional.empty(),
                newId(),
                baseTableScan,
                ImmutableMap.of(aggregationVariable, new Aggregation(
                        new CallExpression("approx_percentile",
                                APPROX_PERCENTILE,
                                VARBINARY,
                                ImmutableList.of(variableC5)),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of()),
                ImmutableList.of(),
                INTERMEDIATE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        assertTypesValid(node);
    }

    @Test
    public void testValidPartialAggregation()
    {
        VariableReferenceExpression aggregationVariable = variableAllocator.newVariable("approx_percentile", VARBINARY);

        PlanNode node = new AggregationNode(
                Optional.empty(),
                newId(),
                baseTableScan,
                ImmutableMap.of(aggregationVariable, new Aggregation(
                        new CallExpression("approx_percentile",
                                APPROX_PERCENTILE,
                                VARBINARY,
                                ImmutableList.of(variableC)),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of()),
                ImmutableList.of(),
                PARTIAL,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        assertTypesValid(node);
    }

    @Test
    public void testValidTypeOnlyCoercion()
    {
        Expression expression = new Cast(new SymbolReference(variableB.getName()), StandardTypes.BIGINT);
        Assignments assignments = Assignments.builder()
                .put(newVariable(variableAllocator, expression, BIGINT), translator.translate(expression, TypeProvider.fromVariables(ImmutableList.of(variableB))))
                .put(newVariable(variableAllocator, new SymbolReference(variableE.getName()), VARCHAR), variableE) // implicit coercion from varchar(3) to varchar
                .build();
        PlanNode node = new ProjectNode(newId(), baseTableScan, assignments);

        assertTypesValid(node);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "type of variable 'expr(_[0-9]+)?' is expected to be bigint, but the actual type is integer")
    public void testInvalidProject()
    {
        Expression expression1 = new Cast(new SymbolReference(variableB.getName()), StandardTypes.INTEGER);
        Expression expression2 = new Cast(new SymbolReference(variableA.getName()), StandardTypes.INTEGER);
        Assignments assignments = Assignments.builder()
                .put(newVariable(variableAllocator, expression1, BIGINT), translator.translate(expression1, TypeProvider.fromVariables(ImmutableList.of(variableB)))) // should be INTEGER
                .put(newVariable(variableAllocator, expression1, INTEGER), translator.translate(expression2, TypeProvider.fromVariables(ImmutableList.of(variableA))))
                .build();
        PlanNode node = new ProjectNode(
                newId(),
                baseTableScan,
                assignments);

        assertTypesValid(node);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Return type for intermediate aggregation must be the same as the type of its single argument: expected 'varbinary', got 'double'")
    public void testInvalidIntermediateAggregationReturnType()
    {
        VariableReferenceExpression aggregationVariable = variableAllocator.newVariable("approx_percentile", VARBINARY);

        PlanNode node = new AggregationNode(
                Optional.empty(),
                newId(),
                baseTableScan,
                ImmutableMap.of(aggregationVariable, new Aggregation(
                        new CallExpression("approx_percentile",
                                APPROX_PERCENTILE,
                                DOUBLE, // Should be VARBINARY
                                ImmutableList.of(variableC5)),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of()),
                ImmutableList.of(),
                INTERMEDIATE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        assertTypesValid(node);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "type of variable 'approx_pct_part_invalid' is expected to be varbinary, but the actual type is double")
    public void testInvalidPartialAggregationReturnType()
    {
        VariableReferenceExpression aggregationVariable = variableAllocator.newVariable("approx_pct_part_invalid", VARBINARY);

        PlanNode node = new AggregationNode(
                Optional.empty(),
                newId(),
                baseTableScan,
                ImmutableMap.of(aggregationVariable, new Aggregation(
                        new CallExpression("approx_percentile",
                                APPROX_PERCENTILE,
                                DOUBLE, // Should be VARBINARY
                                ImmutableList.of(variableC)),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of()),
                ImmutableList.of(),
                PARTIAL,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        assertTypesValid(node);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Expected input types are \\[double\\] but getting \\[bigint\\]")
    public void testInvalidAggregationFunctionCall()
    {
        VariableReferenceExpression aggregationVariable = variableAllocator.newVariable("sum", DOUBLE);

        PlanNode node = new AggregationNode(
                Optional.empty(),
                newId(),
                baseTableScan,
                ImmutableMap.of(aggregationVariable, new Aggregation(
                        new CallExpression(
                                "sum",
                                SUM,
                                DOUBLE,
                                ImmutableList.of(variableA)),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(variableA, variableB)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        assertTypesValid(node);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "type of variable 'sum(_[0-9]+)?' is expected to be double, but the actual type is bigint")
    public void testInvalidAggregationFunctionSignature()
    {
        VariableReferenceExpression aggregationVariable = variableAllocator.newVariable("sum", DOUBLE);

        PlanNode node = new AggregationNode(
                Optional.empty(),
                newId(),
                baseTableScan,
                ImmutableMap.of(aggregationVariable, new Aggregation(
                        new CallExpression(
                                "sum",
                                FUNCTION_MANAGER.lookupFunction("sum", fromTypes(BIGINT)), // should be DOUBLE
                                DOUBLE,
                                ImmutableList.of(variableC)),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(variableA, variableB)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        assertTypesValid(node);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "type of variable 'sum(_[0-9]+)?' is expected to be double, but the actual type is bigint")
    public void testInvalidWindowFunctionCall()
    {
        VariableReferenceExpression windowVariable = variableAllocator.newVariable("sum", DOUBLE);
        FunctionHandle functionHandle = FUNCTION_MANAGER.lookupFunction("sum", fromTypes(DOUBLE));

        WindowNode.Frame frame = new WindowNode.Frame(
                RANGE,
                UNBOUNDED_PRECEDING,
                Optional.empty(),
                Optional.empty(),
                UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        WindowNode.Function function = new WindowNode.Function(call("sum", functionHandle, BIGINT, variableA), frame, false);

        DataOrganizationSpecification specification = new DataOrganizationSpecification(ImmutableList.of(), Optional.empty());

        PlanNode node = new WindowNode(
                Optional.empty(),
                newId(),
                baseTableScan,
                specification,
                ImmutableMap.of(windowVariable, function),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        assertTypesValid(node);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "type of variable 'sum(_[0-9]+)?' is expected to be double, but the actual type is bigint")
    public void testInvalidWindowFunctionSignature()
    {
        VariableReferenceExpression windowVariable = variableAllocator.newVariable("sum", DOUBLE);
        FunctionHandle functionHandle = FUNCTION_MANAGER.lookupFunction("sum", fromTypes(BIGINT)); // should be DOUBLE

        WindowNode.Frame frame = new WindowNode.Frame(
                RANGE,
                UNBOUNDED_PRECEDING,
                Optional.empty(),
                Optional.empty(),
                UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        WindowNode.Function function = new WindowNode.Function(call("sum", functionHandle, BIGINT, variableC), frame, false);

        DataOrganizationSpecification specification = new DataOrganizationSpecification(ImmutableList.of(), Optional.empty());

        PlanNode node = new WindowNode(
                Optional.empty(),
                newId(),
                baseTableScan,
                specification,
                ImmutableMap.of(windowVariable, function),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        assertTypesValid(node);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "type of variable 'output(_[0-9]+)?' is expected to be date, but the actual type is bigint")
    public void testInvalidUnion()
    {
        VariableReferenceExpression output = variableAllocator.newVariable("output", DATE);

        PlanNode node = new UnionNode(
                Optional.empty(),
                newId(),
                ImmutableList.of(baseTableScan, baseTableScan),
                ImmutableList.of(output),
                ImmutableMap.of(
                        output,
                        ImmutableList.of(
                                variableD,
                                variableA))); // should be a symbol with DATE type

        assertTypesValid(node);
    }

    private void assertTypesValid(PlanNode node)
    {
        TYPE_VALIDATOR.validate(node, TEST_SESSION, createTestMetadataManager(), WarningCollector.NOOP);
    }

    private static PlanNodeId newId()
    {
        return new PlanNodeId(UUID.randomUUID().toString());
    }
}
