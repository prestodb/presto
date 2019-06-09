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

import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.planner.sanity.TypeValidator;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.UNBOUNDED_FOLLOWING;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.UNBOUNDED_PRECEDING;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.WindowType.RANGE;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.variable;

@Test(singleThreaded = true)
public class TestTypeValidator
{
    private static final TableHandle TEST_TABLE_HANDLE = new TableHandle(
            new ConnectorId("test"),
            new TestingTableHandle(),
            TestingTransactionHandle.create(),
            Optional.empty());
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final TypeValidator TYPE_VALIDATOR = new TypeValidator();
    private static final FunctionManager FUNCTION_MANAGER = createTestMetadataManager().getFunctionManager();
    private static final FunctionHandle SUM = FUNCTION_MANAGER.lookupFunction("sum", fromTypes(DOUBLE));

    private SymbolAllocator symbolAllocator;
    private TableScanNode baseTableScan;
    private Symbol columnA;
    private Symbol columnB;
    private Symbol columnC;
    private Symbol columnD;
    private Symbol columnE;
    private VariableReferenceExpression variableA;
    private VariableReferenceExpression variableB;
    private VariableReferenceExpression variableC;
    private VariableReferenceExpression variableD;
    private VariableReferenceExpression variableE;

    @BeforeMethod
    public void setUp()
    {
        symbolAllocator = new SymbolAllocator();
        columnA = symbolAllocator.newSymbol("a", BIGINT);
        columnB = symbolAllocator.newSymbol("b", INTEGER);
        columnC = symbolAllocator.newSymbol("c", DOUBLE);
        columnD = symbolAllocator.newSymbol("d", DATE);
        columnE = symbolAllocator.newSymbol("e", VarcharType.createVarcharType(3));  // varchar(3), to test type only coercion

        variableA = new VariableReferenceExpression(columnA.getName(), BIGINT);
        variableB = new VariableReferenceExpression(columnB.getName(), INTEGER);
        variableC = new VariableReferenceExpression(columnC.getName(), DOUBLE);
        variableD = new VariableReferenceExpression(columnD.getName(), DATE);
        variableE = new VariableReferenceExpression(columnE.getName(), VarcharType.createVarcharType(3));

        Map<VariableReferenceExpression, ColumnHandle> assignments = ImmutableMap.<VariableReferenceExpression, ColumnHandle>builder()
                .put(variableA, new TestingColumnHandle("a"))
                .put(variableB, new TestingColumnHandle("b"))
                .put(variableC, new TestingColumnHandle("c"))
                .put(variableD, new TestingColumnHandle("d"))
                .put(variableE, new TestingColumnHandle("e"))
                .build();

        baseTableScan = new TableScanNode(
                newId(),
                TEST_TABLE_HANDLE,
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.all(),
                TupleDomain.all());
    }

    @Test
    public void testValidProject()
    {
        Expression expression1 = new Cast(columnB.toSymbolReference(), StandardTypes.BIGINT);
        Expression expression2 = new Cast(columnC.toSymbolReference(), StandardTypes.BIGINT);
        Assignments assignments = Assignments.builder()
                .put(symbolAllocator.newVariable(expression1, BIGINT), expression1)
                .put(symbolAllocator.newVariable(expression2, BIGINT), expression2)
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
        VariableReferenceExpression output = symbolAllocator.newVariable("output", DATE);
        ListMultimap<VariableReferenceExpression, VariableReferenceExpression> mappings = ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                .put(output, variableD)
                .put(output, variableD)
                .build();

        PlanNode node = new UnionNode(
                newId(),
                ImmutableList.of(baseTableScan, baseTableScan),
                mappings);

        assertTypesValid(node);
    }

    @Test
    public void testValidWindow()
    {
        Symbol windowSymbol = symbolAllocator.newSymbol("sum", DOUBLE);
        VariableReferenceExpression windowVariable = new VariableReferenceExpression(windowSymbol.getName(), DOUBLE);
        FunctionHandle functionHandle = FUNCTION_MANAGER.lookupFunction("sum", fromTypes(DOUBLE));

        WindowNode.Frame frame = new WindowNode.Frame(
                RANGE,
                UNBOUNDED_PRECEDING,
                Optional.empty(),
                UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        WindowNode.Function function = new WindowNode.Function(call("sum", functionHandle, DOUBLE, variable(columnC.getName(), DOUBLE)), frame);

        WindowNode.Specification specification = new WindowNode.Specification(ImmutableList.of(), Optional.empty());

        PlanNode node = new WindowNode(
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
        VariableReferenceExpression aggregationVariable = symbolAllocator.newVariable("sum", DOUBLE);

        PlanNode node = new AggregationNode(
                newId(),
                baseTableScan,
                ImmutableMap.of(aggregationVariable, new Aggregation(
                        new CallExpression("sum",
                                SUM,
                                DOUBLE,
                                ImmutableList.of(variable(columnC.getName(), DOUBLE))),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(variableA, variableB)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty());

        assertTypesValid(node);
    }

    @Test
    public void testValidTypeOnlyCoercion()
    {
        Expression expression = new Cast(columnB.toSymbolReference(), StandardTypes.BIGINT);
        Assignments assignments = Assignments.builder()
                .put(symbolAllocator.newVariable(expression, BIGINT), expression)
                .put(symbolAllocator.newVariable(columnE.toSymbolReference(), VARCHAR), columnE.toSymbolReference()) // implicit coercion from varchar(3) to varchar
                .build();
        PlanNode node = new ProjectNode(newId(), baseTableScan, assignments);

        assertTypesValid(node);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "type of variable 'expr(_[0-9]+)?' is expected to be bigint, but the actual type is integer")
    public void testInvalidProject()
    {
        Expression expression1 = new Cast(columnB.toSymbolReference(), StandardTypes.INTEGER);
        Expression expression2 = new Cast(columnA.toSymbolReference(), StandardTypes.INTEGER);
        Assignments assignments = Assignments.builder()
                .put(symbolAllocator.newVariable(expression1, BIGINT), expression1) // should be INTEGER
                .put(symbolAllocator.newVariable(expression1, INTEGER), expression2)
                .build();
        PlanNode node = new ProjectNode(
                newId(),
                baseTableScan,
                assignments);

        assertTypesValid(node);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Expected input types are \\[double\\] but getting \\[bigint\\]")
    public void testInvalidAggregationFunctionCall()
    {
        VariableReferenceExpression aggregationVariable = symbolAllocator.newVariable("sum", DOUBLE);

        PlanNode node = new AggregationNode(
                newId(),
                baseTableScan,
                ImmutableMap.of(aggregationVariable, new Aggregation(
                        new CallExpression(
                                "sum",
                                SUM,
                                DOUBLE,
                                ImmutableList.of(variable(columnA.getName(), BIGINT))),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(variableA, variableB)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty());

        assertTypesValid(node);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "type of variable 'sum(_[0-9]+)?' is expected to be double, but the actual type is bigint")
    public void testInvalidAggregationFunctionSignature()
    {
        VariableReferenceExpression aggregationVariable = symbolAllocator.newVariable("sum", DOUBLE);

        PlanNode node = new AggregationNode(
                newId(),
                baseTableScan,
                ImmutableMap.of(aggregationVariable, new Aggregation(
                        new CallExpression(
                                "sum",
                                FUNCTION_MANAGER.lookupFunction("sum", fromTypes(BIGINT)), // should be DOUBLE
                                DOUBLE,
                                ImmutableList.of(variable(columnC.getName(), BIGINT))),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(variableA, variableB)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty());

        assertTypesValid(node);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "type of variable 'sum(_[0-9]+)?' is expected to be double, but the actual type is bigint")
    public void testInvalidWindowFunctionCall()
    {
        Symbol windowSymbol = symbolAllocator.newSymbol("sum", DOUBLE);
        VariableReferenceExpression windowVariable = new VariableReferenceExpression(windowSymbol.getName(), DOUBLE);
        FunctionHandle functionHandle = FUNCTION_MANAGER.lookupFunction("sum", fromTypes(DOUBLE));

        WindowNode.Frame frame = new WindowNode.Frame(
                RANGE,
                UNBOUNDED_PRECEDING,
                Optional.empty(),
                UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        WindowNode.Function function = new WindowNode.Function(call("sum", functionHandle, BIGINT, new VariableReferenceExpression(columnA.getName(), BIGINT)), frame);

        WindowNode.Specification specification = new WindowNode.Specification(ImmutableList.of(), Optional.empty());

        PlanNode node = new WindowNode(
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
        Symbol windowSymbol = symbolAllocator.newSymbol("sum", DOUBLE);
        VariableReferenceExpression windowVariable = new VariableReferenceExpression(windowSymbol.getName(), DOUBLE);
        FunctionHandle functionHandle = FUNCTION_MANAGER.lookupFunction("sum", fromTypes(BIGINT)); // should be DOUBLE

        WindowNode.Frame frame = new WindowNode.Frame(
                RANGE,
                UNBOUNDED_PRECEDING,
                Optional.empty(),
                UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        WindowNode.Function function = new WindowNode.Function(call("sum", functionHandle, BIGINT, new VariableReferenceExpression(columnC.getName(), DOUBLE)), frame);

        WindowNode.Specification specification = new WindowNode.Specification(ImmutableList.of(), Optional.empty());

        PlanNode node = new WindowNode(
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
        VariableReferenceExpression output = symbolAllocator.newVariable("output", DATE);
        ListMultimap<VariableReferenceExpression, VariableReferenceExpression> mappings = ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                .put(output, variableD)
                .put(output, variableA) // should be a symbol with DATE type
                .build();

        PlanNode node = new UnionNode(
                newId(),
                ImmutableList.of(baseTableScan, baseTableScan),
                mappings);

        assertTypesValid(node);
    }

    private void assertTypesValid(PlanNode node)
    {
        TYPE_VALIDATOR.validate(node, TEST_SESSION, createTestMetadataManager(), SQL_PARSER, symbolAllocator.getTypes(), WarningCollector.NOOP);
    }

    private static PlanNodeId newId()
    {
        return new PlanNodeId(UUID.randomUUID().toString());
    }
}
