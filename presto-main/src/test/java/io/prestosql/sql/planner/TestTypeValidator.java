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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import io.prestosql.connector.ConnectorId;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.TableHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.AggregationNode.Aggregation;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.planner.plan.UnionNode;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.planner.sanity.TypeValidator;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FrameBound;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.WindowFrame;
import io.prestosql.testing.TestingMetadata.TestingColumnHandle;
import io.prestosql.testing.TestingMetadata.TestingTableHandle;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.prestosql.sql.planner.plan.AggregationNode.singleGroupingSet;

@Test(singleThreaded = true)
public class TestTypeValidator
{
    private static final TableHandle TEST_TABLE_HANDLE = new TableHandle(new ConnectorId("test"), new TestingTableHandle());
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final TypeValidator TYPE_VALIDATOR = new TypeValidator();

    private SymbolAllocator symbolAllocator;
    private TableScanNode baseTableScan;
    private Symbol columnA;
    private Symbol columnB;
    private Symbol columnC;
    private Symbol columnD;
    private Symbol columnE;

    @BeforeMethod
    public void setUp()
    {
        symbolAllocator = new SymbolAllocator();
        columnA = symbolAllocator.newSymbol("a", BIGINT);
        columnB = symbolAllocator.newSymbol("b", INTEGER);
        columnC = symbolAllocator.newSymbol("c", DOUBLE);
        columnD = symbolAllocator.newSymbol("d", DATE);
        columnE = symbolAllocator.newSymbol("e", VarcharType.createVarcharType(3));  // varchar(3), to test type only coercion

        Map<Symbol, ColumnHandle> assignments = ImmutableMap.<Symbol, ColumnHandle>builder()
                .put(columnA, new TestingColumnHandle("a"))
                .put(columnB, new TestingColumnHandle("b"))
                .put(columnC, new TestingColumnHandle("c"))
                .put(columnD, new TestingColumnHandle("d"))
                .put(columnE, new TestingColumnHandle("e"))
                .build();

        baseTableScan = new TableScanNode(
                newId(),
                TEST_TABLE_HANDLE,
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                Optional.empty(),
                TupleDomain.all(),
                TupleDomain.all());
    }

    @Test
    public void testValidProject()
    {
        Expression expression1 = new Cast(columnB.toSymbolReference(), StandardTypes.BIGINT);
        Expression expression2 = new Cast(columnC.toSymbolReference(), StandardTypes.BIGINT);
        Assignments assignments = Assignments.builder()
                .put(symbolAllocator.newSymbol(expression1, BIGINT), expression1)
                .put(symbolAllocator.newSymbol(expression2, BIGINT), expression2)
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
        Symbol outputSymbol = symbolAllocator.newSymbol("output", DATE);
        ListMultimap<Symbol, Symbol> mappings = ImmutableListMultimap.<Symbol, Symbol>builder()
                .put(outputSymbol, columnD)
                .put(outputSymbol, columnD)
                .build();

        PlanNode node = new UnionNode(
                newId(),
                ImmutableList.of(baseTableScan, baseTableScan),
                mappings,
                ImmutableList.copyOf(mappings.keySet()));

        assertTypesValid(node);
    }

    @Test
    public void testValidWindow()
    {
        Symbol windowSymbol = symbolAllocator.newSymbol("sum", DOUBLE);
        Signature signature = new Signature(
                "sum",
                FunctionKind.WINDOW,
                ImmutableList.of(),
                ImmutableList.of(),
                DOUBLE.getTypeSignature(),
                ImmutableList.of(DOUBLE.getTypeSignature()),
                false);
        FunctionCall functionCall = new FunctionCall(QualifiedName.of("sum"), ImmutableList.of(columnC.toSymbolReference()));

        WindowNode.Frame frame = new WindowNode.Frame(
                WindowFrame.Type.RANGE,
                FrameBound.Type.UNBOUNDED_PRECEDING,
                Optional.empty(),
                FrameBound.Type.UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        WindowNode.Function function = new WindowNode.Function(functionCall, signature, frame);

        WindowNode.Specification specification = new WindowNode.Specification(ImmutableList.of(), Optional.empty());

        PlanNode node = new WindowNode(
                newId(),
                baseTableScan,
                specification,
                ImmutableMap.of(windowSymbol, function),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        assertTypesValid(node);
    }

    @Test
    public void testValidAggregation()
    {
        Symbol aggregationSymbol = symbolAllocator.newSymbol("sum", DOUBLE);

        PlanNode node = new AggregationNode(
                newId(),
                baseTableScan,
                ImmutableMap.of(aggregationSymbol, new Aggregation(
                        new FunctionCall(QualifiedName.of("sum"), ImmutableList.of(columnC.toSymbolReference())),
                        new Signature(
                                "sum",
                                FunctionKind.AGGREGATE,
                                ImmutableList.of(),
                                ImmutableList.of(),
                                DOUBLE.getTypeSignature(),
                                ImmutableList.of(DOUBLE.getTypeSignature()),
                                false),
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(columnA, columnB)),
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
                .put(symbolAllocator.newSymbol(expression, BIGINT), expression)
                .put(symbolAllocator.newSymbol(columnE.toSymbolReference(), VARCHAR), columnE.toSymbolReference()) // implicit coercion from varchar(3) to varchar
                .build();
        PlanNode node = new ProjectNode(newId(), baseTableScan, assignments);

        assertTypesValid(node);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "type of symbol 'expr(_[0-9]+)?' is expected to be bigint, but the actual type is integer")
    public void testInvalidProject()
    {
        Expression expression1 = new Cast(columnB.toSymbolReference(), StandardTypes.INTEGER);
        Expression expression2 = new Cast(columnA.toSymbolReference(), StandardTypes.INTEGER);
        Assignments assignments = Assignments.builder()
                .put(symbolAllocator.newSymbol(expression1, BIGINT), expression1) // should be INTEGER
                .put(symbolAllocator.newSymbol(expression1, INTEGER), expression2)
                .build();
        PlanNode node = new ProjectNode(
                newId(),
                baseTableScan,
                assignments);

        assertTypesValid(node);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "type of symbol 'sum(_[0-9]+)?' is expected to be double, but the actual type is bigint")
    public void testInvalidAggregationFunctionCall()
    {
        Symbol aggregationSymbol = symbolAllocator.newSymbol("sum", DOUBLE);

        PlanNode node = new AggregationNode(
                newId(),
                baseTableScan,
                ImmutableMap.of(aggregationSymbol, new Aggregation(
                        new FunctionCall(QualifiedName.of("sum"), ImmutableList.of(columnA.toSymbolReference())),
                        new Signature(
                                "sum",
                                FunctionKind.AGGREGATE,
                                ImmutableList.of(),
                                ImmutableList.of(),
                                DOUBLE.getTypeSignature(),
                                ImmutableList.of(DOUBLE.getTypeSignature()),
                                false),
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(columnA, columnB)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty());

        assertTypesValid(node);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "type of symbol 'sum(_[0-9]+)?' is expected to be double, but the actual type is bigint")
    public void testInvalidAggregationFunctionSignature()
    {
        Symbol aggregationSymbol = symbolAllocator.newSymbol("sum", DOUBLE);

        PlanNode node = new AggregationNode(
                newId(),
                baseTableScan,
                ImmutableMap.of(aggregationSymbol, new Aggregation(
                        new FunctionCall(QualifiedName.of("sum"), ImmutableList.of(columnC.toSymbolReference())),
                        new Signature(
                                "sum",
                                FunctionKind.AGGREGATE,
                                ImmutableList.of(),
                                ImmutableList.of(),
                                BIGINT.getTypeSignature(), // should be DOUBLE
                                ImmutableList.of(DOUBLE.getTypeSignature()),
                                false),
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(columnA, columnB)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty());

        assertTypesValid(node);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "type of symbol 'sum(_[0-9]+)?' is expected to be double, but the actual type is bigint")
    public void testInvalidWindowFunctionCall()
    {
        Symbol windowSymbol = symbolAllocator.newSymbol("sum", DOUBLE);
        Signature signature = new Signature(
                "sum",
                FunctionKind.WINDOW,
                ImmutableList.of(),
                ImmutableList.of(),
                DOUBLE.getTypeSignature(),
                ImmutableList.of(DOUBLE.getTypeSignature()),
                false);
        FunctionCall functionCall = new FunctionCall(QualifiedName.of("sum"), ImmutableList.of(columnA.toSymbolReference())); // should be columnC

        WindowNode.Frame frame = new WindowNode.Frame(
                WindowFrame.Type.RANGE,
                FrameBound.Type.UNBOUNDED_PRECEDING,
                Optional.empty(),
                FrameBound.Type.UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        WindowNode.Function function = new WindowNode.Function(functionCall, signature, frame);

        WindowNode.Specification specification = new WindowNode.Specification(ImmutableList.of(), Optional.empty());

        PlanNode node = new WindowNode(
                newId(),
                baseTableScan,
                specification,
                ImmutableMap.of(windowSymbol, function),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        assertTypesValid(node);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "type of symbol 'sum(_[0-9]+)?' is expected to be double, but the actual type is bigint")
    public void testInvalidWindowFunctionSignature()
    {
        Symbol windowSymbol = symbolAllocator.newSymbol("sum", DOUBLE);
        Signature signature = new Signature(
                "sum",
                FunctionKind.WINDOW,
                ImmutableList.of(),
                ImmutableList.of(),
                BIGINT.getTypeSignature(), // should be DOUBLE
                ImmutableList.of(DOUBLE.getTypeSignature()),
                false);
        FunctionCall functionCall = new FunctionCall(QualifiedName.of("sum"), ImmutableList.of(columnC.toSymbolReference()));

        WindowNode.Frame frame = new WindowNode.Frame(
                WindowFrame.Type.RANGE,
                FrameBound.Type.UNBOUNDED_PRECEDING,
                Optional.empty(),
                FrameBound.Type.UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        WindowNode.Function function = new WindowNode.Function(functionCall, signature, frame);

        WindowNode.Specification specification = new WindowNode.Specification(ImmutableList.of(), Optional.empty());

        PlanNode node = new WindowNode(
                newId(),
                baseTableScan,
                specification,
                ImmutableMap.of(windowSymbol, function),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        assertTypesValid(node);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "type of symbol 'output(_[0-9]+)?' is expected to be date, but the actual type is bigint")
    public void testInvalidUnion()
    {
        Symbol outputSymbol = symbolAllocator.newSymbol("output", DATE);
        ListMultimap<Symbol, Symbol> mappings = ImmutableListMultimap.<Symbol, Symbol>builder()
                .put(outputSymbol, columnD)
                .put(outputSymbol, columnA) // should be a symbol with DATE type
                .build();

        PlanNode node = new UnionNode(
                newId(),
                ImmutableList.of(baseTableScan, baseTableScan),
                mappings,
                ImmutableList.copyOf(mappings.keySet()));

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
