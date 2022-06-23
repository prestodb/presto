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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.constraints.PrimaryKeyConstraint;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.constraints.UniqueConstraint;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.LogicalProperties;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TestTableConstraintsConnectorFactory;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.properties.EquivalenceClassProperty;
import com.facebook.presto.sql.planner.iterative.properties.Key;
import com.facebook.presto.sql.planner.iterative.properties.KeyProperty;
import com.facebook.presto.sql.planner.iterative.properties.LogicalPropertiesImpl;
import com.facebook.presto.sql.planner.iterative.properties.LogicalPropertiesProviderImpl;
import com.facebook.presto.sql.planner.iterative.properties.MaxCardProperty;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tpch.TpchTableLayoutHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Collections.emptyList;

public class TestLogicalPropertyPropagation
        extends BaseRuleTest
{
    private TableHandle customerTableHandle;
    private TableHandle ordersTableHandle;
    private TableHandle lineitemTableHandle;

    private ColumnHandle customerCustKeyColumn;
    private ColumnHandle customerNationKeyColumn;
    private ColumnHandle customerCommentColumn;
    private ColumnHandle mktSegmentColumn;
    private ColumnHandle acctBalColumn;
    private ColumnHandle ordersCustKeyColumn;
    private ColumnHandle ordersOrderKeyColumn;
    private ColumnHandle ordersOrderPriorityColumn;
    private ColumnHandle ordersCommentColumn;
    private ColumnHandle shipPriorityColumn;
    private ColumnHandle lineitemOrderkeyColumn;
    private ColumnHandle lineitemLinenumberColumn;
    private ColumnHandle lineitemExtendedPriceColumn;

    private VariableReferenceExpression customerCustKeyVariable;
    private VariableReferenceExpression customerNationKeyVariable;
    private VariableReferenceExpression customerCommentVariable;
    private VariableReferenceExpression shipPriorityVariable;
    private VariableReferenceExpression mktSegmentVariable;
    private VariableReferenceExpression acctBalVariable;
    private VariableReferenceExpression ordersCustKeyVariable;
    private VariableReferenceExpression ordersOrderKeyVariable;
    private VariableReferenceExpression ordersOrderPriorityVariable;
    private VariableReferenceExpression ordersCommentVariable;
    private VariableReferenceExpression lineitemOrderkeyVariable;
    private VariableReferenceExpression lineitemLinenumberVariable;
    private VariableReferenceExpression lineitemExtendedPriceVariable;

    private FunctionResolution functionResolution;
    private LogicalPropertiesProviderImpl logicalPropertiesProvider;

    @BeforeClass
    public final void setUp()
    {
        tester = new RuleTester(emptyList(), ImmutableMap.of(), Optional.of(1), new TestTableConstraintsConnectorFactory(1));
        ConnectorId connectorId = tester().getCurrentConnectorId();
        functionResolution = new FunctionResolution(tester.getMetadata().getFunctionAndTypeManager());
        logicalPropertiesProvider = new LogicalPropertiesProviderImpl(functionResolution);

        TpchTableHandle customerTpchTableHandle = new TpchTableHandle("customer", 1.0);
        TpchTableHandle ordersTpchTableHandle = new TpchTableHandle("orders", 1.0);
        TpchTableHandle lineitemTpchTableHandle = new TpchTableHandle("lineitem", 1.0);

        customerTableHandle = new TableHandle(
                connectorId,
                customerTpchTableHandle,
                TestingTransactionHandle.create(),
                Optional.of(new TpchTableLayoutHandle(customerTpchTableHandle, TupleDomain.all())));

        ordersTableHandle = new TableHandle(
                connectorId,
                ordersTpchTableHandle,
                TestingTransactionHandle.create(),
                Optional.of(new TpchTableLayoutHandle(ordersTpchTableHandle, TupleDomain.all())));

        lineitemTableHandle = new TableHandle(
                connectorId,
                lineitemTpchTableHandle,
                TestingTransactionHandle.create(),
                Optional.of(new TpchTableLayoutHandle(lineitemTpchTableHandle, TupleDomain.all())));

        customerCustKeyColumn = new TpchColumnHandle("custkey", BIGINT);
        customerCommentColumn = new TpchColumnHandle("comment", VARCHAR);
        customerNationKeyColumn = new TpchColumnHandle("nationkey", BIGINT);
        mktSegmentColumn = new TpchColumnHandle("mktsegment", VARCHAR);
        acctBalColumn = new TpchColumnHandle("acctbal", DOUBLE);
        ordersCustKeyColumn = new TpchColumnHandle("custkey", BIGINT);
        ordersOrderKeyColumn = new TpchColumnHandle("orderkey", BIGINT);
        ordersOrderPriorityColumn = new TpchColumnHandle("orderpriority", BIGINT);
        shipPriorityColumn = new TpchColumnHandle("shippriority", INTEGER);
        ordersCommentColumn = new TpchColumnHandle("comment", VARCHAR);
        lineitemOrderkeyColumn = new TpchColumnHandle("orderkey", BIGINT);
        lineitemLinenumberColumn = new TpchColumnHandle("linenumber", BIGINT);
        lineitemExtendedPriceColumn = new TpchColumnHandle("extendedprice", DOUBLE);

        customerCustKeyVariable = new VariableReferenceExpression(Optional.empty(), "c_custkey", BIGINT);
        customerNationKeyVariable = new VariableReferenceExpression(Optional.empty(), "nationkey", BIGINT);
        customerCommentVariable = new VariableReferenceExpression(Optional.empty(), "c_comment", VARCHAR);
        mktSegmentVariable = new VariableReferenceExpression(Optional.empty(), "c_mktsegment", VARCHAR);
        acctBalVariable = new VariableReferenceExpression(Optional.empty(), "c_acctbal", DOUBLE);
        ordersCustKeyVariable = new VariableReferenceExpression(Optional.empty(), "o_custkey", BIGINT);
        ordersOrderKeyVariable = new VariableReferenceExpression(Optional.empty(), "o_orderkey", BIGINT);
        ordersOrderPriorityVariable = new VariableReferenceExpression(Optional.empty(), "o_orderpriority", VARCHAR);
        shipPriorityVariable = new VariableReferenceExpression(Optional.empty(), "o_shippriority", INTEGER);
        ordersCommentVariable = new VariableReferenceExpression(Optional.empty(), "o_comment", DOUBLE);
        lineitemOrderkeyVariable = new VariableReferenceExpression(Optional.empty(), "l_orderkey", BIGINT);
        lineitemLinenumberVariable = new VariableReferenceExpression(Optional.empty(), "l_linenumber", BIGINT);
        lineitemExtendedPriceVariable = new VariableReferenceExpression(Optional.empty(), "l_extendedprice", DOUBLE);
    }

    @Test
    void testValuesNodeLogicalProperties()
    {
        LogicalProperties expectedLogicalProperties = new LogicalPropertiesImpl(new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(1L),
                new KeyProperty(ImmutableSet.of()));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    VariableReferenceExpression c = p.variable("c");
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(c)
                            .source(p.values(1, c)));
                })
                .matches(expectedLogicalProperties);

        //Values has more than one row.
        VariableReferenceExpression a = new VariableReferenceExpression(Optional.empty(), "a", BIGINT);
        expectedLogicalProperties = new LogicalPropertiesImpl(new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(3L),
                new KeyProperty(ImmutableSet.of()));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> p.values(
                        ImmutableList.of(a),
                        ImmutableList.of(ImmutableList.of(constant(1L, BIGINT)),
                                ImmutableList.of(constant(2L, BIGINT)),
                                ImmutableList.of(constant(3L, BIGINT)))))
                .matches(expectedLogicalProperties);
    }

    @Test
    public void testTableScanNodeLogicalProperties()
    {
        // "custkey" should be a key in the result of TableScan(customer)
        LogicalProperties expectedLogicalProperties = new LogicalPropertiesImpl(new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(customerCustKeyVariable)))));
        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> p.tableScan(
                        customerTableHandle,
                        ImmutableList.of(customerCustKeyVariable),
                        ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                        TupleDomain.none(),
                        TupleDomain.none(),
                        tester().getTableConstraints(customerTableHandle)))
                .matches(expectedLogicalProperties);

        // add an additional unique constraint on customer (comment, nationkey)column
        Set<ColumnHandle> commentcolumnSet = new HashSet<>();
        commentcolumnSet.add(customerCommentColumn);
        UniqueConstraint<ColumnHandle> commentConstraint = new UniqueConstraint<>(commentcolumnSet, true, true);
        List<TableConstraint<ColumnHandle>> customerConstraints = new ArrayList<>(tester().getTableConstraints(customerTableHandle));
        customerConstraints.add(commentConstraint);

        expectedLogicalProperties = new LogicalPropertiesImpl(new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(customerCustKeyVariable)), new Key(ImmutableSet.of(customerCommentVariable)))));
        List<TableConstraint<ColumnHandle>> finalCustomerConstraints = customerConstraints;
        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> p.tableScan(
                        customerTableHandle,
                        ImmutableList.of(customerCustKeyVariable, customerCommentVariable),
                        ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn, customerCommentVariable, customerCommentColumn),
                        TupleDomain.none(),
                        TupleDomain.none(),
                        finalCustomerConstraints))
                .matches(expectedLogicalProperties);

        //TEST: the previous test but there is no assigment for the comment column
        expectedLogicalProperties = new LogicalPropertiesImpl(new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(customerCustKeyVariable)))));
        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> p.tableScan(
                        customerTableHandle,
                        ImmutableList.of(customerCustKeyVariable),
                        ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                        TupleDomain.none(),
                        TupleDomain.none(),
                        finalCustomerConstraints))
                .matches(expectedLogicalProperties);

        //TEST: add a superfulous unique constraint on the (custkey, comment) combination
        Set<ColumnHandle> custkeyCommentColumnSet = new HashSet<>();
        custkeyCommentColumnSet.add(customerCustKeyColumn);
        custkeyCommentColumnSet.add(customerCommentColumn);
        UniqueConstraint<ColumnHandle> custkeyCommentConstraint = new UniqueConstraint<>(custkeyCommentColumnSet, true, true);
        customerConstraints = new ArrayList<>(tester().getTableConstraints(customerTableHandle));
        customerConstraints.add(custkeyCommentConstraint);

        expectedLogicalProperties = new LogicalPropertiesImpl(new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(customerCustKeyVariable)))));
        List<TableConstraint<ColumnHandle>> finalCustomerConstraints1 = customerConstraints;
        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> p.tableScan(
                        customerTableHandle,
                        ImmutableList.of(customerCustKeyVariable, customerCommentVariable),
                        ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn,
                                customerCommentVariable, customerCommentColumn),
                        TupleDomain.none(),
                        TupleDomain.none(),
                        finalCustomerConstraints1))
                .matches(expectedLogicalProperties);

        //Define a table with key (A,B) but only give a table scan mapping for A (B). The key property of the table scan should be empty.
        expectedLogicalProperties = new LogicalPropertiesImpl(new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of()));
        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> p.tableScan(
                        customerTableHandle,
                        ImmutableList.of(customerCustKeyVariable),
                        ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                        TupleDomain.none(),
                        TupleDomain.none(),
                        ImmutableList.of(custkeyCommentConstraint)))
                .matches(expectedLogicalProperties);

        // INVARIANT: define a table with primary key (A) and unique key (A,B) and ensure that the table scan key property only has key (A) (both A and B should have mappings)
        expectedLogicalProperties = new LogicalPropertiesImpl(new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(customerCustKeyVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> p.tableScan(
                        customerTableHandle,
                        ImmutableList.of(customerCustKeyVariable, customerCommentVariable),
                        ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn,
                                customerCommentVariable, customerCommentColumn),
                        TupleDomain.none(),
                        TupleDomain.none(),
                        finalCustomerConstraints1))
                .matches(expectedLogicalProperties);

        // INVARIANT: define a table with primary key (A,B) and unique key (A) and ensure that the table scan key property only has key (A) (both A and B should have mappings)
        PrimaryKeyConstraint<ColumnHandle> custkeyCommentPK = new PrimaryKeyConstraint<>("primarykey", custkeyCommentColumnSet, true, true);
        UniqueConstraint<ColumnHandle> custkeyUniqueConstraint = new UniqueConstraint<>(ImmutableSet.of(customerCustKeyColumn), true, true);

        expectedLogicalProperties = new LogicalPropertiesImpl(new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(customerCustKeyVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> p.tableScan(
                        customerTableHandle,
                        ImmutableList.of(customerCustKeyVariable, customerCommentVariable),
                        ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn,
                                customerCommentVariable, customerCommentColumn),
                        TupleDomain.none(),
                        TupleDomain.none(),
                        ImmutableList.of(custkeyCommentPK, custkeyUniqueConstraint)))
                .matches(expectedLogicalProperties);
    }

    @Test
    void testFilterNodeLogicalProperties()
    {
        ConstantExpression constExpr = new ConstantExpression(100L, BIGINT);
        EquivalenceClassProperty equivalenceClasses = new EquivalenceClassProperty(functionResolution);
        equivalenceClasses.update(customerCustKeyVariable, constExpr);

        LogicalProperties expectedLogicalProperties = new LogicalPropertiesImpl(
                equivalenceClasses,
                new MaxCardProperty(1L),
                new KeyProperty(ImmutableSet.of()));

        // Primary key will be propagated till the FilterNode, where maxCardProperty will be set to 1
        // and KeyProperty will be cleared. Equivalence class property reflects the predicate "custkey = 100".
        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    p.variable(customerCustKeyVariable);
                    return p.filter(p.rowExpression("c_custkey = BIGINT '100'"),
                            p.tableScan(
                                    customerTableHandle,
                                    ImmutableList.of(acctBalVariable),
                                    ImmutableMap.of(acctBalVariable, acctBalColumn,
                                            customerCustKeyVariable, customerCustKeyColumn),
                                    TupleDomain.none(),
                                    TupleDomain.none(),
                                    tester().getTableConstraints(customerTableHandle)));
                })
                .matches(expectedLogicalProperties);

        //INVARIANT: define a table with keys (A) and (B,C) and apply predicate A=B and ensure that the filter key property only has key (A)
        ColumnHandle colA = new TpchColumnHandle("A", BIGINT);
        ColumnHandle colB = new TpchColumnHandle("B", BIGINT);
        ColumnHandle colC = new TpchColumnHandle("C", BIGINT);

        PrimaryKeyConstraint<ColumnHandle> primaryKeyConstraint = new PrimaryKeyConstraint<>("primarykey", ImmutableSet.of(colA), true, true);
        UniqueConstraint<ColumnHandle> uniqueConstraint = new UniqueConstraint<>(ImmutableSet.of(colB, colC), true, true);
        List<TableConstraint<ColumnHandle>> tableConstraints = ImmutableList.of(primaryKeyConstraint, uniqueConstraint);

        VariableReferenceExpression varA = new VariableReferenceExpression(Optional.empty(), "A", BIGINT);
        VariableReferenceExpression varB = new VariableReferenceExpression(Optional.empty(), "B", BIGINT);
        VariableReferenceExpression varC = new VariableReferenceExpression(Optional.empty(), "C", BIGINT);

        equivalenceClasses = new EquivalenceClassProperty(functionResolution);
        equivalenceClasses.update(varA, varB);

        expectedLogicalProperties = new LogicalPropertiesImpl(
                equivalenceClasses,
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(varA)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    p.variable(varA);
                    p.variable(varB);
                    return p.filter(p.rowExpression("A = B"),
                            p.tableScan(
                                    customerTableHandle,
                                    ImmutableList.of(varA),
                                    ImmutableMap.of(varA, colA, varB, colB, varC, colC),
                                    TupleDomain.none(),
                                    TupleDomain.none(),
                                    tableConstraints));
                })
                .matches(expectedLogicalProperties);

        //INVARIANT: define a table with keys (A,C) and (B,C) and apply predicate A=constant and ensure that the filter key property has only has one key (C)
        PrimaryKeyConstraint<ColumnHandle> primaryKeyConstraint1 = new PrimaryKeyConstraint<>("primarykey", ImmutableSet.of(colA, colC), true, true);
        UniqueConstraint<ColumnHandle> uniqueConstraint1 = new UniqueConstraint<>(ImmutableSet.of(colB, colC), true, true);
        List<TableConstraint<ColumnHandle>> tableConstraints1 = ImmutableList.of(primaryKeyConstraint1, uniqueConstraint1);

        equivalenceClasses = new EquivalenceClassProperty(functionResolution);
        equivalenceClasses.update(varA, constant(100L, BIGINT));

        expectedLogicalProperties = new LogicalPropertiesImpl(
                equivalenceClasses,
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(varC)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    p.variable(varA);
                    p.variable(varB);
                    return p.filter(p.rowExpression("A = BIGINT '100'"),
                            p.tableScan(
                                    customerTableHandle,
                                    ImmutableList.of(varA),
                                    ImmutableMap.of(varA, colA, varB, colB, varC, colC),
                                    TupleDomain.none(),
                                    TupleDomain.none(),
                                    tableConstraints1));
                })
                .matches(expectedLogicalProperties);

        //INVARIANT: define a table with key (A,B) and apply predicates A=constant1 and B=constant2 ensure that the filter has maxcard=1 and key property is empty

        List<TableConstraint<ColumnHandle>> tableConstraints2 = ImmutableList.of(new PrimaryKeyConstraint<>("primarykey", ImmutableSet.of(colA, colB), true, true));

        equivalenceClasses = new EquivalenceClassProperty(functionResolution);
        equivalenceClasses.update(varA, constant(100L, BIGINT));
        equivalenceClasses.update(varB, constant(50L, BIGINT));

        expectedLogicalProperties = new LogicalPropertiesImpl(
                equivalenceClasses,
                new MaxCardProperty(1L),
                new KeyProperty(ImmutableSet.of()));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    p.variable(varA);
                    p.variable(varB);
                    return p.filter(p.rowExpression("A = BIGINT '100' AND B = BIGINT '50'"),
                            p.tableScan(
                                    customerTableHandle,
                                    ImmutableList.of(varA),
                                    ImmutableMap.of(varA, colA, varB, colB, varC, colC),
                                    TupleDomain.none(),
                                    TupleDomain.none(),
                                    tableConstraints2));
                })
                .matches(expectedLogicalProperties);

        //INVARIANT: define a table with key (A,B) and apply predicates A=constant and A=B ensure that the filter has maxcard=1 and key property is empty
        equivalenceClasses = new EquivalenceClassProperty(functionResolution);
        equivalenceClasses.update(varA, varB);
        equivalenceClasses.update(varA, constant(100L, BIGINT));

        expectedLogicalProperties = new LogicalPropertiesImpl(
                equivalenceClasses,
                new MaxCardProperty(1L),
                new KeyProperty(ImmutableSet.of()));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    p.variable(varA);
                    p.variable(varB);
                    return p.filter(p.rowExpression("A = B AND A = BIGINT '100'"),
                            p.tableScan(
                                    customerTableHandle,
                                    ImmutableList.of(varA),
                                    ImmutableMap.of(varA, colA, varB, colB, varC, colC),
                                    TupleDomain.none(),
                                    TupleDomain.none(),
                                    tableConstraints2));
                })
                .matches(expectedLogicalProperties);
    }

    @Test
    public void testProjectNodeLogicalProperties()
    {
        VariableReferenceExpression projectedCustKeyVariable = new VariableReferenceExpression(Optional.empty(), "newcustkey", BIGINT);
        Assignments assignments = Assignments.builder().put(projectedCustKeyVariable, customerCustKeyVariable).build();

        LogicalProperties expectedLogicalProperties = new LogicalPropertiesImpl(new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(projectedCustKeyVariable)))));

        // Test Logical properties generated for the project node
        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> p.project(assignments, p.tableScan(
                        customerTableHandle,
                        ImmutableList.of(customerCustKeyVariable),
                        ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                        TupleDomain.none(),
                        TupleDomain.none(),
                        tester().getTableConstraints(customerTableHandle))))
                .matches(expectedLogicalProperties);

        //TableScan has key property (A,B). Project only has mapping A->A' and hence result key property should be empty.
        ColumnHandle colA = new TpchColumnHandle("A", BIGINT);
        ColumnHandle colB = new TpchColumnHandle("B", BIGINT);
        VariableReferenceExpression varA = new VariableReferenceExpression(Optional.empty(), "A", BIGINT);
        VariableReferenceExpression varB = new VariableReferenceExpression(Optional.empty(), "B", BIGINT);
        VariableReferenceExpression projectedVarA = new VariableReferenceExpression(Optional.empty(), "A1", BIGINT);
        List<TableConstraint<ColumnHandle>> tableConstraints = ImmutableList.of(new PrimaryKeyConstraint<>("primarykey", ImmutableSet.of(colA, colB), true, true));
        Assignments assignments1 = Assignments.builder().put(projectedVarA, varA).build();

        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of()));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    p.variable(varA);
                    p.variable(varB);
                    return p.project(assignments1, p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(varA),
                            ImmutableMap.of(varA, colA, varB, colB),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tableConstraints));
                })
                .matches(expectedLogicalProperties);

        //TableScan key property has key (A), Filter applies predicate A=B, Project only has a mapping B->B'. Project should have key property with (B').
        List<TableConstraint<ColumnHandle>> tableConstraints1 = ImmutableList.of(new PrimaryKeyConstraint<>("primarykey", ImmutableSet.of(colA), true, true));
        VariableReferenceExpression projectedA = new VariableReferenceExpression(Optional.empty(), "A1", BIGINT);
        Assignments assignments2 = Assignments.builder().put(projectedA, varA).build();

        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(projectedA)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    p.variable(varA);
                    p.variable(varB);
                    return p.project(assignments2,
                            p.filter(p.rowExpression("A = B"),
                                    p.tableScan(
                                            customerTableHandle,
                                            ImmutableList.of(varA),
                                            ImmutableMap.of(varA, colA, varB, colB),
                                            TupleDomain.none(),
                                            TupleDomain.none(),
                                            tableConstraints1)));
                })
                .matches(expectedLogicalProperties);

        // INVARIANT Filter creates multiple equivalence classes e.g. (A, B, C) (D, E, F). Test various cases where
        // all or only some of these have mappings. These should include cases where the equivalence class heads are
        // projected out, members are projected out, and also cases where the entire equivalence class is projected out.
        ColumnHandle colC = new TpchColumnHandle("C", BIGINT);
        ColumnHandle colD = new TpchColumnHandle("D", BIGINT);
        ColumnHandle colE = new TpchColumnHandle("E", BIGINT);
        ColumnHandle colF = new TpchColumnHandle("F", BIGINT);
        VariableReferenceExpression varC = new VariableReferenceExpression(Optional.empty(), "C", BIGINT);
        VariableReferenceExpression varD = new VariableReferenceExpression(Optional.empty(), "D", BIGINT);
        VariableReferenceExpression varE = new VariableReferenceExpression(Optional.empty(), "E", BIGINT);
        VariableReferenceExpression varF = new VariableReferenceExpression(Optional.empty(), "F", BIGINT);
        VariableReferenceExpression projectedB = new VariableReferenceExpression(Optional.empty(), "B1", BIGINT);
        VariableReferenceExpression projectedC = new VariableReferenceExpression(Optional.empty(), "C1", BIGINT);
        VariableReferenceExpression projectedD = new VariableReferenceExpression(Optional.empty(), "D1", BIGINT);
        VariableReferenceExpression projectedE = new VariableReferenceExpression(Optional.empty(), "E1", BIGINT);
        VariableReferenceExpression projectedF = new VariableReferenceExpression(Optional.empty(), "F1", BIGINT);

        Map<VariableReferenceExpression, ColumnHandle> scanAssignments =
                new ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle>()
                        .put(varA, colA)
                        .put(varB, colB)
                        .put(varC, colC)
                        .put(varD, colD)
                        .put(varE, colE)
                        .put(varF, colF).build();

        Assignments projectAssignments = Assignments.builder()
                .put(projectedA, varA)
                .put(projectedB, varB)
                .put(projectedC, varC)
                .put(projectedD, varD)
                .put(projectedE, varE)
                .put(projectedF, varF)
                .build();

        // A = B and B = C and D = E and E = F
        EquivalenceClassProperty equivalenceClassProperty = new EquivalenceClassProperty(functionResolution);
        equivalenceClassProperty.update(projectedA, projectedB);
        equivalenceClassProperty.update(projectedB, projectedC);
        equivalenceClassProperty.update(projectedD, projectedE);
        equivalenceClassProperty.update(projectedE, projectedF);
        expectedLogicalProperties = new LogicalPropertiesImpl(
                equivalenceClassProperty,
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(projectedA)))));

        // ProjectNode projects all variables used in the filter.
        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    p.variable(varA);
                    p.variable(varB);
                    p.variable(varC);
                    p.variable(varD);
                    p.variable(varE);
                    p.variable(varF);

                    return p.project(projectAssignments,
                            p.filter(p.rowExpression("A = B AND B = C AND D = E AND E = F"),
                                    p.tableScan(
                                            customerTableHandle,
                                            ImmutableList.of(varA, varB, varC, varD, varE, varF),
                                            scanAssignments,
                                            TupleDomain.none(),
                                            TupleDomain.none(),
                                            tableConstraints1)));
                })
                .matches(expectedLogicalProperties);

        // ProjectNode projects only equivalence class heads(A, D). Equivalence classes should be empty. KeyProperty
        // should be set to A1.
        Assignments projectAssignments1 = Assignments.builder()
                .put(projectedA, varA)
                .put(projectedD, varD)
                .build();
        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(projectedA)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    p.variable(varA);
                    p.variable(varB);
                    p.variable(varC);
                    p.variable(varD);
                    p.variable(varE);
                    p.variable(varF);

                    return p.project(projectAssignments1,
                            p.filter(p.rowExpression("A = B AND B = C AND D = E AND E = F"),
                                    p.tableScan(
                                            customerTableHandle,
                                            ImmutableList.of(varA, varB, varC, varD, varE, varF),
                                            scanAssignments,
                                            TupleDomain.none(),
                                            TupleDomain.none(),
                                            tableConstraints1)));
                })
                .matches(expectedLogicalProperties);

        // ProjectNode projects only equivalence class members(B,C,E,F). Equivalence classes should have (B,C), (E,F).
        // KeyProperty should have B1
        Assignments projectAssignments2 = Assignments.builder()
                .put(projectedB, varB)
                .put(projectedC, varC)
                .put(projectedE, varE)
                .put(projectedF, varF)
                .build();
        EquivalenceClassProperty equivalenceClassProperty1 = new EquivalenceClassProperty(functionResolution);
        equivalenceClassProperty1.update(projectedB, projectedC);
        equivalenceClassProperty1.update(projectedE, projectedF);

        expectedLogicalProperties = new LogicalPropertiesImpl(
                equivalenceClassProperty1,
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(projectedB)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    p.variable(varA);
                    p.variable(varB);
                    p.variable(varC);
                    p.variable(varD);
                    p.variable(varE);
                    p.variable(varF);

                    return p.project(projectAssignments2,
                            p.filter(p.rowExpression("A = B AND B = C AND D = E AND E = F"),
                                    p.tableScan(
                                            customerTableHandle,
                                            ImmutableList.of(varA, varB, varC, varD, varE, varF),
                                            scanAssignments,
                                            TupleDomain.none(),
                                            TupleDomain.none(),
                                            tableConstraints1)));
                })
                .matches(expectedLogicalProperties);

        // ProjectNode projects only equivalence class members(E,F). Equivalence classes should have (E,F).
        // KeyProperty should become empty since A,B,C are removed from projection.
        Assignments projectAssignments3 = Assignments.builder()
                .put(projectedE, varE)
                .put(projectedF, varF)
                .build();
        EquivalenceClassProperty equivalenceClassProperty2 = new EquivalenceClassProperty(functionResolution);
        equivalenceClassProperty2.update(projectedE, projectedF);

        expectedLogicalProperties = new LogicalPropertiesImpl(
                equivalenceClassProperty2,
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of()));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    p.variable(varA);
                    p.variable(varB);
                    p.variable(varC);
                    p.variable(varD);
                    p.variable(varE);
                    p.variable(varF);

                    return p.project(projectAssignments3,
                            p.filter(p.rowExpression("A = B AND B = C AND D = E AND E = F"),
                                    p.tableScan(
                                            customerTableHandle,
                                            ImmutableList.of(varA, varB, varC, varD, varE, varF),
                                            scanAssignments,
                                            TupleDomain.none(),
                                            TupleDomain.none(),
                                            tableConstraints1)));
                })
                .matches(expectedLogicalProperties);
    }

    @Test
    public void testJoinNodeLogicalProperties()
    {
        // TEST: n to 1 inner join between orders and customers with limit 5 on left table.
        // orders key property,  maxcard=5 and equivalence classes(o_custkey=c_custkey), (shippriority=10) and
        // (mktsegment='BUILDING') should be propagated.
        EquivalenceClassProperty equivalenceClasses = new EquivalenceClassProperty(functionResolution);
        equivalenceClasses.update(ordersCustKeyVariable, customerCustKeyVariable);
        equivalenceClasses.update(shipPriorityVariable, constant(10L, INTEGER));
        equivalenceClasses.update(mktSegmentVariable, constant(Slices.utf8Slice("BUILDING"), createVarcharType(8)));

        LogicalProperties expectedLogicalProperties = new LogicalPropertiesImpl(
                equivalenceClasses,
                new MaxCardProperty(5L),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(ordersOrderKeyVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable, mktSegmentVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn, mktSegmentVariable, mktSegmentColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    TableScanNode ordersTableScan = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable, shipPriorityVariable),
                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn,
                                    ordersOrderKeyVariable, ordersOrderKeyColumn,
                                    shipPriorityVariable, shipPriorityColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(ordersTableHandle));

                    p.variable(shipPriorityVariable);
                    p.variable(mktSegmentVariable);
                    return p.join(JoinNode.Type.INNER,
                            p.limit(5, ordersTableScan),
                            p.filter(p.rowExpression("c_mktsegment = 'BUILDING'"), customerTableScan),
                            p.rowExpression("o_shippriority = 10"),
                            new JoinNode.EquiJoinClause(ordersCustKeyVariable, customerCustKeyVariable));
                })
                .matches(expectedLogicalProperties);

        // TEST: n to 1 inner join between orders and customers with limit 1 on left table. maxcard=1 and equivalence
        // classes(o_custkey=c_custkey),(shippriority=10) and (mktsegment='BUILDING') should be propagated.
        expectedLogicalProperties = new LogicalPropertiesImpl(
                equivalenceClasses,
                new MaxCardProperty(1L),
                new KeyProperty(ImmutableSet.of()));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable, mktSegmentVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn, mktSegmentVariable, mktSegmentColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    TableScanNode ordersTableScan = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable, shipPriorityVariable),
                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn,
                                    ordersOrderKeyVariable, ordersOrderKeyColumn,
                                    shipPriorityVariable, shipPriorityColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(ordersTableHandle));

                    p.variable(shipPriorityVariable);
                    p.variable(mktSegmentVariable);
                    return p.join(JoinNode.Type.INNER,
                            p.limit(1, ordersTableScan),
                            p.filter(p.rowExpression("c_mktsegment = 'BUILDING'"), customerTableScan),
                            p.rowExpression("o_shippriority = 10"),
                            new JoinNode.EquiJoinClause(ordersCustKeyVariable, customerCustKeyVariable));
                })
                .matches(expectedLogicalProperties);

        // TEST: 1 to n inner join between customers and orders with limit(6) on the right table.
        // orders key property and maxcard=6 and equivalence classes(o_custkey=c_custkey),(shippriority=10) and
        // (mktsegment='BUILDING') should be propagated.
        expectedLogicalProperties = new LogicalPropertiesImpl(
                equivalenceClasses,
                new MaxCardProperty(6L),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(ordersOrderKeyVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable, mktSegmentVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn, mktSegmentVariable, mktSegmentColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    TableScanNode ordersTableScan = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable, shipPriorityVariable),
                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn,
                                    ordersOrderKeyVariable, ordersOrderKeyColumn,
                                    shipPriorityVariable, shipPriorityColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(ordersTableHandle));

                    p.variable(shipPriorityVariable);
                    p.variable(mktSegmentVariable);
                    return p.join(JoinNode.Type.INNER,
                            p.filter(p.rowExpression("c_mktsegment = 'BUILDING'"), customerTableScan),
                            p.limit(6, ordersTableScan),
                            p.rowExpression("o_shippriority = 10"),
                            new JoinNode.EquiJoinClause(customerCustKeyVariable, ordersCustKeyVariable));
                })
                .matches(expectedLogicalProperties);

        // TEST: 1 to n inner join between customers and orders with limit(1) on the right table.
        // Only maxcard=1 should get propagated
        expectedLogicalProperties = new LogicalPropertiesImpl(
                equivalenceClasses,
                new MaxCardProperty(1L),
                new KeyProperty(ImmutableSet.of()));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable, mktSegmentVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn, mktSegmentVariable, mktSegmentColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    TableScanNode ordersTableScan = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable, shipPriorityVariable),
                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn,
                                    ordersOrderKeyVariable, ordersOrderKeyColumn,
                                    shipPriorityVariable, shipPriorityColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(ordersTableHandle));

                    p.variable(shipPriorityVariable);
                    p.variable(mktSegmentVariable);
                    return p.join(JoinNode.Type.INNER,
                            p.filter(p.rowExpression("c_mktsegment = 'BUILDING'"), customerTableScan),
                            p.limit(1, ordersTableScan),
                            p.rowExpression("o_shippriority = 10"),
                            new JoinNode.EquiJoinClause(customerCustKeyVariable, ordersCustKeyVariable));
                })
                .matches(expectedLogicalProperties);

        // TEST: n to 1 left join between orders and customers with limit(7) on the left table.
        // orders keys and maxcard=7 are propagated
        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(7L),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(ordersOrderKeyVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    TableScanNode ordersTableScan = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable),
                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn, ordersOrderKeyVariable, ordersOrderKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(ordersTableHandle));

                    return p.join(JoinNode.Type.LEFT, p.limit(7, ordersTableScan), customerTableScan,
                            new JoinNode.EquiJoinClause(ordersCustKeyVariable, customerCustKeyVariable));
                })
                .matches(expectedLogicalProperties);

        // TEST: n to 1 left join between orders and customers with limit on right table.
        // orders keys are propagated. Maxcard should not be propagated.
        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(ordersOrderKeyVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    TableScanNode ordersTableScan = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable),
                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn, ordersOrderKeyVariable, ordersOrderKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(ordersTableHandle));

                    return p.join(JoinNode.Type.LEFT, ordersTableScan, p.limit(8, customerTableScan),
                            new JoinNode.EquiJoinClause(ordersCustKeyVariable, customerCustKeyVariable));
                })
                .matches(expectedLogicalProperties);

        // TEST: 1 to n right join between customers and orders. Limit(9) on the right table.
        // orders keys are propagated and maxcard=9 should be propagated.
        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(9L),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(ordersOrderKeyVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    TableScanNode ordersTableScan = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable),
                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn, ordersOrderKeyVariable, ordersOrderKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(ordersTableHandle));

                    return p.join(JoinNode.Type.RIGHT, customerTableScan, p.limit(9, ordersTableScan),
                            new JoinNode.EquiJoinClause(customerCustKeyVariable, ordersCustKeyVariable));
                })
                .matches(expectedLogicalProperties);

        // TEST: 1 to n right join between customers and orders. Limit(10) on the left table.
        // orders keys are propagated. maxcard should not be propagated.
        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(ordersOrderKeyVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    TableScanNode ordersTableScan = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable),
                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn, ordersOrderKeyVariable, ordersOrderKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(ordersTableHandle));

                    return p.join(JoinNode.Type.RIGHT, p.limit(10, customerTableScan), ordersTableScan,
                            new JoinNode.EquiJoinClause(customerCustKeyVariable, ordersCustKeyVariable));
                })
                .matches(expectedLogicalProperties);

        // TEST: 1 to n left join between customers and orders - no keys are propagated
        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty());

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    TableScanNode ordersTableScan = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable),
                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn, ordersOrderKeyVariable, ordersOrderKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(ordersTableHandle));

                    return p.join(JoinNode.Type.LEFT, customerTableScan, ordersTableScan,
                            new JoinNode.EquiJoinClause(customerCustKeyVariable, ordersCustKeyVariable));
                })
                .matches(expectedLogicalProperties);

        // TEST: n to m  inner join between customers and orders - concatenated key (orderkey, custkey) should get propagated.
        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(ordersOrderKeyVariable, customerCustKeyVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    TableScanNode ordersTableScan = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable),
                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn, ordersOrderKeyVariable, ordersOrderKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(ordersTableHandle));

                    return p.join(JoinNode.Type.INNER, customerTableScan, ordersTableScan, emptyList(), ImmutableList.of(ordersOrderKeyVariable, customerCustKeyVariable, ordersCustKeyVariable), Optional.empty());
                })
                .matches(expectedLogicalProperties);

        // TEST: n to m  inner join between customers and orders and limit 11 left table and limit 12 on right table.
        // concatenated key (orderkey, custkey) should get propagated. Maxcard should be maxCardLeft * maxCardRight = 132.
        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(132L),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(ordersOrderKeyVariable, customerCustKeyVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    TableScanNode ordersTableScan = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable),
                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn, ordersOrderKeyVariable, ordersOrderKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(ordersTableHandle));

                    return p.join(JoinNode.Type.INNER, p.limit(11, customerTableScan), p.limit(12, ordersTableScan), emptyList(), ImmutableList.of(ordersOrderKeyVariable, ordersCustKeyVariable, customerCustKeyVariable), Optional.empty());
                })
                .matches(expectedLogicalProperties);

        //test m to n cases where there are multiple keys in the left and right tables to concatenate e.g. add unique keys customer.comment and orders.comment
        List<TableConstraint<ColumnHandle>> customerTableConstraints = new ArrayList<>(tester().getTableConstraints(customerTableHandle));
        customerTableConstraints.add(new UniqueConstraint<>(ImmutableSet.of(customerCommentColumn), true, true));

        List<TableConstraint<ColumnHandle>> orderTableConstraints = new ArrayList<>(tester().getTableConstraints(ordersTableHandle));
        orderTableConstraints.add(new UniqueConstraint<>(ImmutableSet.of(ordersCommentColumn), true, true));

        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(ordersOrderKeyVariable, customerCustKeyVariable)),
                        new Key(ImmutableSet.of(customerCommentVariable, ordersCommentVariable)),
                        new Key(ImmutableSet.of(ordersOrderKeyVariable, customerCommentVariable)),
                        new Key(ImmutableSet.of(customerCustKeyVariable, ordersCommentVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable, customerCommentVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn, customerCommentVariable, customerCommentColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            customerTableConstraints);

                    TableScanNode ordersTableScan = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable, ordersCommentVariable),
                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn,
                                    ordersOrderKeyVariable, ordersOrderKeyColumn,
                                    ordersCommentVariable, ordersCommentColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            orderTableConstraints);

                    return p.join(JoinNode.Type.INNER, customerTableScan, ordersTableScan, emptyList(),
                            ImmutableList.of(ordersOrderKeyVariable, ordersCustKeyVariable, ordersCommentVariable,
                                    customerCustKeyVariable, customerCommentVariable),
                            Optional.empty());
                })
                .matches(expectedLogicalProperties);

        // TEST: n to m  inner join between values(1) and values(1) - maxcard(1) should get propagated.
        VariableReferenceExpression c1 = new VariableReferenceExpression(Optional.empty(), "c1", BIGINT);
        VariableReferenceExpression c2 = new VariableReferenceExpression(Optional.empty(), "c2", BIGINT);

        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(1L),
                new KeyProperty());

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    ValuesNode values1 = p.values(1, c1);
                    ValuesNode values2 = p.values(1, c2);
                    return p.join(JoinNode.Type.INNER, values1, values2, emptyList(), ImmutableList.of(c1, c2), Optional.empty());
                })
                .matches(expectedLogicalProperties);

        // TEST: n to m  full join between values(1) and values(1) - maxcard(1) should get propagated.
        VariableReferenceExpression c3 = new VariableReferenceExpression(Optional.empty(), "c1", BIGINT);
        VariableReferenceExpression c4 = new VariableReferenceExpression(Optional.empty(), "c2", BIGINT);

        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(1L),
                new KeyProperty());

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    ValuesNode values1 = p.values(1, c3);
                    ValuesNode values2 = p.values(1, c4);
                    return p.join(JoinNode.Type.FULL, values1, values2, emptyList(), ImmutableList.of(c3, c4), Optional.empty());
                })
                .matches(expectedLogicalProperties);

        // TEST: 1 to n  full join between customers and orders - nothing should get propagated.
        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty());

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    TableScanNode ordersTableScan = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable),
                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn, ordersOrderKeyVariable, ordersOrderKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(ordersTableHandle));

                    return p.join(JoinNode.Type.FULL, customerTableScan, ordersTableScan, emptyList(), ImmutableList.of(customerCustKeyVariable, ordersOrderKeyVariable), Optional.empty());
                })
                .matches(expectedLogicalProperties);

        // TEST: n to 1  full join between customers and orders with limit(12) on left and and limit(10) on right table.
        // The product of the maxcards 120 should get propagated
        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(120L),
                new KeyProperty());

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    TableScanNode ordersTableScan = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable),
                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn, ordersOrderKeyVariable, ordersOrderKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(ordersTableHandle));

                    return p.join(JoinNode.Type.FULL, p.limit(12, customerTableScan), p.limit(10, ordersTableScan),
                            ImmutableList.of(new JoinNode.EquiJoinClause(customerCustKeyVariable, ordersCustKeyVariable)),
                            ImmutableList.of(ordersOrderKeyVariable, customerCustKeyVariable),
                            Optional.empty());
                })
                .matches(expectedLogicalProperties);

        // TEST: n to m  full join between customers and orders with maxcard 2 on left and unknown maxcard on right table.
        // Concatenated keys and a maxcard of unknown should get propagated
        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(ordersOrderKeyVariable, customerCustKeyVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    TableScanNode ordersTableScan = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable),
                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn, ordersOrderKeyVariable, ordersOrderKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(ordersTableHandle));

                    return p.join(JoinNode.Type.INNER, p.limit(2, customerTableScan), ordersTableScan, emptyList(),
                            ImmutableList.of(ordersOrderKeyVariable, ordersCustKeyVariable,
                                    customerCustKeyVariable),
                            Optional.empty());
                })
                .matches(expectedLogicalProperties);

        // TEST: n to m  full join between customers and orders with maxcard 2 on left and unknown maxcard on right table.
        // Concatenated keys and a maxcard of unknown should get propagated
        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(ordersOrderKeyVariable, customerCustKeyVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    TableScanNode ordersTableScan = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable),
                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn, ordersOrderKeyVariable, ordersOrderKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(ordersTableHandle));

                    return p.join(JoinNode.Type.INNER, ordersTableScan, p.limit(2, customerTableScan), emptyList(),
                            ImmutableList.of(ordersOrderKeyVariable, ordersCustKeyVariable,
                                    customerCustKeyVariable),
                            Optional.empty());
                })
                .matches(expectedLogicalProperties);

        // TEST: 1 to 1 inner join between values(1) and customers - maxcard(1) is propagated
        equivalenceClasses = new EquivalenceClassProperty(functionResolution);
        VariableReferenceExpression c = new VariableReferenceExpression(Optional.empty(), "c", BIGINT);
        equivalenceClasses.update(c, customerCustKeyVariable);
        expectedLogicalProperties = new LogicalPropertiesImpl(
                equivalenceClasses,
                new MaxCardProperty(1L),
                new KeyProperty());

        VariableReferenceExpression finalC1 = c;
        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    ValuesNode values = p.values(1, finalC1);

                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    return p.join(JoinNode.Type.INNER, values, customerTableScan,
                            new JoinNode.EquiJoinClause(finalC1, customerCustKeyVariable));
                })
                .matches(expectedLogicalProperties);

        // TEST: 1 to 1 inner join between customers and values(1) - maxcard(1) is propagated
        equivalenceClasses = new EquivalenceClassProperty(functionResolution);
        c = new VariableReferenceExpression(Optional.empty(), "c", BIGINT);
        equivalenceClasses.update(c, customerCustKeyVariable);
        expectedLogicalProperties = new LogicalPropertiesImpl(
                equivalenceClasses,
                new MaxCardProperty(1L),
                new KeyProperty());

        VariableReferenceExpression finalC = c;
        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    ValuesNode values = p.values(1, finalC);

                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    return p.join(JoinNode.Type.INNER, customerTableScan, values,
                            new JoinNode.EquiJoinClause(customerCustKeyVariable, finalC));
                })
                .matches(expectedLogicalProperties);

        // TEST: 1 to n full join between customers and values(1) where n=1 - maxcard(1) is propagated
        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(1L),
                new KeyProperty());

        VariableReferenceExpression finalC2 = c;
        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    ValuesNode values = p.values(1, finalC2);

                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    return p.join(JoinNode.Type.FULL, customerTableScan, values,
                            new JoinNode.EquiJoinClause(customerCustKeyVariable, finalC2));
                })
                .matches(expectedLogicalProperties);

        // TEST: n to 1 full join between values(1) and customers where n=1 - maxcard(1) is propagated
        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(1L),
                new KeyProperty());

        VariableReferenceExpression finalC3 = c;
        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    ValuesNode values = p.values(1, finalC3);
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    return p.join(JoinNode.Type.FULL, values, customerTableScan,
                            new JoinNode.EquiJoinClause(finalC3, customerCustKeyVariable));
                })
                .matches(expectedLogicalProperties);

        // Three table join. Key (l_orderkey, l_linenumber), maxCard=6 and equivalence classes (o_orderkey,l_orderkey) and
        // (o_custkey, c_custkey) should be propagated.
        equivalenceClasses = new EquivalenceClassProperty(functionResolution);
        equivalenceClasses.update(customerCustKeyVariable, ordersCustKeyVariable);
        equivalenceClasses.update(ordersOrderKeyVariable, lineitemOrderkeyVariable);

        expectedLogicalProperties = new LogicalPropertiesImpl(
                equivalenceClasses,
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(lineitemLinenumberVariable, ordersOrderKeyVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable, mktSegmentVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn, mktSegmentVariable, mktSegmentColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    TableScanNode ordersTableScan = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable, shipPriorityVariable),
                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn,
                                    ordersOrderKeyVariable, ordersOrderKeyColumn,
                                    shipPriorityVariable, shipPriorityColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(ordersTableHandle));

                    TableScanNode lineitemTableScan = p.tableScan(
                            lineitemTableHandle,
                            ImmutableList.of(lineitemLinenumberVariable, lineitemOrderkeyVariable),
                            ImmutableMap.of(lineitemLinenumberVariable, lineitemLinenumberColumn,
                                    lineitemOrderkeyVariable, lineitemOrderkeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(lineitemTableHandle));

                    JoinNode customerOrderJoin = p.join(JoinNode.Type.INNER,
                            customerTableScan,
                            p.limit(6, ordersTableScan),
                            new JoinNode.EquiJoinClause(customerCustKeyVariable, ordersCustKeyVariable));

                    return p.join(JoinNode.Type.INNER,
                            customerOrderJoin,
                            lineitemTableScan,
                            new JoinNode.EquiJoinClause(ordersOrderKeyVariable, lineitemOrderkeyVariable));
                })
                .matches(expectedLogicalProperties);
    }

    @Test
    public void testSemiJoinNodeLogicalProperties()
    {
        EquivalenceClassProperty equivalenceClasses = new EquivalenceClassProperty(functionResolution);
        equivalenceClasses.update(ordersCustKeyVariable, customerCustKeyVariable);

        LogicalProperties expectedLogicalProperties = new LogicalPropertiesImpl(new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(ordersOrderKeyVariable)))));

        // Test Logical properties generated for semi join node. It just propagates its non-filtering source's properties.
        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    TableScanNode ordersTableScan = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable),
                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn, ordersOrderKeyVariable, ordersOrderKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(ordersTableHandle));

                    return p.semiJoin(ordersCustKeyVariable, customerCustKeyVariable, ordersOrderKeyVariable,
                            Optional.empty(), Optional.empty(), ordersTableScan, customerTableScan);
                })
                .matches(expectedLogicalProperties);

        //source table is 1-tuple
        expectedLogicalProperties = new LogicalPropertiesImpl(new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(1L),
                new KeyProperty());

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    ValuesNode semiJoinSource = p.values(1, ordersCustKeyVariable);
                    TableScanNode semiJoinFilteringSource = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    return p.semiJoin(ordersCustKeyVariable, customerCustKeyVariable, ordersOrderKeyVariable,
                            Optional.empty(), Optional.empty(), semiJoinSource, semiJoinFilteringSource);
                })
                .matches(expectedLogicalProperties);

        //maxcard derived from limit propagates semijoin
        EquivalenceClassProperty equivalenceClassProperty = new EquivalenceClassProperty(functionResolution);
        equivalenceClassProperty.update(ordersOrderPriorityVariable, constant(Slices.utf8Slice("URGENT"), createVarcharType(6)));

        expectedLogicalProperties = new LogicalPropertiesImpl(equivalenceClassProperty,
                new MaxCardProperty(5L),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(ordersOrderKeyVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode semiJoinFilteringSource = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    p.variable(ordersOrderPriorityVariable);
                    LimitNode semiJoinSource = p.limit(5,
                            p.filter(p.rowExpression("o_orderpriority = 'URGENT'"),
                                    p.tableScan(
                                            ordersTableHandle,
                                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable),
                                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn,
                                                    ordersOrderKeyVariable, ordersOrderKeyColumn,
                                                    ordersOrderPriorityVariable, ordersOrderPriorityColumn),
                                            TupleDomain.none(),
                                            TupleDomain.none(),
                                            tester().getTableConstraints(ordersTableHandle))));

                    return p.semiJoin(ordersCustKeyVariable, customerCustKeyVariable, ordersOrderKeyVariable,
                            Optional.empty(), Optional.empty(), semiJoinSource, semiJoinFilteringSource);
                })
                .matches(expectedLogicalProperties);
    }

    @Test
    public void testAggregationNodeLogicalProperties()
    {
        // Aggregation node adds new key (nationkey)
        LogicalProperties expectedLogicalProperties = new LogicalPropertiesImpl(new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(customerNationKeyVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(customerNationKeyVariable)
                        .source(p.tableScan(
                                customerTableHandle,
                                ImmutableList.of(customerNationKeyVariable),
                                ImmutableMap.of(customerNationKeyVariable, customerNationKeyColumn),
                                TupleDomain.none(),
                                TupleDomain.none(),
                                emptyList()))))
                .matches(expectedLogicalProperties);

        //INVARIANT: Grouping on (nationkey, custkey) but (custkey) is already a key. So grouping result should have only (custkey)
        expectedLogicalProperties = new LogicalPropertiesImpl(new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(customerCustKeyVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(customerCustKeyVariable, customerNationKeyVariable)
                        .source(p.tableScan(
                                customerTableHandle,
                                ImmutableList.of(customerCustKeyVariable),
                                ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                                TupleDomain.none(),
                                TupleDomain.none(),
                                tester().getTableConstraints(customerTableHandle)))))
                .matches(expectedLogicalProperties);

        //INVARIANT. Group by nationkey Filter binds nationkey to a constant before grouping. Result should have maxcard=1;
        EquivalenceClassProperty equivalenceClassProperty = new EquivalenceClassProperty(functionResolution);
        equivalenceClassProperty.update(customerNationKeyVariable, constant(20L, BIGINT));
        expectedLogicalProperties = new LogicalPropertiesImpl(equivalenceClassProperty,
                new MaxCardProperty(1L),
                new KeyProperty());

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    p.variable(customerNationKeyVariable);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(customerNationKeyVariable)
                            .source(p.filter(p.rowExpression("nationkey = BIGINT '20'"),
                                    p.tableScan(
                                            customerTableHandle,
                                            ImmutableList.of(customerNationKeyVariable),
                                            ImmutableMap.of(customerNationKeyVariable, customerNationKeyColumn),
                                            TupleDomain.none(),
                                            TupleDomain.none(),
                                            emptyList()))));
                })
                .matches(expectedLogicalProperties);

        //INVARIANT. Group on (nationkey, mktsegment) and after first binding "mktsegment" to a constant. The grouping result should have key (nationkey)
        EquivalenceClassProperty equivalenceClassProperty1 = new EquivalenceClassProperty(functionResolution);
        equivalenceClassProperty1.update(mktSegmentVariable, constant(Slices.utf8Slice("BUILDING"), createVarcharType(8)));
        expectedLogicalProperties = new LogicalPropertiesImpl(equivalenceClassProperty1,
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(customerNationKeyVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    p.variable(mktSegmentVariable);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(customerNationKeyVariable, mktSegmentVariable)
                            .source(p.filter(p.rowExpression("c_mktsegment = 'BUILDING'"),
                                    p.tableScan(
                                            customerTableHandle,
                                            ImmutableList.of(customerNationKeyVariable),
                                            ImmutableMap.of(customerNationKeyVariable, customerNationKeyColumn, mktSegmentVariable, mktSegmentColumn),
                                            TupleDomain.none(),
                                            TupleDomain.none(),
                                            tester().getTableConstraints(customerTableHandle)))));
                })
                .matches(expectedLogicalProperties);

        //INVARIANT. Group by with aggregate functions but no grouping columns. Maxard should be 1 and no keys propagated.
        expectedLogicalProperties = new LogicalPropertiesImpl(new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(1L),
                new KeyProperty());

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    p.variable(customerNationKeyVariable);
                    return p.aggregation(builder -> builder
                            .addAggregation(p.variable("count_nk"), p.rowExpression("count(nationkey)"))
                            .globalGrouping()
                            .source(p.tableScan(
                                    customerTableHandle,
                                    ImmutableList.of(customerNationKeyVariable),
                                    ImmutableMap.of(customerNationKeyVariable, customerNationKeyColumn, mktSegmentVariable, mktSegmentColumn),
                                    TupleDomain.none(),
                                    TupleDomain.none(),
                                    tester().getTableConstraints(customerTableHandle))));
                })
                .matches(expectedLogicalProperties);

        //INVARIANT. Maxcard is set to 1 prior to group by with grouping columns. Maxard of group by should be 1 and no keys propagated.
        expectedLogicalProperties = new LogicalPropertiesImpl(new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(1L),
                new KeyProperty());

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    p.variable(customerNationKeyVariable);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(customerNationKeyVariable)
                            .source(p.limit(1, p.tableScan(
                                    customerTableHandle,
                                    ImmutableList.of(customerNationKeyVariable),
                                    ImmutableMap.of(customerNationKeyVariable, customerNationKeyColumn),
                                    TupleDomain.none(),
                                    TupleDomain.none(),
                                    emptyList()))));
                })
                .matches(expectedLogicalProperties);

        // Test propagation of equivalence classes through aggregation.
        // None of the equivalence classes from aggregation's source node should be propagated since none of the
        // members are projected by the aggregation node.
        // Key property (shippriority, linenumber) which form the group by keys and maxcard=6 should be propagated.

        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(shipPriorityVariable, lineitemLinenumberVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable, mktSegmentVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn, mktSegmentVariable, mktSegmentColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    TableScanNode ordersTableScan = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable, shipPriorityVariable),
                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn,
                                    ordersOrderKeyVariable, ordersOrderKeyColumn,
                                    shipPriorityVariable, shipPriorityColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(ordersTableHandle));

                    TableScanNode lineitemTableScan = p.tableScan(
                            lineitemTableHandle,
                            ImmutableList.of(lineitemLinenumberVariable, lineitemOrderkeyVariable),
                            ImmutableMap.of(lineitemLinenumberVariable, lineitemLinenumberColumn,
                                    lineitemOrderkeyVariable, lineitemOrderkeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(lineitemTableHandle));

                    JoinNode customerOrderJoin = p.join(JoinNode.Type.INNER,
                            customerTableScan,
                            p.limit(6, ordersTableScan),
                            new JoinNode.EquiJoinClause(customerCustKeyVariable, ordersCustKeyVariable));

                    p.variable(lineitemExtendedPriceVariable);
                    return p.aggregation(builder -> builder
                            .addAggregation(p.variable("sum_price", DOUBLE), p.rowExpression("sum(l_extendedprice)"))
                            .singleGroupingSet(lineitemLinenumberVariable, shipPriorityVariable)
                            .source(p.join(JoinNode.Type.INNER,
                                    customerOrderJoin,
                                    lineitemTableScan,
                                    new JoinNode.EquiJoinClause(ordersOrderKeyVariable, lineitemOrderkeyVariable))));
                })
                .matches(expectedLogicalProperties);

        // A variation to the above case, where in groupby keys are (l_lineitem,o_orderkey,shippriority). Since
        // (o_orderkey, l_lineitem) are already a key, the key should be normalized to have only (o_orderkey, l_lineitem).
        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(ordersOrderKeyVariable, lineitemLinenumberVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable, mktSegmentVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn, mktSegmentVariable, mktSegmentColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    TableScanNode ordersTableScan = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable, shipPriorityVariable),
                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn,
                                    ordersOrderKeyVariable, ordersOrderKeyColumn,
                                    shipPriorityVariable, shipPriorityColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(ordersTableHandle));

                    TableScanNode lineitemTableScan = p.tableScan(
                            lineitemTableHandle,
                            ImmutableList.of(lineitemLinenumberVariable, lineitemOrderkeyVariable),
                            ImmutableMap.of(lineitemLinenumberVariable, lineitemLinenumberColumn,
                                    lineitemOrderkeyVariable, lineitemOrderkeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(lineitemTableHandle));

                    JoinNode customerOrderJoin = p.join(JoinNode.Type.INNER,
                            customerTableScan,
                            p.limit(6, ordersTableScan),
                            new JoinNode.EquiJoinClause(customerCustKeyVariable, ordersCustKeyVariable));

                    p.variable(lineitemExtendedPriceVariable);
                    return p.aggregation(builder -> builder
                            .addAggregation(p.variable("sum_price", DOUBLE), p.rowExpression("sum(l_extendedprice)"))
                            .singleGroupingSet(lineitemLinenumberVariable, ordersOrderKeyVariable, shipPriorityVariable)
                            .source(p.join(JoinNode.Type.INNER,
                                    customerOrderJoin,
                                    lineitemTableScan,
                                    new JoinNode.EquiJoinClause(ordersOrderKeyVariable, lineitemOrderkeyVariable))));
                })
                .matches(expectedLogicalProperties);
    }

    @Test
    void testAssignUniqueIdNodeLogicalProperties()
    {
        VariableReferenceExpression c = new VariableReferenceExpression(Optional.empty(), "c", BIGINT);
        VariableReferenceExpression unique = new VariableReferenceExpression(Optional.empty(), "unique", BIGINT);

        LogicalProperties expectedLogicalProperties = new LogicalPropertiesImpl(new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(5L),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(unique)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> p.assignUniqueId(unique,
                        p.values(5, c)))
                .matches(expectedLogicalProperties);
    }

    @Test
    void testDistinctLimitNodeLogicalProperties()
    {
        VariableReferenceExpression c = new VariableReferenceExpression(Optional.empty(), "c", BIGINT);

        LogicalProperties expectedLogicalProperties = new LogicalPropertiesImpl(new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(3L),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(c)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> p.distinctLimit(3, ImmutableList.of(c), p.values(5, c)))
                .matches(expectedLogicalProperties);

        //Tests where where DistinctLimit adds a key! Mirror the aggregation tests.

        // DistinctLimit node adds new key (nationkey)
        expectedLogicalProperties = new LogicalPropertiesImpl(new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(5L),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(customerNationKeyVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> p.distinctLimit(5, ImmutableList.of(customerNationKeyVariable),
                        p.tableScan(
                                customerTableHandle,
                                ImmutableList.of(customerNationKeyVariable),
                                ImmutableMap.of(customerNationKeyVariable, customerNationKeyColumn),
                                TupleDomain.none(),
                                TupleDomain.none(),
                                emptyList())))
                .matches(expectedLogicalProperties);

        //INVARIANT: DistinctLimit on (nationkey, custkey) but (custkey) is already a key. So grouping result should have only (custkey)
        expectedLogicalProperties = new LogicalPropertiesImpl(new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(6L),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(customerCustKeyVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> p.distinctLimit(6, ImmutableList.of(customerCustKeyVariable, customerNationKeyVariable),
                        p.tableScan(
                                customerTableHandle,
                                ImmutableList.of(customerCustKeyVariable),
                                ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                                TupleDomain.none(),
                                TupleDomain.none(),
                                tester().getTableConstraints(customerTableHandle))))
                .matches(expectedLogicalProperties);

        //INVARIANT. DistinctLimit with nationkey as distinct symbol.
        // Filter binds nationkey to a constant before grouping. Result should have maxcard=1;
        EquivalenceClassProperty equivalenceClassProperty = new EquivalenceClassProperty(functionResolution);
        equivalenceClassProperty.update(customerNationKeyVariable, constant(20L, BIGINT));
        expectedLogicalProperties = new LogicalPropertiesImpl(equivalenceClassProperty,
                new MaxCardProperty(1L),
                new KeyProperty());

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    p.variable(customerNationKeyVariable);
                    return p.distinctLimit(6, ImmutableList.of(customerNationKeyVariable),
                            p.filter(p.rowExpression("nationkey = BIGINT '20'"),
                                    p.tableScan(
                                            customerTableHandle,
                                            ImmutableList.of(customerNationKeyVariable),
                                            ImmutableMap.of(customerNationKeyVariable, customerNationKeyColumn),
                                            TupleDomain.none(),
                                            TupleDomain.none(),
                                            emptyList())));
                })
                .matches(expectedLogicalProperties);

        //INVARIANT. DistinctLimit with (nationkey, mktsegment) as symbols and after first binding "mktsegment" to a constant.
        // The grouping result should have key (nationkey)
        EquivalenceClassProperty equivalenceClassProperty1 = new EquivalenceClassProperty(functionResolution);
        equivalenceClassProperty1.update(mktSegmentVariable, constant(Slices.utf8Slice("BUILDING"), createVarcharType(8)));
        expectedLogicalProperties = new LogicalPropertiesImpl(equivalenceClassProperty1,
                new MaxCardProperty(7L),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(customerNationKeyVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    p.variable(mktSegmentVariable);
                    return p.distinctLimit(7, ImmutableList.of(customerNationKeyVariable, mktSegmentVariable),
                            p.filter(p.rowExpression("c_mktsegment = 'BUILDING'"),
                                    p.tableScan(
                                            customerTableHandle,
                                            ImmutableList.of(customerNationKeyVariable),
                                            ImmutableMap.of(customerNationKeyVariable, customerNationKeyColumn, mktSegmentVariable, mktSegmentColumn),
                                            TupleDomain.none(),
                                            TupleDomain.none(),
                                            tester().getTableConstraints(customerTableHandle))));
                })
                .matches(expectedLogicalProperties);

        //INVARIANT. Maxcard is set to 1 prior to distinct limit. Maxard of distinct limit should be 1 and no keys propagated.
        expectedLogicalProperties = new LogicalPropertiesImpl(new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(1L),
                new KeyProperty());

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    p.variable(customerNationKeyVariable);
                    return p.distinctLimit(8, ImmutableList.of(customerNationKeyVariable),
                            p.limit(1, p.tableScan(
                                    customerTableHandle,
                                    ImmutableList.of(customerNationKeyVariable),
                                    ImmutableMap.of(customerNationKeyVariable, customerNationKeyColumn),
                                    TupleDomain.none(),
                                    TupleDomain.none(),
                                    emptyList())));
                })
                .matches(expectedLogicalProperties);

        //test cases where the DistinctLimit count is 1 and results in maxcard 1
        expectedLogicalProperties = new LogicalPropertiesImpl(new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(1L),
                new KeyProperty());

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    p.variable(customerNationKeyVariable);
                    return p.distinctLimit(1, ImmutableList.of(customerNationKeyVariable),
                            p.tableScan(
                                    customerTableHandle,
                                    ImmutableList.of(customerNationKeyVariable),
                                    ImmutableMap.of(customerNationKeyVariable, customerNationKeyColumn),
                                    TupleDomain.none(),
                                    TupleDomain.none(),
                                    emptyList()));
                })
                .matches(expectedLogicalProperties);
    }

    @Test
    public void testLimitNodeLogicalProperties()
    {
        EquivalenceClassProperty equivalenceClasses = new EquivalenceClassProperty(functionResolution);
        equivalenceClasses.update(ordersCustKeyVariable, customerCustKeyVariable);

        LogicalProperties expectedLogicalProperties = new LogicalPropertiesImpl(
                equivalenceClasses,
                new MaxCardProperty(6L),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(ordersOrderKeyVariable)))));

        // Test Logical properties generated for the Limit node. It updates the MaxCardProperty from the source properties.
        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    TableScanNode ordersTableScan = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable),
                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn, ordersOrderKeyVariable, ordersOrderKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(ordersTableHandle));

                    JoinNode ordersCustomerJoin = p.join(JoinNode.Type.INNER, ordersTableScan, customerTableScan,
                            new JoinNode.EquiJoinClause(ordersCustKeyVariable, customerCustKeyVariable));

                    return p.limit(6, ordersCustomerJoin);
                })
                .matches(expectedLogicalProperties);

        //Do a variation of the previous test where the innerjoin(TopN(orders), customer). The maxcard(5) should propagate.
        expectedLogicalProperties = new LogicalPropertiesImpl(
                equivalenceClasses,
                new MaxCardProperty(5L),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(ordersOrderKeyVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    TableScanNode ordersTableScan = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable),
                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn, ordersOrderKeyVariable, ordersOrderKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(ordersTableHandle));

                    return p.join(JoinNode.Type.INNER, p.limit(5, ordersTableScan), customerTableScan,
                            new JoinNode.EquiJoinClause(ordersCustKeyVariable, customerCustKeyVariable));
                })
                .matches(expectedLogicalProperties);

        //INVARIANT: maxcard is set to K (by Values or Filter) and TopN and/or Limit comes along and tries to set it to N>K. Should still be set to K.
        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(5L),
                new KeyProperty());

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> p.limit(10, p.values(5, p.variable("c"))))
                .matches(expectedLogicalProperties);

        //INVARIANT: maxcard is set to K (by Values or Filter) and TopN and/or Limit comes along and tries to set it to N<K. Should still be set to N.
        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(6L),
                new KeyProperty());

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> p.limit(6, p.values(10, p.variable("c"))))
                .matches(expectedLogicalProperties);

        //INVARIANT: TableScan with key (A) and TopN and/or Limit sets result N=1. Key property should be emptied.
        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(1L),
                new KeyProperty());

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    return p.limit(1, customerTableScan);
                })
                .matches(expectedLogicalProperties);
    }

    @Test
    public void testTopNNodeLogicalProperties()
    {
        //just duplicate the comprehensive limit tests but also do negative tests for the case where TopN is not final
        EquivalenceClassProperty equivalenceClasses = new EquivalenceClassProperty(functionResolution);
        equivalenceClasses.update(ordersCustKeyVariable, customerCustKeyVariable);

        LogicalProperties expectedLogicalProperties = new LogicalPropertiesImpl(
                equivalenceClasses,
                new MaxCardProperty(6L),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(ordersOrderKeyVariable)))));

        // Test Logical properties generated for the TopN node. It updates the MaxCardProperty from the source properties.
        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    TableScanNode ordersTableScan = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable),
                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn, ordersOrderKeyVariable, ordersOrderKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(ordersTableHandle));

                    JoinNode ordersCustomerJoin = p.join(JoinNode.Type.INNER, ordersTableScan, customerTableScan,
                            new JoinNode.EquiJoinClause(ordersCustKeyVariable, customerCustKeyVariable));

                    return p.topN(6, ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable),
                            ordersCustomerJoin);
                })
                .matches(expectedLogicalProperties);

        //Variation of the previous test where the innerjoin(TopN(orders), customer). The maxcard(5) should propagate.
        expectedLogicalProperties = new LogicalPropertiesImpl(
                equivalenceClasses,
                new MaxCardProperty(5L),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(ordersOrderKeyVariable)))));

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    TableScanNode ordersTableScan = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustKeyVariable, ordersOrderKeyVariable),
                            ImmutableMap.of(ordersCustKeyVariable, ordersCustKeyColumn, ordersOrderKeyVariable, ordersOrderKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(ordersTableHandle));

                    return p.join(JoinNode.Type.INNER, p.topN(5, ImmutableList.of(ordersCustKeyVariable), ordersTableScan), customerTableScan,
                            new JoinNode.EquiJoinClause(ordersCustKeyVariable, customerCustKeyVariable));
                })
                .matches(expectedLogicalProperties);

        //INVARIANT: maxcard is set to K (by Values or Filter) and TopN comes along and tries to set it to N>K. Should still be set to K.
        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(5L),
                new KeyProperty());

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    VariableReferenceExpression var = p.variable("c");
                    return p.topN(10, ImmutableList.of(var), p.values(5, var));
                })
                .matches(expectedLogicalProperties);

        //INVARIANT: maxcard is set to K (by Values or Filter) and TopN and/or Limit comes along and tries to set it to N<K. Should still be set to N.
        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(6L),
                new KeyProperty());

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    VariableReferenceExpression var = p.variable("c");
                    return p.topN(6, ImmutableList.of(var), p.values(10, var));
                })
                .matches(expectedLogicalProperties);

        //INVARIANT: TableScan with key (A) and TopN and/or Limit sets result N=1. Key property should be emptied.
        expectedLogicalProperties = new LogicalPropertiesImpl(
                new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(1L),
                new KeyProperty());

        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode customerTableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));

                    return p.topN(1, ImmutableList.of(customerCustKeyVariable), customerTableScan);
                })
                .matches(expectedLogicalProperties);
    }

    @Test
    void testSortNodeLogicalProperties()
    {
        // Test KeyProperty propagation through sort.
        LogicalProperties expectedLogicalProperties = new LogicalPropertiesImpl(new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty(ImmutableSet.of(new Key(ImmutableSet.of(customerCustKeyVariable)))));
        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode tableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));
                    return p.sort(ImmutableList.of(customerCustKeyVariable), tableScan);
                })
                .matches(expectedLogicalProperties);

        //TEST: Propagate maxcard through the filter below the sort
        ConstantExpression constExpr = new ConstantExpression(100L, BIGINT);
        EquivalenceClassProperty equivalenceClasses = new EquivalenceClassProperty(functionResolution);
        equivalenceClasses.update(customerCustKeyVariable, constExpr);

        expectedLogicalProperties = new LogicalPropertiesImpl(equivalenceClasses,
                new MaxCardProperty(1L),
                new KeyProperty());
        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    TableScanNode tableScan = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustKeyVariable),
                            ImmutableMap.of(customerCustKeyVariable, customerCustKeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester().getTableConstraints(customerTableHandle));
                    p.variable(customerCustKeyVariable);
                    FilterNode filterNode = p.filter(p.rowExpression("c_custkey = BIGINT '100'"), tableScan);
                    return p.sort(ImmutableList.of(customerCustKeyVariable), filterNode);
                })
                .matches(expectedLogicalProperties);
    }

    @Test
    public void testDefaultLogicalProperties()
    {
        LogicalProperties defaultLogicalProperties = new LogicalPropertiesImpl(new EquivalenceClassProperty(functionResolution),
                new MaxCardProperty(),
                new KeyProperty());

        //Union node should not propagate any logical properties
        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> p.union(
                        ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                .putAll(p.variable("o1"), p.variable("s1_c1"), p.variable("s2_c1"))
                                .putAll(p.variable("o2"), p.variable("s1_c2"), p.variable("s2_c2"))
                                .build(),
                        ImmutableList.of(
                                p.values(1, p.variable("s1_c1"), p.variable("s1_c2")),
                                p.values(2, p.variable("s2_c1"), p.variable("s2_c2")))))
                .matches(defaultLogicalProperties);

        //Intersect node should not propagate any logical properties
        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> p.intersect(
                        ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                .putAll(p.variable("o1"), p.variable("s1_c1"), p.variable("s2_c1"))
                                .putAll(p.variable("o2"), p.variable("s1_c2"), p.variable("s2_c2"))
                                .build(),
                        ImmutableList.of(
                                p.values(1, p.variable("s1_c1"), p.variable("s1_c2")),
                                p.values(2, p.variable("s2_c1"), p.variable("s2_c2")))))
                .matches(defaultLogicalProperties);

        //Lateral node should not propagate any logical properties
        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> p.lateral(
                        ImmutableList.of(p.variable("a")),
                        p.values(p.variable("a")),
                        p.values(p.variable("a"))))
                .matches(defaultLogicalProperties);

        //MarkDistinct node should not propagate any logical properties
        tester().assertThat(new NoOpRule(), logicalPropertiesProvider)
                .on(p -> {
                    VariableReferenceExpression key = p.variable("key");
                    VariableReferenceExpression mark = p.variable("mark");
                    return p.markDistinct(mark, ImmutableList.of(key), p.values(10, key));
                })
                .matches(defaultLogicalProperties);
    }

    private static class NoOpRule
            implements Rule<PlanNode>
    {
        private final Pattern pattern = Pattern.any();

        @Override
        public Pattern<PlanNode> getPattern()
        {
            return pattern;
        }

        @Override
        public Result apply(PlanNode node, Captures captures, Context context)
        {
            return Result.ofPlanNode(node);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("pattern", pattern)
                    .toString();
        }
    }
}
