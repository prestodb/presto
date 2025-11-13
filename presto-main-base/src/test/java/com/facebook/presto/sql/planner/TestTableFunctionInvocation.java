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

import com.facebook.presto.connector.tvf.TestTVFConnectorFactory;
import com.facebook.presto.connector.tvf.TestTVFConnectorPlugin;
import com.facebook.presto.connector.tvf.TestingTableFunctions;
import com.facebook.presto.connector.tvf.TestingTableFunctions.DescriptorArgumentFunction;
import com.facebook.presto.connector.tvf.TestingTableFunctions.DifferentArgumentTypesFunction;
import com.facebook.presto.connector.tvf.TestingTableFunctions.TestingTableFunctionHandle;
import com.facebook.presto.connector.tvf.TestingTableFunctions.TwoScalarArgumentsFunction;
import com.facebook.presto.connector.tvf.TestingTableFunctions.TwoTableArgumentsFunction;
import com.facebook.presto.spi.connector.TableFunctionApplicationResult;
import com.facebook.presto.spi.function.table.Descriptor;
import com.facebook.presto.spi.function.table.Descriptor.Field;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.RowNumberSymbolMatcher;
import com.facebook.presto.sql.planner.plan.TableFunctionProcessorNode;
import com.facebook.presto.sql.tree.LongLiteral;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.Optimizer.PlanStage.CREATED;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.specification;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableFunction;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictOutput;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableFunctionProcessor;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.assertions.TableFunctionMatcher.DescriptorArgumentValue.descriptorArgument;
import static com.facebook.presto.sql.planner.assertions.TableFunctionMatcher.DescriptorArgumentValue.nullDescriptor;
import static com.facebook.presto.sql.planner.assertions.TableFunctionMatcher.TableArgumentValue.Builder.tableArgument;

public class TestTableFunctionInvocation
        extends BasePlanTest
{
    private static final String TESTING_CATALOG = "test";

    @BeforeClass
    public final void setup()
    {
        getQueryRunner().installPlugin(new TestTVFConnectorPlugin(TestTVFConnectorFactory.builder()
                .withTableFunctions(ImmutableSet.of(
                        new DifferentArgumentTypesFunction(),
                        new TwoScalarArgumentsFunction(),
                        new TwoTableArgumentsFunction(),
                        new DescriptorArgumentFunction(),
                        new TestingTableFunctions.PassThroughFunction()))
                .withApplyTableFunction((session, handle) -> {
                    if (handle instanceof TestingTableFunctionHandle) {
                        TestingTableFunctionHandle functionHandle = (TestingTableFunctionHandle) handle;
                        return Optional.of(new TableFunctionApplicationResult<>(functionHandle.getTableHandle(), functionHandle.getTableHandle().getColumns().orElseThrow(() -> new IllegalStateException("Missing columns"))));
                    }
                    throw new IllegalStateException("Unsupported table function handle: " + handle.getClass().getSimpleName());
                })
                .build()));
        getQueryRunner().createCatalog(TESTING_CATALOG, "testTVF", ImmutableMap.of());
    }

    @Test
    public void testTableFunctionInitialPlan()
    {
        assertPlan(
                "SELECT * FROM TABLE(test.system.different_arguments_function(" +
                        "INPUT_1 => TABLE(SELECT 'a') t1(c1) PARTITION BY c1 ORDER BY c1," +
                        "INPUT_3 => TABLE(SELECT 'b') t3(c3) PARTITION BY c3," +
                        "INPUT_2 => TABLE(VALUES 1) t2(c2)," +
                        "ID => BIGINT '2001'," +
                        "LAYOUT => DESCRIPTOR (x boolean, y bigint)" +
                        "COPARTITION (t1, t3))) t",
                CREATED,
                anyTree(tableFunction(builder -> builder
                                .name("different_arguments_function")
                                .addTableArgument(
                                        "INPUT_1",
                                        tableArgument(0)
                                                .specification(specification(ImmutableList.of("c1"), ImmutableList.of("c1"), ImmutableMap.of("c1", ASC_NULLS_LAST)))
                                                .passThroughVariables(ImmutableSet.of("c1"))
                                                .passThroughColumns())
                                .addTableArgument(
                                        "INPUT_3",
                                        tableArgument(2)
                                                .specification(specification(ImmutableList.of("c3"), ImmutableList.of(), ImmutableMap.of()))
                                                .pruneWhenEmpty()
                                                .passThroughVariables(ImmutableSet.of("c3")))
                                .addTableArgument(
                                        "INPUT_2",
                                        tableArgument(1)
                                                .rowSemantics()
                                                .passThroughVariables(ImmutableSet.of("c2"))
                                                .passThroughColumns())
                                .addScalarArgument("ID", 2001L)
                                .addDescriptorArgument(
                                        "LAYOUT",
                                        descriptorArgument(new Descriptor(ImmutableList.of(
                                                new Field("X", Optional.of(BOOLEAN)),
                                                new Field("Y", Optional.of(BIGINT))))))
                                .addCopartitioning(ImmutableList.of("INPUT_1", "INPUT_3"))
                                .properOutputs(ImmutableList.of("OUTPUT")),
                        anyTree(project(ImmutableMap.of("c1", expression("'a'")), values(1))),
                        anyTree(values(ImmutableList.of("c2"), ImmutableList.of(ImmutableList.of(new LongLiteral("1"))))),
                        anyTree(project(ImmutableMap.of("c3", expression("'b'")), values(1))))));
    }

    @Test
    public void testTableFunctionInitialPlanWithCoercionForCopartitioning()
    {
        assertPlan("SELECT * FROM TABLE(test.system.two_table_arguments_function(" +
                        "INPUT1 => TABLE(VALUES SMALLINT '1') t1(c1) PARTITION BY c1," +
                        "INPUT2 => TABLE(VALUES INTEGER '2') t2(c2) PARTITION BY c2 " +
                        "COPARTITION (t1, t2))) t",
                CREATED,
                anyTree(tableFunction(builder -> builder
                                .name("two_table_arguments_function")
                                .addTableArgument(
                                        "INPUT1",
                                        tableArgument(0)
                                                .specification(specification(ImmutableList.of("c1_coerced"), ImmutableList.of(), ImmutableMap.of()))
                                                .passThroughVariables(ImmutableSet.of("c1")))
                                .addTableArgument(
                                        "INPUT2",
                                        tableArgument(1)
                                                .specification(specification(ImmutableList.of("c2"), ImmutableList.of(), ImmutableMap.of()))
                                                .passThroughVariables(ImmutableSet.of("c2")))
                                .addCopartitioning(ImmutableList.of("INPUT1", "INPUT2"))
                                .properOutputs(ImmutableList.of("COLUMN")),
                        project(ImmutableMap.of("c1_coerced", expression("CAST(c1 AS INTEGER)")),
                                anyTree(values(ImmutableList.of("c1"), ImmutableList.of(ImmutableList.of(new LongLiteral("1")))))),
                        anyTree(values(ImmutableList.of("c2"), ImmutableList.of(ImmutableList.of(new LongLiteral("2"))))))));
    }

    @Test
    public void testNullScalarArgument()
    {
        // the argument NUMBER has null default value
        assertPlan(
                " SELECT * FROM TABLE(test.system.two_scalar_arguments_function(TEXT => null))",
                CREATED,
                anyTree(tableFunction(builder -> builder
                        .name("two_scalar_arguments_function")
                        .addScalarArgument("TEXT", null)
                        .addScalarArgument("NUMBER", null)
                        .properOutputs(ImmutableList.of("OUTPUT")))));
    }

    @Test
    public void testNullDescriptorArgument()
    {
        assertPlan(
                " SELECT * FROM TABLE(test.system.descriptor_argument_function(SCHEMA => CAST(null AS DESCRIPTOR)))",
                CREATED,
                anyTree(tableFunction(builder -> builder
                        .name("descriptor_argument_function")
                        .addDescriptorArgument("SCHEMA", nullDescriptor())
                        .properOutputs(ImmutableList.of("OUTPUT")))));

        // the argument SCHEMA has null default value
        assertPlan(
                " SELECT * FROM TABLE(test.system.descriptor_argument_function())",
                CREATED,
                anyTree(tableFunction(builder -> builder
                        .name("descriptor_argument_function")
                        .addDescriptorArgument("SCHEMA", nullDescriptor())
                        .properOutputs(ImmutableList.of("OUTPUT")))));
    }

    @Test
    public void testPruneTableFunctionColumns()
    {
        // all table function outputs are referenced with SELECT *, no pruning
        assertPlan("SELECT * FROM TABLE(test.system.pass_through_function(input => TABLE(SELECT 1, true) t(a, b)))",
                strictOutput(
                        ImmutableList.of("x", "a", "b"),
                        tableFunctionProcessor(
                                builder -> builder
                                        .name("pass_through_function")
                                        .properOutputs(ImmutableList.of("x"))
                                        .passThroughSymbols(
                                                ImmutableList.of(ImmutableList.of("a", "b")))
                                        .requiredSymbols(ImmutableList.of(ImmutableList.of("a")))
                                        .specification(specification(ImmutableList.of(), ImmutableList.of(), ImmutableMap.of())),
                                project(ImmutableMap.of("a", expression("INTEGER'1'"), "b", expression("BOOLEAN'true'")), values(1)))));

        // no table function outputs are referenced. All pass-through symbols are pruned from the TableFunctionProcessorNode. The unused symbol "b" is pruned from the source values node.
        assertPlan("SELECT 'constant' c FROM TABLE(test.system.pass_through_function(input => TABLE(SELECT 1, true) t(a, b)))",
                strictOutput(
                        ImmutableList.of("c"),
                        strictProject(
                                ImmutableMap.of("c", expression("VARCHAR'constant'")),
                                tableFunctionProcessor(
                                        builder -> builder
                                                .name("pass_through_function")
                                                .properOutputs(ImmutableList.of("x"))
                                                .passThroughSymbols(ImmutableList.of(ImmutableList.of()))
                                                .requiredSymbols(ImmutableList.of(ImmutableList.of("a")))
                                                .specification(specification(ImmutableList.of(), ImmutableList.of(), ImmutableMap.of())),
                                        project(ImmutableMap.of("a", expression("INTEGER'1'")), values(1))))));
    }

    @Test
    public void testRemoveRedundantTableFunction()
    {
        assertPlan("SELECT * FROM TABLE(test.system.pass_through_function(input => TABLE(SELECT 1, true WHERE false) t(a, b) PRUNE WHEN EMPTY))",
                output(values(ImmutableList.of("x", "a", "b"))));

        assertPlan("SELECT *\n" +
                        "FROM TABLE(test.system.two_table_arguments_function(\n" +
                        "                input1 => TABLE(SELECT 1, true WHERE false) t1(a, b) PRUNE WHEN EMPTY,\n" +
                        "                input2 => TABLE(SELECT 2, false) t2(c, d) KEEP WHEN EMPTY))\n",
                output(values(ImmutableList.of("column"))));

        assertPlan("SELECT *\n" +
                        "FROM TABLE(test.system.two_table_arguments_function(\n" +
                        "                input1 => TABLE(SELECT 1, true WHERE false) t1(a, b) PRUNE WHEN EMPTY,\n" +
                        "                input2 => TABLE(SELECT 2, false WHERE false) t2(c, d) PRUNE WHEN EMPTY))\n",
                output(values(ImmutableList.of("column"))));

        assertPlan("SELECT *\n" +
                        "FROM TABLE(test.system.two_table_arguments_function(\n" +
                        "                input1 => TABLE(SELECT 1, true WHERE false) t1(a, b) PRUNE WHEN EMPTY,\n" +
                        "                input2 => TABLE(SELECT 2, false WHERE false) t2(c, d) KEEP WHEN EMPTY))\n",
                output(values(ImmutableList.of("column"))));

        assertPlan("SELECT *\n" +
                        "FROM TABLE(test.system.two_table_arguments_function(\n" +
                        "                input1 => TABLE(SELECT 1, true WHERE false) t1(a, b) KEEP WHEN EMPTY,\n" +
                        "                input2 => TABLE(SELECT 2, false WHERE false) t2(c, d) KEEP WHEN EMPTY))\n",
                output(
                        node(TableFunctionProcessorNode.class,
                                values(ImmutableList.of("a", "marker_1", "c", "marker_2", "row_number")))));

        assertPlan("SELECT *\n" +
                        "FROM TABLE(test.system.two_table_arguments_function(\n" +
                        "                input1 => TABLE(SELECT 1, true WHERE false) t1(a, b) KEEP WHEN EMPTY,\n" +
                        "                input2 => TABLE(SELECT 2, false) t2(c, d) PRUNE WHEN EMPTY))\n",
                output(
                        node(TableFunctionProcessorNode.class,
                                project(
                                        project(
                                                rowNumber(
                                                        builder -> builder.partitionBy(ImmutableList.of()),
                                                        project(
                                                                ImmutableMap.of("c", expression("INTEGER'2'")),
                                                                values(1))
                                                ).withAlias("input_2_row_number", new RowNumberSymbolMatcher()))))));
    }
}
