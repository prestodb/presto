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

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.functionNamespace.SqlInvokedFunctionNamespaceManagerConfig;
import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutors;
import com.facebook.presto.functionNamespace.testing.InMemoryFunctionNamespaceManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.RoutineCharacteristics.Language;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.functionNamespace.testing.SqlInvokedFunctionTestUtils.FUNCTION_TANGENT;
import static com.facebook.presto.spi.function.FunctionImplementationType.THRIFT;
import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.DETERMINISTIC;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Language.SQL;
import static com.facebook.presto.spi.function.RoutineCharacteristics.NullCallClause.RETURNS_NULL_ON_NULL_INPUT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestRewriteFilterWithExternalFunctionToProject
        extends BaseRuleTest
{
    private static final QualifiedObjectName REMOTE_FOO = QualifiedObjectName.valueOf(new CatalogSchemaName("unittest", "memory"), "remote_foo");
    private static final Language JAVA = new Language("java");

    private static final SqlInvokedFunction FUNCTION_REMOTE_FOO = new SqlInvokedFunction(
            REMOTE_FOO,
            ImmutableList.of(new Parameter("x", parseTypeSignature(StandardTypes.INTEGER))),
            parseTypeSignature(StandardTypes.INTEGER),
            "remote_foo(x)",
            RoutineCharacteristics.builder().setLanguage(JAVA).setDeterminism(DETERMINISTIC).setNullCallClause(RETURNS_NULL_ON_NULL_INPUT).build(),
            "",
            notVersioned());

    @BeforeClass
    public void setup()
    {
        FunctionAndTypeManager functionAndTypeManager = getFunctionManager();
        functionAndTypeManager.addFunctionNamespace(
                "unittest",
                new InMemoryFunctionNamespaceManager(
                        "unittest",
                        new SqlFunctionExecutors(
                                ImmutableMap.of(
                                        SQL, FunctionImplementationType.SQL,
                                        JAVA, THRIFT),
                                null),
                        new SqlInvokedFunctionNamespaceManagerConfig().setSupportedFunctionLanguages("sql,java")));
        functionAndTypeManager.createFunction(FUNCTION_TANGENT, true);
        functionAndTypeManager.createFunction(FUNCTION_REMOTE_FOO, true);
    }

    @Test
    public void testNoExternalFunctionInFilter()
    {
        tester().assertThat(new RewriteFilterWithExternalFunctionToProject(getFunctionManager()))
                .on(p -> {
                    p.variable("x", INTEGER);
                    return p.filter(
                            p.rowExpression("unittest.memory.tangent(x) > 1"),
                            p.values(p.variable("x", INTEGER)));
                })
                .doesNotFire();
    }

    @Test
    public void testFilterWithExternalFunctionRewrite()
    {
        tester().assertThat(new RewriteFilterWithExternalFunctionToProject(getFunctionManager()))
                .on(p -> {
                    p.variable("x", INTEGER);
                    return p.filter(
                                    p.rowExpression("unittest.memory.remote_foo(x) > 1"),
                                    p.values(p.variable("x", INTEGER)));
                })
                .matches(
                        project(
                                ImmutableMap.of("x", PlanMatchPattern.expression("x")),
                                filter(
                                        "greater_than",
                                        project(
                                                ImmutableMap.of(
                                                        "greater_than", PlanMatchPattern.expression("unittest.memory.remote_foo(x) > 1"),
                                                        "x", PlanMatchPattern.expression("x")),
                                                values(ImmutableMap.of("x", 0))))));
    }
}
