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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.execution.warnings.DefaultWarningCollector;
import com.facebook.presto.execution.warnings.WarningCollectorConfig;
import com.facebook.presto.execution.warnings.WarningHandlingLevel;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.WarningCode;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.RuleStatsRecorder;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.TranslateExpressions;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.UNKNOWN;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.IntStream.range;
import static org.testng.Assert.fail;

public class TestPlannerWarnings
{
    private LocalQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
    {
        queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .build());

        queryRunner.createCatalog(
                queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
    }

    @Test
    public void testWarning()
    {
        List<PrestoWarning> warnings = createTestWarnings(3);
        List<WarningCode> warningCodes = warnings.stream()
                .map(PrestoWarning::getWarningCode)
                .collect(toImmutableList());
        assertPlannerWarnings(queryRunner, "SELECT * FROM NATION", ImmutableMap.of(), warningCodes, Optional.of(ImmutableList.of(new TestWarningsRule(warnings), new PassRemoteProjectSanityChecker())));
    }

    public static void assertPlannerWarnings(LocalQueryRunner queryRunner, @Language("SQL") String sql, Map<String, String> sessionProperties, List<WarningCode> expectedWarnings, Optional<List<Rule<?>>> rules)
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog(queryRunner.getDefaultSession().getCatalog().get())
                .setSchema(queryRunner.getDefaultSession().getSchema().get());
        sessionProperties.forEach(sessionBuilder::setSystemProperty);
        WarningCollector warningCollector = new DefaultWarningCollector(new WarningCollectorConfig(), WarningHandlingLevel.NORMAL);
        try {
            queryRunner.inTransaction(sessionBuilder.build(), transactionSession -> {
                if (rules.isPresent()) {
                    createPlan(queryRunner, transactionSession, sql, warningCollector, rules.get());
                }
                else {
                    queryRunner.createPlan(transactionSession, sql, LogicalPlanner.Stage.CREATED, false, warningCollector);
                }
                return null;
            });
        }
        catch (SemanticException e) {
            // ignore
        }
        Set<WarningCode> warnings = warningCollector.getWarnings().stream()
                .map(PrestoWarning::getWarningCode)
                .collect(toImmutableSet());
        for (WarningCode expectedWarning : expectedWarnings) {
            if (!warnings.contains(expectedWarning)) {
                fail("Expected warning: " + expectedWarning);
            }
        }
    }

    private static Plan createPlan(LocalQueryRunner queryRunner, Session session, String sql, WarningCollector warningCollector, List<Rule<?>> rules)
    {
        // Warnings from testing rules will be added
        PlanOptimizer optimizer = new IterativeOptimizer(
                new RuleStatsRecorder(),
                queryRunner.getStatsCalculator(),
                queryRunner.getCostCalculator(),
                ImmutableSet.copyOf(rules));

        // Translate all OriginalExpression in planNodes to RowExpression so that we can do plan pattern asserting and printing on RowExpression only.
        PlanOptimizer expressionTranslator = new IterativeOptimizer(
                new RuleStatsRecorder(),
                queryRunner.getStatsCalculator(),
                queryRunner.getCostCalculator(),
                ImmutableSet.copyOf(new TranslateExpressions(queryRunner.getMetadata(), queryRunner.getSqlParser()).rules()));

        return queryRunner.createPlan(session, sql, ImmutableList.of(optimizer, expressionTranslator), LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, warningCollector);
    }

    public static List<PrestoWarning> createTestWarnings(int numberOfWarnings)
    {
        checkArgument(numberOfWarnings > 0, "numberOfWarnings must be > 0");
        ImmutableList.Builder<PrestoWarning> builder = ImmutableList.builder();
        range(1, numberOfWarnings)
                .mapToObj(code -> new PrestoWarning(new WarningCode(code, "testWarning"), "Test warning " + code))
                .forEach(builder::add);
        return builder.build();
    }

    public static class TestWarningsRule
            implements Rule<ProjectNode>
    {
        private final List<PrestoWarning> warnings;

        public TestWarningsRule(List<PrestoWarning> warnings)
        {
            this.warnings = ImmutableList.copyOf(requireNonNull(warnings, "warnings is null"));
        }

        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return project();
        }

        @Override
        public Result apply(ProjectNode node, Captures captures, Context context)
        {
            warnings.stream()
                    .forEach(context.getWarningCollector()::add);
            return Result.empty();
        }
    }

    public static class PassRemoteProjectSanityChecker
            implements Rule<ProjectNode>
    {
        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return project();
        }

        @Override
        public Result apply(ProjectNode node, Captures captures, Context context)
        {
            if (node.getLocality().equals(UNKNOWN)) {
                return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), node.getSource(), node.getAssignments(), LOCAL));
            }
            return Result.empty();
        }
    }
}
