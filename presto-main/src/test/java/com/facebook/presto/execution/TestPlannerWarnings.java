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

import com.facebook.presto.Session.SessionBuilder;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.spi.WarningCode;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static org.testng.Assert.fail;

public class TestPlannerWarnings
{
    private LocalQueryRunner queryRunner;

    @BeforeClass
    public final void setUp()
    {
        SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("task_concurrency", "1");

        queryRunner = new LocalQueryRunner(sessionBuilder.build());

        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());
    }

    @AfterClass(alwaysRun = true)
    public final void tearDown()
    {
        closeAllRuntimeException(queryRunner);
        queryRunner = null;
    }

    @Test
    public void testLegacyOrderBy()
    {
        assertWarnings(
                "SELECT -a AS a FROM (VALUES -1, 0, 2) t(a) ORDER BY -a",
                ImmutableMap.of(SystemSessionProperties.LEGACY_ORDER_BY, "true"),
                WarningCode.LEGACY_ORDER_BY);

        assertNoWarnings("SELECT -a AS a FROM (VALUES -1, 0, 2) t(a) ORDER BY -a", ImmutableMap.of());
    }

    private void assertNoWarnings(@Language("SQL") String sql, Map<String, String> sessionProperties)
    {
        assertWarnings(sql, sessionProperties);
    }

    private void assertWarnings(@Language("SQL") String sql, Map<String, String> sessionProperties, WarningCode... expectedWarnings)
    {
        SessionBuilder sessionBuilder = testSessionBuilder();
        sessionProperties.forEach(sessionBuilder::setSystemProperty);

        WarningSink sink = new DeduplicatingWarningSink();
        queryRunner.inTransaction(sessionBuilder.build(), transactionSession -> {
            queryRunner.createPlan(transactionSession, sql, LogicalPlanner.Stage.CREATED, false, sink);
            return null;
        });

        Set<WarningCode> warnings = sink.getWarnings().stream()
                .map(PrestoWarning::getCode)
                .collect(Collectors.toSet());

        for (WarningCode expectedWarning : expectedWarnings) {
            if (!warnings.contains(expectedWarning)) {
                fail("Expected warning " + expectedWarning.name());
            }
        }
    }
}
