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

package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_PAYLOAD_JOINS;
import static com.facebook.presto.SystemSessionProperties.VERBOSE_OPTIMIZER_INFO_ENABLED;
import static com.facebook.presto.SystemSessionProperties.VERBOSE_OPTIMIZER_RESULTS;
import static com.facebook.presto.testing.TestingSession.DEFAULT_TIME_ZONE_KEY;
import static com.facebook.presto.testing.TestingSession.TESTING_CATALOG;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestVerboseOptimizerInfo
        extends AbstractTestQueries
{

    private static final String CATALOG = "local";
    private static final String SCHEMA = TINY_SCHEMA_NAME;

    @Override
    protected QueryRunner createQueryRunner()
    {
        return createLocalQueryRunner();
    }

    public static LocalQueryRunner createLocalQueryRunner()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(SCHEMA)
                .build();

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);

        // add the tpch catalog
        // local queries run directly against the generator
        localQueryRunner.createCatalog(
                defaultSession.getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());

        localQueryRunner.getMetadata().registerBuiltInFunctions(CUSTOM_FUNCTIONS);

        SessionPropertyManager sessionPropertyManager = localQueryRunner.getMetadata().getSessionPropertyManager();
        sessionPropertyManager.addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
        sessionPropertyManager.addConnectorSessionProperties(new ConnectorId(TESTING_CATALOG), TEST_CATALOG_PROPERTIES);

        return localQueryRunner;
    }

    @Test
    public void testApplicableOptimizers()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(VERBOSE_OPTIMIZER_INFO_ENABLED, "true")
                .build();
        String query = "SELECT o.orderkey FROM part p, orders o, lineitem l WHERE p.partkey = l.partkey AND l.orderkey = o.orderkey AND p.partkey <> o.orderkey AND p.name < l.comment";
        MaterializedResult materializedResult = computeActual(session, "explain " + query);
        String explain = (String) getOnlyElement(materializedResult.getOnlyColumnAsSet());

        checkOptimizerInfo(explain, true, ImmutableList.of("PruneCrossJoinColumns"));
        checkOptimizerInfo(explain, false, ImmutableList.of("AddNotNullFiltersToJoinNode"));

        String payloadJoinQuery = "SELECT l.* FROM (select *, map(ARRAY[1,3], ARRAY[2,4]) as m1 from lineitem) l left join orders o on (l.orderkey = o.orderkey) left join part p on (l.partkey=p.partkey)";
        materializedResult = computeActual(session, "explain " + payloadJoinQuery);
        String explainPayloadJoinQuery = (String) getOnlyElement(materializedResult.getOnlyColumnAsSet());

        checkOptimizerInfo(explainPayloadJoinQuery, false, ImmutableList.of("PayloadJoinOptimizer"));

        Session sessionWithPayload = Session.builder(session)
                .setSystemProperty(OPTIMIZE_PAYLOAD_JOINS, "true")
                .build();
        materializedResult = computeActual(sessionWithPayload, "explain " + payloadJoinQuery);
        explainPayloadJoinQuery = (String) getOnlyElement(materializedResult.getOnlyColumnAsSet());

        checkOptimizerInfo(explainPayloadJoinQuery, true, ImmutableList.of("PayloadJoinOptimizer"));
    }

    @Test
    public void testVerboseOptimizerResults()
    {
        Session sessionPrintAll = Session.builder(getSession())
                .setSystemProperty(VERBOSE_OPTIMIZER_INFO_ENABLED, "true")
                .setSystemProperty(VERBOSE_OPTIMIZER_RESULTS, "all")
                .setSystemProperty(OPTIMIZE_PAYLOAD_JOINS, "true")
                .build();
        String query = "SELECT l.* FROM (select *, map(ARRAY[1,3], ARRAY[2,4]) as m1 from lineitem) l left join orders o on (l.orderkey = o.orderkey) left join part p on (l.partkey=p.partkey)";
        MaterializedResult materializedResult = computeActual(sessionPrintAll, "explain " + query);
        String explain = (String) getOnlyElement(materializedResult.getOnlyColumnAsSet());

        checkOptimizerResults(explain, ImmutableList.of("PayloadJoinOptimizer", "RemoveRedundantIdentityProjections", "PruneUnreferencedOutputs"), ImmutableList.of());

        Session sessionPrintSome = Session.builder(sessionPrintAll)
                .setSystemProperty(VERBOSE_OPTIMIZER_RESULTS, "PayloadJoinOptimizer,RemoveRedundantIdentityProjections")
                .build();
        materializedResult = computeActual(sessionPrintSome, "explain " + query);
        explain = (String) getOnlyElement(materializedResult.getOnlyColumnAsSet());

        checkOptimizerResults(explain, ImmutableList.of("PayloadJoinOptimizer", "RemoveRedundantIdentityProjections"), ImmutableList.of("PruneUnreferencedOutputs"));
    }

    @DataProvider
    public Object[][] cteSpecificationVariants()
    {
        String fullQualifiedTablePrefix = String.format("%s.%s.", CATALOG, SCHEMA);
        String schemaOnlyPrefix = String.format("%s.", SCHEMA);

        return new Object[][] {
                {Optional.of(CATALOG), Optional.of(SCHEMA),
                        "EXPLAIN with tbl as (select * from lineitem), tbl2 as (select * from tbl) select * from tbl, tbl2",
                        (Consumer<String>) (String explain) -> {
                            checkCTEInfo(explain, "tbl", 2, false);
                            checkCTEInfo(explain, "tbl2", 1, false);
                        }},
                {Optional.of(CATALOG), Optional.empty(),
                        "EXPLAIN with tbl as (select * from " + schemaOnlyPrefix + "lineitem), tbl2 as (select * from tbl) select * from tbl, tbl2",
                        (Consumer<String>) (String explain) -> {
                            checkCTEInfo(explain, "tbl", 2, false);
                            checkCTEInfo(explain, "tbl2", 1, false);
                        }},
                {Optional.empty(), Optional.empty(),
                        "EXPLAIN with tbl as (select * from " + fullQualifiedTablePrefix + "lineitem), tbl2 as (select * from tbl) select * from tbl, tbl2",
                        (Consumer<String>) (String explain) -> {
                            checkCTEInfo(explain, "tbl", 2, false);
                            checkCTEInfo(explain, "tbl2", 1, false);
                        }},
                {Optional.of(CATALOG), Optional.empty(),
                        // No name collisions occur for CTEs when intermixed with fully or partially specified tables
                        "EXPLAIN with tbl as (select * from " + schemaOnlyPrefix + "lineitem), lineitem as (select * from tbl) select * from tbl, lineitem",
                        (Consumer<String>) (String explain) -> {
                            checkCTEInfo(explain, "tbl", 2, false);
                            checkCTEInfo(explain, "lineitem", 1, false);
                        }},
        };
    }

    @Test(dataProvider = "cteSpecificationVariants")
    public void testVerboseCTEResults(Optional<String> sessionCatalog, Optional<String> sessionSchema, @Language("SQL") String explainQuery, Consumer<String> explainAssertions)
    {
        Session.SessionBuilder sessionBuilder = Session.builder(new SessionPropertyManager())
                .setQueryId(new QueryIdGenerator().createNextQueryId())
                .setIdentity(new Identity("user", Optional.empty()))
                .setTimeZoneKey(DEFAULT_TIME_ZONE_KEY)
                .setLocale(ENGLISH)
                .setRemoteUserAddress("address")
                .setUserAgent("agent")
                .setSystemProperty(VERBOSE_OPTIMIZER_INFO_ENABLED, "true");

        sessionCatalog.ifPresent(sessionBuilder::setCatalog);
        sessionSchema.ifPresent(sessionBuilder::setSchema);

        Session sessionPrintAll = sessionBuilder.build();

        MaterializedResult materializedResult = computeActual(sessionPrintAll, explainQuery);
        String explain = (String) getOnlyElement(materializedResult.getOnlyColumnAsSet());

        explainAssertions.accept(explain);
    }

    private void checkOptimizerInfo(String explain, boolean checkTriggered, List<String> optimizers)
    {
        String regex = checkTriggered ? "Triggered optimizers.*" : "Applicable optimizers.*";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(explain);
        assertTrue(matcher.find());

        String optimizerInfo = matcher.group();
        for (String opt : optimizers) {
            assertTrue(optimizerInfo.contains(opt));
        }
    }

    private void checkOptimizerResults(String explain, List<String> includedOptimizers, List<String> excludedOptimizers)
    {
        for (String opt : includedOptimizers) {
            assertTrue(explain.contains(opt + " (before):"));
            assertTrue(explain.contains(opt + " (after):"));
        }

        for (String opt : excludedOptimizers) {
            assertFalse(explain.contains(opt + " (before):"));
        }
    }

    private void checkCTEInfo(String explain, String name, int frequency, boolean isView)
    {
        String regex = "CTEInfo.*";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(explain);
        assertTrue(matcher.find());

        String cteInfo = matcher.group();
        assertTrue(cteInfo.contains(name + ": " + frequency + " (is_view: " + isView + ")"));
    }
}
