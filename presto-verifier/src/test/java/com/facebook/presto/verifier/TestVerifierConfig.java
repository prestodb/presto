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
package com.facebook.presto.verifier;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.verifier.QueryType.CREATE;
import static com.facebook.presto.verifier.QueryType.MODIFY;
import static com.facebook.presto.verifier.QueryType.READ;

public class TestVerifierConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(VerifierConfig.class)
                .setTestUsernameOverride(null)
                .setControlUsernameOverride(null)
                .setTestPasswordOverride(null)
                .setControlPasswordOverride(null)
                .setSuite(null)
                .setSuites(null)
                .setControlQueryTypes(Joiner.on(",").join(CREATE, READ, MODIFY))
                .setTestQueryTypes(Joiner.on(",").join(CREATE, READ, MODIFY))
                .setSource(null)
                .setRunId(new DateTime().toString("yyyy-MM-dd"))
                .setEventClients("human-readable")
                .setThreadCount(10)
                .setQueryDatabase(null)
                .setControlGateway(null)
                .setTestGateway(null)
                .setControlTimeout(new Duration(10, TimeUnit.MINUTES))
                .setTestTimeout(new Duration(1, TimeUnit.HOURS))
                .setBlacklist("")
                .setWhitelist("")
                .setMaxRowCount(10_000)
                .setMaxQueries(1_000_000)
                .setAlwaysReport(false)
                .setSuiteRepetitions(1)
                .setCheckCorrectnessEnabled(true)
                .setExplainOnly(false)
                .setSkipCorrectnessRegex("^$")
                .setQueryRepetitions(1)
                .setTestCatalogOverride(null)
                .setTestSchemaOverride(null)
                .setControlCatalogOverride(null)
                .setControlSchemaOverride(null)
                .setQuiet(false)
                .setVerboseResultsComparison(false)
                .setEventLogFile(null)
                .setAdditionalJdbcDriverPath(null)
                .setTestJdbcDriverName(null)
                .setControlJdbcDriverName(null)
                .setDoublePrecision(3)
                .setRegressionMinCpuTime(new Duration(5, TimeUnit.MINUTES))
                .setControlTeardownRetries(1)
                .setTestTeardownRetries(1));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("suites", "my_suite")
                .put("suite", "my_suite")
                .put("control.query-types", READ.name())
                .put("test.query-types", MODIFY.name())
                .put("source", "my_source")
                .put("run-id", "my_run_id")
                .put("event-client", "file,human-readable")
                .put("thread-count", "1")
                .put("blacklist", "1,2")
                .put("whitelist", "3,4")
                .put("verbose-results-comparison", "true")
                .put("max-row-count", "1")
                .put("max-queries", "1")
                .put("always-report", "true")
                .put("suite-repetitions", "2")
                .put("query-repetitions", "2")
                .put("check-correctness", "false")
                .put("explain-only", "true")
                .put("skip-correctness-regex", "limit")
                .put("quiet", "true")
                .put("event-log-file", "./test")
                .put("query-database", "jdbc:mysql://localhost:3306/my_database?user=my_username&password=my_password")
                .put("test.username-override", "test_user")
                .put("test.password-override", "test_password")
                .put("test.gateway", "jdbc:presto://localhost:8080")
                .put("test.timeout", "1s")
                .put("test.catalog-override", "my_catalog")
                .put("test.schema-override", "my_schema")
                .put("control.username-override", "control_user")
                .put("control.password-override", "control_password")
                .put("control.gateway", "jdbc:presto://localhost:8081")
                .put("control.timeout", "1s")
                .put("control.catalog-override", "my_catalog")
                .put("control.schema-override", "my_schema")
                .put("additional-jdbc-driver-path", "/test/path")
                .put("test.jdbc-driver-class", "com.facebook.exampleclass")
                .put("control.jdbc-driver-class", "com.facebook.exampleclass")
                .put("expected-double-precision", "5")
                .put("regression.min-cpu-time", "30s")
                .put("control.teardown-retries", "5")
                .put("test.teardown-retries", "7")
                .build();

        VerifierConfig expected = new VerifierConfig().setTestUsernameOverride("verifier-test")
                .setSuites("my_suite")
                .setSuite("my_suite")
                .setControlQueryTypes(READ.name())
                .setTestQueryTypes(MODIFY.name())
                .setSource("my_source")
                .setRunId("my_run_id")
                .setEventClients("file,human-readable")
                .setThreadCount(1)
                .setBlacklist("1,2")
                .setWhitelist("3,4")
                .setMaxRowCount(1)
                .setMaxQueries(1)
                .setAlwaysReport(true)
                .setVerboseResultsComparison(true)
                .setSuiteRepetitions(2)
                .setQueryRepetitions(2)
                .setCheckCorrectnessEnabled(false)
                .setExplainOnly(true)
                .setSkipCorrectnessRegex("limit")
                .setQuiet(true)
                .setEventLogFile("./test")
                .setQueryDatabase("jdbc:mysql://localhost:3306/my_database?user=my_username&password=my_password")
                .setTestUsernameOverride("test_user")
                .setTestPasswordOverride("test_password")
                .setTestGateway("jdbc:presto://localhost:8080")
                .setTestTimeout(new Duration(1, TimeUnit.SECONDS))
                .setTestCatalogOverride("my_catalog")
                .setTestSchemaOverride("my_schema")
                .setControlUsernameOverride("control_user")
                .setControlPasswordOverride("control_password")
                .setControlGateway("jdbc:presto://localhost:8081")
                .setControlTimeout(new Duration(1, TimeUnit.SECONDS))
                .setControlCatalogOverride("my_catalog")
                .setControlSchemaOverride("my_schema")
                .setAdditionalJdbcDriverPath("/test/path")
                .setTestJdbcDriverName("com.facebook.exampleclass")
                .setControlJdbcDriverName("com.facebook.exampleclass")
                .setDoublePrecision(5)
                .setRegressionMinCpuTime(new Duration(30, TimeUnit.SECONDS))
                .setControlTeardownRetries(5)
                .setTestTeardownRetries(7);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
