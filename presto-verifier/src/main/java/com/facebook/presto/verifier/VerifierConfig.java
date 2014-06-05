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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class VerifierConfig
{
    private String testUsername = "verifier-test";
    private String controlUsername = "verifier-test";
    private String testPassword;
    private String controlPassword;
    private String suite;
    private String source;
    private String runId = new DateTime().toString("yyyy-MM-dd");
    private String eventClient = "human-readable";
    private int threadCount = 10;
    private String queryDatabase;
    private String controlGateway;
    private String testGateway;
    private Duration controlTimeout = new Duration(10, TimeUnit.MINUTES);
    private Duration testTimeout = new Duration(1, TimeUnit.HOURS);
    private Set<String> blacklist = ImmutableSet.of();
    private Set<String> whitelist = ImmutableSet.of();
    private int maxRowCount = 10_000;
    private int maxQueries = 1_000_000;
    private boolean alwaysReport;
    private String eventLogFile;
    private int suiteRepetitions = 1;
    private int queryRepetitions = 1;
    private boolean checkCorrectness = true;
    private String testCatalogOverride;
    private String testSchemaOverride;
    private String controlCatalogOverride;
    private String controlSchemaOverride;
    private boolean quiet;

    public boolean isQuiet()
    {
        return quiet;
    }

    @ConfigDescription("Reduces the number of informational messages printed")
    @Config("quiet")
    public VerifierConfig setQuiet(boolean quiet)
    {
        this.quiet = quiet;
        return this;
    }

    public int getQueryRepetitions()
    {
        return queryRepetitions;
    }

    @ConfigDescription("The number of times to repeat each query")
    @Config("query-repetitions")
    public VerifierConfig setQueryRepetitions(int queryRepetitions)
    {
        this.queryRepetitions = queryRepetitions;
        return this;
    }

    @NotNull
    public String getSuite()
    {
        return suite;
    }

    @ConfigDescription("The suite of queries in the query database to run")
    @Config("suite")
    public VerifierConfig setSuite(String suite)
    {
        this.suite = suite;
        return this;
    }

    @Min(1)
    @Max(50)
    public int getThreadCount()
    {
        return threadCount;
    }

    @ConfigDescription("The concurrency level")
    @Config("thread-count")
    public VerifierConfig setThreadCount(int threadCount)
    {
        this.threadCount = threadCount;
        return this;
    }

    @NotNull
    public String getQueryDatabase()
    {
        return queryDatabase;
    }

    @ConfigDescription("Database to fetch query suites from")
    @Config("query-database")
    public VerifierConfig setQueryDatabase(String queryDatabase)
    {
        this.queryDatabase = queryDatabase;
        return this;
    }

    @NotNull
    public Set<String> getBlacklist()
    {
        return blacklist;
    }

    @ConfigDescription("Names of queries which are blacklisted")
    @Config("blacklist")
    public VerifierConfig setBlacklist(String blacklist)
    {
        ImmutableSet.Builder<String> blacklistBuilder = ImmutableSet.builder();
        for (String value : Splitter.on(',').trimResults().omitEmptyStrings().split(blacklist)) {
            blacklistBuilder.add(value);
        }

        this.blacklist = blacklistBuilder.build();
        return this;
    }

    @NotNull
    public Set<String> getWhitelist()
    {
        return whitelist;
    }

    @ConfigDescription("Names of queries which are whitelisted. Whitelist is applied before the blacklist")
    @Config("whitelist")
    public VerifierConfig setWhitelist(String whitelist)
    {
        ImmutableSet.Builder<String> whitelistBuilder = ImmutableSet.builder();
        for (String value : Splitter.on(',').trimResults().omitEmptyStrings().split(whitelist)) {
            whitelistBuilder.add(value);
        }

        this.whitelist = whitelistBuilder.build();
        return this;
    }

    @Min(1)
    public int getMaxRowCount()
    {
        return maxRowCount;
    }

    @ConfigDescription("The maximum number of rows a query may return. If it exceeds this limit it's marked as failed")
    @Config("max-row-count")
    public VerifierConfig setMaxRowCount(int maxRowCount)
    {
        this.maxRowCount = maxRowCount;
        return this;
    }

    public boolean isAlwaysReport()
    {
        return alwaysReport;
    }

    @ConfigDescription("Print more informational messages")
    @Config("always-report")
    public VerifierConfig setAlwaysReport(boolean alwaysReport)
    {
        this.alwaysReport = alwaysReport;
        return this;
    }

    public int getMaxQueries()
    {
        return maxQueries;
    }

    @ConfigDescription("The maximum number of queries from the suite to run")
    @Config("max-queries")
    public VerifierConfig setMaxQueries(int maxQueries)
    {
        this.maxQueries = maxQueries;
        return this;
    }

    public boolean isCheckCorrectnessEnabled()
    {
        return checkCorrectness;
    }

    @ConfigDescription("Whether to check that the rows from control and test match")
    @Config("check-correctness")
    public VerifierConfig setCheckCorrectnessEnabled(boolean checkCorrectness)
    {
        this.checkCorrectness = checkCorrectness;
        return this;
    }

    public int getSuiteRepetitions()
    {
        return suiteRepetitions;
    }

    @ConfigDescription("Number of times to run the suite")
    @Config("suite-repetitions")
    public VerifierConfig setSuiteRepetitions(int suiteRepetitions)
    {
        this.suiteRepetitions = suiteRepetitions;
        return this;
    }

    @NotNull
    public String getEventClient()
    {
        return eventClient;
    }

    @ConfigDescription("The event client to log the results to")
    @Config("event-client")
    public VerifierConfig setEventClient(String eventClient)
    {
        this.eventClient = checkNotNull(eventClient, "eventClient is null");
        return this;
    }

    public String getSource()
    {
        return source;
    }

    @ConfigDescription("The source to pass to Presto")
    @Config("source")
    public VerifierConfig setSource(String source)
    {
        this.source = source;
        return this;
    }

    public String getRunId()
    {
        return runId;
    }

    @ConfigDescription("A customizable string that will be logged with the results")
    @Config("run-id")
    public VerifierConfig setRunId(String runId)
    {
        this.runId = runId;
        return this;
    }

    public String getEventLogFile()
    {
        return eventLogFile;
    }

    @ConfigDescription("The file to log events to. Used with event-client=file")
    @Config("event-log-file")
    public VerifierConfig setEventLogFile(String eventLogFile)
    {
        this.eventLogFile = eventLogFile;
        return this;
    }

    public String getTestCatalogOverride()
    {
        return testCatalogOverride;
    }

    @ConfigDescription("Overrides the test_catalog field in all queries in the suite")
    @Config("test.catalog-override")
    public VerifierConfig setTestCatalogOverride(String testCatalogOverride)
    {
        this.testCatalogOverride = testCatalogOverride;
        return this;
    }

    public String getTestSchemaOverride()
    {
        return testSchemaOverride;
    }

    @ConfigDescription("Overrides the test_schema field in all queries in the suite")
    @Config("test.schema-override")
    public VerifierConfig setTestSchemaOverride(String testSchemaOverride)
    {
        this.testSchemaOverride = testSchemaOverride;
        return this;
    }

    public Duration getTestTimeout()
    {
        return testTimeout;
    }

    @ConfigDescription("Timeout for queries to the test cluster")
    @Config("test.timeout")
    public VerifierConfig setTestTimeout(Duration testTimeout)
    {
        this.testTimeout = testTimeout;
        return this;
    }

    @NotNull
    public String getTestUsername()
    {
        return testUsername;
    }

    @ConfigDescription("Username for test cluster")
    @Config("test.username")
    public VerifierConfig setTestUsername(String testUsername)
    {
        this.testUsername = testUsername;
        return this;
    }

    @Nullable
    public String getTestPassword()
    {
        return testPassword;
    }

    @ConfigDescription("Password for test cluster")
    @Config("test.password")
    public VerifierConfig setTestPassword(String testPassword)
    {
        this.testPassword = testPassword;
        return this;
    }

    @NotNull
    public String getTestGateway()
    {
        return testGateway;
    }

    @ConfigDescription("URL for test cluster")
    @Config("test.gateway")
    public VerifierConfig setTestGateway(String testGateway)
    {
        this.testGateway = testGateway;
        return this;
    }

    public String getControlCatalogOverride()
    {
        return controlCatalogOverride;
    }

    @ConfigDescription("Overrides the control_catalog field in all queries in the suite")
    @Config("control.catalog-override")
    public VerifierConfig setControlCatalogOverride(String controlCatalogOverride)
    {
        this.controlCatalogOverride = controlCatalogOverride;
        return this;
    }

    public String getControlSchemaOverride()
    {
        return controlSchemaOverride;
    }

    @ConfigDescription("Overrides the control_schema field in all queries in the suite")
    @Config("control.schema-override")
    public VerifierConfig setControlSchemaOverride(String controlSchemaOverride)
    {
        this.controlSchemaOverride = controlSchemaOverride;
        return this;
    }

    public Duration getControlTimeout()
    {
        return controlTimeout;
    }

    @ConfigDescription("Timeout for queries to the control cluster")
    @Config("control.timeout")
    public VerifierConfig setControlTimeout(Duration controlTimeout)
    {
        this.controlTimeout = controlTimeout;
        return this;
    }

    @NotNull
    public String getControlUsername()
    {
        return controlUsername;
    }

    @ConfigDescription("Username for control cluster")
    @Config("control.username")
    public VerifierConfig setControlUsername(String controlUsername)
    {
        this.controlUsername = controlUsername;
        return this;
    }

    @Nullable
    public String getControlPassword()
    {
        return controlPassword;
    }

    @ConfigDescription("Password for control cluster")
    @Config("control.password")
    public VerifierConfig setControlPassword(String controlPassword)
    {
        this.controlPassword = controlPassword;
        return this;
    }

    @NotNull
    public String getControlGateway()
    {
        return controlGateway;
    }

    @ConfigDescription("URL for control cluster")
    @Config("control.gateway")
    public VerifierConfig setControlGateway(String controlGateway)
    {
        this.controlGateway = controlGateway;
        return this;
    }
}
