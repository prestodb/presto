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
package com.facebook.presto.verifier.framework;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.verifier.source.MySqlSourceQuerySupplier.MYSQL_SOURCE_QUERY_SUPPLIER;
import static java.util.concurrent.TimeUnit.MINUTES;

public class VerifierConfig
{
    private Optional<String> additionalJdbcDriverPath = Optional.empty();
    private Optional<String> controlJdbcDriverClass = Optional.empty();
    private Optional<String> testJdbcDriverClass = Optional.empty();

    private String controlJdbcUrl;
    private String testJdbcUrl;

    private Duration controlTimeout = new Duration(10, MINUTES);
    private Duration testTimeout = new Duration(30, MINUTES);
    private Duration metadataTimeout = new Duration(3, MINUTES);
    private Duration checksumTimeout = new Duration(20, MINUTES);

    private QualifiedName controlTablePrefix = QualifiedName.of("tmp_verifier_control");
    private QualifiedName testTablePrefix = QualifiedName.of("tmp_verifier_test");

    private Optional<Set<String>> whitelist = Optional.empty();
    private Optional<Set<String>> blacklist = Optional.empty();

    private String sourceQuerySupplier = MYSQL_SOURCE_QUERY_SUPPLIER;

    private Set<String> eventClients = ImmutableSet.of("human-readable");
    private Optional<String> jsonEventLogFile = Optional.empty();
    private Optional<String> humanReadableEventLogFile = Optional.empty();

    private String testId;
    private int maxConcurrency = 10;
    private int suiteRepetitions = 1;
    private int queryRepetitions = 1;

    private double relativeErrorMargin = 1e-4;
    private double absoluteErrorMargin = 1e-12;
    private boolean runTearDownOnResultMismatch;
    private boolean failureResolverEnabled = true;
    private int verificationResubmissionLimit = 2;

    @NotNull
    public Optional<String> getAdditionalJdbcDriverPath()
    {
        return additionalJdbcDriverPath;
    }

    @ConfigDescription("Path for test jdbc driver")
    @Config("additional-jdbc-driver-path")
    public VerifierConfig setAdditionalJdbcDriverPath(String additionalJdbcDriverPath)
    {
        this.additionalJdbcDriverPath = Optional.ofNullable(additionalJdbcDriverPath);
        return this;
    }

    @NotNull
    public Optional<String> getControlJdbcDriverClass()
    {
        return controlJdbcDriverClass;
    }

    @ConfigDescription("Fully qualified control JDBC driver name")
    @Config("control.jdbc-driver-class")
    public VerifierConfig setControlJdbcDriverClass(String controlJdbcDriverClass)
    {
        this.controlJdbcDriverClass = Optional.ofNullable(controlJdbcDriverClass);
        return this;
    }

    @NotNull
    public Optional<String> getTestJdbcDriverClass()
    {
        return testJdbcDriverClass;
    }

    @ConfigDescription("Fully qualified test JDBC driver name")
    @Config("test.jdbc-driver-class")
    public VerifierConfig setTestJdbcDriverClass(String testJdbcDriverClass)
    {
        this.testJdbcDriverClass = Optional.ofNullable(testJdbcDriverClass);
        return this;
    }

    @NotNull
    public String getControlJdbcUrl()
    {
        return controlJdbcUrl;
    }

    @ConfigDescription("URL for the control cluster")
    @Config("control.jdbc-url")
    public VerifierConfig setControlJdbcUrl(String controlJdbcUrl)
    {
        this.controlJdbcUrl = controlJdbcUrl;
        return this;
    }

    @NotNull
    public String getTestJdbcUrl()
    {
        return testJdbcUrl;
    }

    @ConfigDescription("URL for the test cluster")
    @Config("test.jdbc-url")
    public VerifierConfig setTestJdbcUrl(String testJdbcUrl)
    {
        this.testJdbcUrl = testJdbcUrl;
        return this;
    }

    @MinDuration("1s")
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

    @MinDuration("1s")
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

    @MinDuration("1s")
    public Duration getMetadataTimeout()
    {
        return metadataTimeout;
    }

    @Config("metadata.timeout")
    public VerifierConfig setMetadataTimeout(Duration metadataTimeout)
    {
        this.metadataTimeout = metadataTimeout;
        return this;
    }

    @MinDuration("1s")
    public Duration getChecksumTimeout()
    {
        return checksumTimeout;
    }

    @Config("checksum.timeout")
    public VerifierConfig setChecksumTimeout(Duration checksumTimeout)
    {
        this.checksumTimeout = checksumTimeout;
        return this;
    }

    @NotNull
    public QualifiedName getControlTablePrefix()
    {
        return controlTablePrefix;
    }

    @ConfigDescription("The prefix to use for temporary control shadow tables. May be fully qualified like 'tmp_catalog.tmp_schema.tmp_'")
    @Config("control.table-prefix")
    public VerifierConfig setControlTablePrefix(String controlTablePrefix)
    {
        this.controlTablePrefix = controlTablePrefix == null ?
                null :
                QualifiedName.of(Splitter.on(".").splitToList(controlTablePrefix));
        return this;
    }

    @NotNull
    public QualifiedName getTestTablePrefix()
    {
        return testTablePrefix;
    }

    @ConfigDescription("The prefix to use for temporary test shadow tables. May be fully qualified like 'tmp_catalog.tmp_schema.tmp_'")
    @Config("test.table-prefix")
    public VerifierConfig setTestTablePrefix(String testTablePrefix)
    {
        this.testTablePrefix = testTablePrefix == null ?
                null :
                QualifiedName.of(Splitter.on(".").splitToList(testTablePrefix));
        return this;
    }

    @NotNull
    public Optional<Set<String>> getWhitelist()
    {
        return whitelist;
    }

    @ConfigDescription("Names of queries which are whitelisted. Whitelist is applied before the blacklist")
    @Config("whitelist")
    public VerifierConfig setWhitelist(String whitelist)
    {
        this.whitelist = whitelist == null ?
                Optional.empty() :
                Optional.of(ImmutableSet.copyOf(Splitter.on(',').trimResults().omitEmptyStrings().splitToList(whitelist)));
        return this;
    }

    @NotNull
    public Optional<Set<String>> getBlacklist()
    {
        return blacklist;
    }

    @ConfigDescription("Names of queries which are blacklisted")
    @Config("blacklist")
    public VerifierConfig setBlacklist(String blacklist)
    {
        this.blacklist = blacklist == null ?
                Optional.empty() :
                Optional.of(ImmutableSet.copyOf(Splitter.on(',').trimResults().omitEmptyStrings().splitToList(blacklist)));
        return this;
    }

    @NotNull
    public String getSourceQuerySupplier()
    {
        return sourceQuerySupplier;
    }

    @ConfigDescription("The type of source query supplier")
    @Config("source-query.supplier")
    public VerifierConfig setSourceQuerySupplier(String sourceQuerySupplier)
    {
        this.sourceQuerySupplier = sourceQuerySupplier;
        return this;
    }

    @NotNull
    public Set<String> getEventClients()
    {
        return eventClients;
    }

    @ConfigDescription("The event client(s) to log the results to")
    @Config("event-clients")
    public VerifierConfig setEventClients(String eventClients)
    {
        this.eventClients = ImmutableSet.copyOf(Splitter.on(',').trimResults().omitEmptyStrings().splitToList(eventClients));
        return this;
    }

    @NotNull
    public Optional<String> getJsonEventLogFile()
    {
        return jsonEventLogFile;
    }

    @ConfigDescription("The file to log json events. Used with event-clients=json. Print to standard output stream if not specified.")
    @Config("json.log-file")
    public VerifierConfig setJsonEventLogFile(String jsonEventLogFile)
    {
        this.jsonEventLogFile = Optional.ofNullable(jsonEventLogFile);
        return this;
    }

    @NotNull
    public Optional<String> getHumanReadableEventLogFile()
    {
        return humanReadableEventLogFile;
    }

    @ConfigDescription("The file to log human readable events. Used with event-clients=human-readable. Print to standard output stream if not specified.")
    @Config("human-readable.log-file")
    public VerifierConfig setHumanReadableEventLogFile(String humanReadableEventLogFile)
    {
        this.humanReadableEventLogFile = Optional.ofNullable(humanReadableEventLogFile);
        return this;
    }

    @NotNull
    public String getTestId()
    {
        return testId;
    }

    @ConfigDescription("A customizable string that will be logged with the results")
    @Config("test-id")
    public VerifierConfig setTestId(String testId)
    {
        this.testId = testId;
        return this;
    }

    @Min(1)
    public int getMaxConcurrency()
    {
        return maxConcurrency;
    }

    @Config("max-concurrency")
    public VerifierConfig setMaxConcurrency(int maxConcurrency)
    {
        this.maxConcurrency = maxConcurrency;
        return this;
    }

    @Min(1)
    public int getSuiteRepetitions()
    {
        return suiteRepetitions;
    }

    @ConfigDescription("Number of times to run each suite")
    @Config("suite-repetitions")
    public VerifierConfig setSuiteRepetitions(int suiteRepetitions)
    {
        this.suiteRepetitions = suiteRepetitions;
        return this;
    }

    @Min(1)
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

    @Min(0)
    public double getRelativeErrorMargin()
    {
        return relativeErrorMargin;
    }

    @ConfigDescription("The maximum tolerable relative error between the sum of two floating point columns.")
    @Config("relative-error-margin")
    public VerifierConfig setRelativeErrorMargin(double relativeErrorMargin)
    {
        this.relativeErrorMargin = relativeErrorMargin;
        return this;
    }

    @Min(0)
    public double getAbsoluteErrorMargin()
    {
        return absoluteErrorMargin;
    }

    @ConfigDescription("The maximum tolerable difference between the mean of two floating point columns. Applicable when one mean value is 0.")
    @Config("absolute-error-margin")
    public VerifierConfig setAbsoluteErrorMargin(double absoluteErrorMargin)
    {
        this.absoluteErrorMargin = absoluteErrorMargin;
        return this;
    }

    public boolean isRunTearDownOnResultMismatch()
    {
        return runTearDownOnResultMismatch;
    }

    @ConfigDescription("When set to false, temporary tables are not dropped in case of checksum failure")
    @Config("run-teardown-on-result-mismatch")
    public VerifierConfig setRunTearDownOnResultMismatch(boolean runTearDownOnResultMismatch)
    {
        this.runTearDownOnResultMismatch = runTearDownOnResultMismatch;
        return this;
    }

    public boolean isFailureResolverEnabled()
    {
        return failureResolverEnabled;
    }

    @Config("failure-resolver.enabled")
    public VerifierConfig setFailureResolverEnabled(boolean failureResolverEnabled)
    {
        this.failureResolverEnabled = failureResolverEnabled;
        return this;
    }

    @Min(0)
    public int getVerificationResubmissionLimit()
    {
        return verificationResubmissionLimit;
    }

    @ConfigDescription("Maximum number of time a transiently failed verification can be resubmitted")
    @Config("verification-resubmission.limit")
    public VerifierConfig setVerificationResubmissionLimit(int verificationResubmissionLimit)
    {
        this.verificationResubmissionLimit = verificationResubmissionLimit;
        return this;
    }
}
