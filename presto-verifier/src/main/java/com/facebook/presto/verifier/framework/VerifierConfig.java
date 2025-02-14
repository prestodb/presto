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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.presto.verifier.rewrite.FunctionCallRewriter;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.verifier.rewrite.FunctionCallRewriter.FunctionCallSubstitute;
import static com.facebook.presto.verifier.source.MySqlSourceQuerySupplier.MYSQL_SOURCE_QUERY_SUPPLIER;
import static java.util.Locale.ENGLISH;

public class VerifierConfig
{
    public static final String CONTROL_TEST_MODE = "control-test";
    public static final String QUERY_BANK_MODE = "query-bank";
    private Optional<Set<String>> whitelist = Optional.empty();
    private Optional<Set<String>> blacklist = Optional.empty();

    private String sourceQuerySupplier = MYSQL_SOURCE_QUERY_SUPPLIER;

    private Set<String> eventClients = ImmutableSet.of("human-readable");
    private Optional<String> jsonEventLogFile = Optional.empty();
    private Optional<String> humanReadableEventLogFile = Optional.empty();

    private String testId;
    private Optional<String> testName = Optional.empty();
    private int maxConcurrency = 10;
    private int suiteRepetitions = 1;
    private int queryRepetitions = 1;

    private double relativeErrorMargin = 1e-4;
    private double absoluteErrorMargin = 1e-12;
    private boolean useErrorMarginForFloatingPointArrays = true;
    private boolean validateStringAsDouble;
    private boolean smartTeardown;
    private int verificationResubmissionLimit = 6;

    private boolean setupOnMainClusters = true;
    private boolean teardownOnMainClusters = true;
    private boolean skipControl;
    private boolean skipChecksum;
    private boolean runDeterminismAnalysisOnTest;
    private boolean concurrentControlAndTest;

    private boolean explain;
    private boolean saveSnapshot;

    private boolean extendedVerification;
    private String runningMode = CONTROL_TEST_MODE;

    private Multimap<String, FunctionCallSubstitute> functionSubstitutes = ImmutableMultimap.of();

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

    @ConfigDescription("A customizable string that will be passed into query client info and logged with the results")
    @Config("test-id")
    public VerifierConfig setTestId(String testId)
    {
        this.testId = testId;
        return this;
    }

    @NotNull
    public Optional<String> getTestName()
    {
        return testName;
    }

    @ConfigDescription("A customizable string that will be passed into query client info")
    @Config("test-name")
    public VerifierConfig setTestName(String testName)
    {
        this.testName = Optional.ofNullable(testName);
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

    public boolean isUseErrorMarginForFloatingPointArrays()
    {
        return useErrorMarginForFloatingPointArrays;
    }

    @ConfigDescription("When set to true, arrays of floating point numbers are validated like floating point columns, using error margins. False by default.")
    @Config("use-error-margin-for-floating-point-arrays")
    public VerifierConfig setUseErrorMarginForFloatingPointArrays(boolean useErrorMarginForFloatingPointArrays)
    {
        this.useErrorMarginForFloatingPointArrays = useErrorMarginForFloatingPointArrays;
        return this;
    }

    public boolean isValidateStringAsDouble()
    {
        return validateStringAsDouble;
    }

    @ConfigDescription("When set to true, validate string column as double if the values are all in the floating point format.")
    @Config("validate-string-as-double")
    public VerifierConfig setValidateStringAsDouble(boolean validateStringAsDouble)
    {
        this.validateStringAsDouble = validateStringAsDouble;
        return this;
    }

    public boolean isSmartTeardown()
    {
        return smartTeardown;
    }

    @ConfigDescription("When set to false, temporary tables are not dropped if verification fails and both control and test query succeeds")
    @Config("smart-teardown")
    public VerifierConfig setSmartTeardown(boolean smartTeardown)
    {
        this.smartTeardown = smartTeardown;
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

    public boolean isSetupOnMainClusters()
    {
        return setupOnMainClusters;
    }

    @ConfigDescription("If true, run control/test setup queries on control/test clusters. Otherwise, run setup queries on the help cluster.")
    @Config("setup-on-main-clusters")
    public VerifierConfig setSetupOnMainClusters(boolean setupOnMainClusters)
    {
        this.setupOnMainClusters = setupOnMainClusters;
        return this;
    }

    public boolean isTeardownOnMainClusters()
    {
        return teardownOnMainClusters;
    }

    @ConfigDescription("If true, run control/test teardown queries on control/test clusters. Otherwise, run teardown queries on the help cluster.")
    @Config("teardown-on-main-clusters")
    public VerifierConfig setTeardownOnMainClusters(boolean teardownOnMainClusters)
    {
        this.teardownOnMainClusters = teardownOnMainClusters;
        return this;
    }

    public boolean isSkipControl()
    {
        return skipControl;
    }

    @ConfigDescription("Skip control queries and result comparison, only run test queries.")
    @Config("skip-control")
    public VerifierConfig setSkipControl(boolean skipControl)
    {
        this.skipControl = skipControl;
        return this;
    }

    @ConfigDescription("Skip checksum, only run control and test queries.")
    @Config("skip-checksum")
    public VerifierConfig setSkipChecksum(boolean skipChecksum)
    {
        this.skipChecksum = skipChecksum;
        return this;
    }

    public boolean isSkipChecksum()
    {
        return skipChecksum;
    }

    @ConfigDescription("Run Determinism Analysis on test rather than control. If the Determinism Analysis returns 'non-deterministic', we then re-run it in control")
    @Config("run-determinism-analysis-on-test")
    public VerifierConfig setRunDeterminismAnalysisOnTest(boolean runDeterminismAnalysisOnTest)
    {
        this.runDeterminismAnalysisOnTest = runDeterminismAnalysisOnTest;
        return this;
    }

    public boolean isRunDeterminismAnalysisOnTest()
    {
        return runDeterminismAnalysisOnTest;
    }

    public boolean isExplain()
    {
        return explain;
    }

    @ConfigDescription("If true, run explain verification on the given queries.")
    @Config("explain")
    public VerifierConfig setExplain(boolean explain)
    {
        this.explain = explain;
        return this;
    }

    public boolean isConcurrentControlAndTest()
    {
        return concurrentControlAndTest;
    }

    @ConfigDescription("Run control and test query concurrently")
    @Config("concurrent-control-and-test")
    public VerifierConfig setConcurrentControlAndTest(boolean concurrentControlAndTest)
    {
        this.concurrentControlAndTest = concurrentControlAndTest;
        return this;
    }

    public boolean isSaveSnapshot()
    {
        return saveSnapshot;
    }
    @ConfigDescription("Save control query results to database as snapshots.")
    @Config("save-snapshot")
    public VerifierConfig setSaveSnapshot(boolean saveSnapshot)
    {
        this.saveSnapshot = saveSnapshot;
        return this;
    }

    public boolean isExtendedVerification()
    {
        return extendedVerification;
    }

    @ConfigDescription("Run extended verification logic in verifier.")
    @Config("extended-verification")
    public VerifierConfig setExtendedVerification(boolean extendedVerification)
    {
        this.extendedVerification = extendedVerification;
        return this;
    }

    public String getRunningMode()
    {
        return runningMode;
    }

    @ConfigDescription("Set running mode to 'control-test' or 'query-bank'.")
    @Config("running-mode")
    public VerifierConfig setRunningMode(String runningMode)
    {
        this.runningMode = runningMode;
        if (QUERY_BANK_MODE.toLowerCase(ENGLISH).equals(runningMode)) {
            skipControl = true;
        }
        return this;
    }

    public Multimap<String, FunctionCallSubstitute> getFunctionSubstitutes()
    {
        return functionSubstitutes;
    }

    @ConfigDescription("Specification of function substitutions, in the format of" +
            " /foo(c0,_)/bar(c0)/,/fred(c0,c1)/baz(qux(c1,c0))/,/foobar(c0)/if(qux(c1),bar(c0),baz(c1))/,...," +
            " where foo(c0, _) would be substituted by bar(c0), with the declared arguments applied" +
            " to the corresponding positions. Concatenate function substitutions with comma.")
    @Config("function-substitutes")
    public VerifierConfig setFunctionSubstitutes(String functionSubstitutes)
    {
        this.functionSubstitutes = FunctionCallRewriter.validateAndConstructFunctionCallSubstituteMap(functionSubstitutes);
        return this;
    }
}
