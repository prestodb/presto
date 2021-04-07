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

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestVerifierConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(VerifierConfig.class)
                .setWhitelist(null)
                .setBlacklist(null)
                .setSourceQuerySupplier("mysql")
                .setEventClients("human-readable")
                .setJsonEventLogFile(null)
                .setHumanReadableEventLogFile(null)
                .setTestId(null)
                .setTestName(null)
                .setMaxConcurrency(10)
                .setSuiteRepetitions(1)
                .setQueryRepetitions(1)
                .setRelativeErrorMargin(1e-4)
                .setAbsoluteErrorMargin(1e-12)
                .setSmartTeardown(false)
                .setVerificationResubmissionLimit(6)
                .setSetupOnMainClusters(true)
                .setTeardownOnMainClusters(true)
                .setSkipControl(false)
                .setExplain(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("whitelist", "a,b,c")
                .put("blacklist", "b,d,f")
                .put("source-query.supplier", "custom-supplier")
                .put("event-clients", "json,human-readable")
                .put("json.log-file", "verifier-json.log")
                .put("human-readable.log-file", "verifier-human-readable.log")
                .put("test-id", "12345")
                .put("test-name", "experiment")
                .put("max-concurrency", "100")
                .put("suite-repetitions", "2")
                .put("query-repetitions", "3")
                .put("relative-error-margin", "2e-5")
                .put("absolute-error-margin", "1e-14")
                .put("smart-teardown", "true")
                .put("verification-resubmission.limit", "1")
                .put("setup-on-main-clusters", "false")
                .put("teardown-on-main-clusters", "false")
                .put("skip-control", "true")
                .put("explain", "true")
                .build();
        VerifierConfig expected = new VerifierConfig()
                .setWhitelist("a,b,c")
                .setBlacklist("b,d,f")
                .setSourceQuerySupplier("custom-supplier")
                .setEventClients("json,human-readable")
                .setJsonEventLogFile("verifier-json.log")
                .setHumanReadableEventLogFile("verifier-human-readable.log")
                .setTestId("12345")
                .setTestName("experiment")
                .setMaxConcurrency(100)
                .setSuiteRepetitions(2)
                .setQueryRepetitions(3)
                .setRelativeErrorMargin(2e-5)
                .setAbsoluteErrorMargin(1e-14)
                .setSmartTeardown(true)
                .setVerificationResubmissionLimit(1)
                .setSetupOnMainClusters(false)
                .setTeardownOnMainClusters(false)
                .setSkipControl(true)
                .setExplain(true);

        assertFullMapping(properties, expected);
    }
}
