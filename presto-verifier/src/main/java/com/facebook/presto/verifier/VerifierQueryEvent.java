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

import com.google.common.collect.ImmutableList;
import io.airlift.event.client.EventField;
import io.airlift.event.client.EventType;

import javax.annotation.concurrent.Immutable;

import java.util.List;

@Immutable
@EventType("VerifierQuery")
public class VerifierQueryEvent
{
    private final String suite;
    private final String runId;
    private final String source;
    private final String name;
    private final boolean failed;

    private final String testCatalog;
    private final String testSchema;
    private final List<String> testSetupQueries;
    private final String testQuery;
    private final List<String> testTeardownQueries;
    private final String testQueryId;
    private final Double testCpuTimeSecs;
    private final Double testWallTimeSecs;

    private final String controlCatalog;
    private final String controlSchema;
    private final List<String> controlSetupQueries;
    private final String controlQuery;
    private final List<String> controlTeardownQueries;
    private final String controlQueryId;
    private final Double controlCpuTimeSecs;
    private final Double controlWallTimeSecs;

    private final String errorMessage;

    public VerifierQueryEvent(
            String suite,
            String runId,
            String source,
            String name,
            boolean failed,
            String testCatalog,
            String testSchema,
            List<String> testSetupQueries,
            String testQuery,
            List<String> testTeardownQueries,
            String testQueryId,
            Double testCpuTimeSecs,
            Double testWallTimeSecs,
            String controlCatalog,
            String controlSchema,
            List<String> controlSetupQueries,
            String controlQuery,
            List<String> controlTeardownQueries,
            String controlQueryId,
            Double controlCpuTimeSecs,
            Double controlWallTimeSecs,
            String errorMessage)
    {
        this.suite = suite;
        this.runId = runId;
        this.source = source;
        this.name = name;
        this.failed = failed;

        this.testCatalog = testCatalog;
        this.testSchema = testSchema;
        this.testSetupQueries = ImmutableList.copyOf(testSetupQueries);
        this.testQuery = testQuery;
        this.testTeardownQueries = ImmutableList.copyOf(testTeardownQueries);
        this.testQueryId = testQueryId;
        this.testCpuTimeSecs = testCpuTimeSecs;
        this.testWallTimeSecs = testWallTimeSecs;

        this.controlCatalog = controlCatalog;
        this.controlSchema = controlSchema;
        this.controlSetupQueries = ImmutableList.copyOf(controlSetupQueries);
        this.controlQuery = controlQuery;
        this.controlTeardownQueries = ImmutableList.copyOf(controlTeardownQueries);
        this.controlQueryId = controlQueryId;
        this.controlCpuTimeSecs = controlCpuTimeSecs;
        this.controlWallTimeSecs = controlWallTimeSecs;

        this.errorMessage = errorMessage;
    }

    @EventField
    public String getSuite()
    {
        return suite;
    }

    @EventField
    public String getRunId()
    {
        return runId;
    }

    @EventField
    public String getSource()
    {
        return source;
    }

    @EventField
    public String getName()
    {
        return name;
    }

    @EventField
    public boolean isFailed()
    {
        return failed;
    }

    @EventField
    public String getTestCatalog()
    {
        return testCatalog;
    }

    @EventField
    public String getTestSchema()
    {
        return testSchema;
    }

    @EventField
    public String getTestQuery()
    {
        return testQuery;
    }

    @EventField
    public String getTestQueryId()
    {
        return testQueryId;
    }

    @EventField
    public Double getTestCpuTimeSecs()
    {
        return testCpuTimeSecs;
    }

    @EventField
    public Double getTestWallTimeSecs()
    {
        return testWallTimeSecs;
    }

    @EventField
    public String getControlCatalog()
    {
        return controlCatalog;
    }

    @EventField
    public String getControlSchema()
    {
        return controlSchema;
    }

    @EventField
    public String getControlQuery()
    {
        return controlQuery;
    }

    @EventField
    public String getControlQueryId()
    {
        return controlQueryId;
    }

    @EventField
    public Double getControlCpuTimeSecs()
    {
        return controlCpuTimeSecs;
    }

    @EventField
    public Double getControlWallTimeSecs()
    {
        return controlWallTimeSecs;
    }

    @EventField
    public String getErrorMessage()
    {
        return errorMessage;
    }

    @EventField
    public List<String> getTestSetupQueries()
    {
        return testSetupQueries;
    }

    @EventField
    public List<String> getTestTeardownQueries()
    {
        return testTeardownQueries;
    }

    @EventField
    public List<String> getControlSetupQueries()
    {
        return controlSetupQueries;
    }

    @EventField
    public List<String> getControlTeardownQueries()
    {
        return controlTeardownQueries;
    }
}
