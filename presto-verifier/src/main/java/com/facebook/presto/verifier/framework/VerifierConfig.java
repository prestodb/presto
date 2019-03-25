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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.MINUTES;

public class VerifierConfig
{
    private String controlJdbcUrl;
    private String testJdbcUrl;

    private Duration controlTimeout = new Duration(10, MINUTES);
    private Duration testTimeout = new Duration(30, MINUTES);
    private Duration metadataTimeout = new Duration(3, MINUTES);
    private Duration checksumTimeout = new Duration(20, MINUTES);

    private double relativeErrorMargin = 1e-4;

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

    @Min(0)
    public double getRelativeErrorMargin()
    {
        return relativeErrorMargin;
    }

    @Config("relative-error-margin")
    public VerifierConfig setRelativeErrorMargin(double relativeErrorMargin)
    {
        this.relativeErrorMargin = relativeErrorMargin;
        return this;
    }
}
