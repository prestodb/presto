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
package com.facebook.presto.plugin.oracle;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.units.Duration;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.math.RoundingMode;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class OracleConfig
{
    private boolean synonymsEnabled;
    private boolean connectionPoolEnabled;
    private int varcharMaxSize = 4000;
    private int timestampDefaultPrecision = 6;
    private int numberDefaultScale = 10;
    private RoundingMode numberRoundingMode = RoundingMode.HALF_UP;
    private int connectionPoolMinSize = 1;
    private int connectionPoolMaxSize = 30;
    private Duration inactiveConnectionTimeout = new Duration(20, MINUTES);
    private Duration connectionPoolWaitDuration = new Duration(3, SECONDS);
    private Integer fetchSize = 1000;

    @NotNull
    public boolean isSynonymsEnabled()
    {
        return synonymsEnabled;
    }

    @Config("oracle.synonyms.enabled")
    public OracleConfig setSynonymsEnabled(boolean enabled)
    {
        this.synonymsEnabled = enabled;
        return this;
    }

    @Min(0)
    @Max(38)
    public int getNumberDefaultScale()
    {
        return numberDefaultScale;
    }

    @Config("oracle.number.default-scale")
    public OracleConfig setNumberDefaultScale(int numberDefaultScale)
    {
        this.numberDefaultScale = numberDefaultScale;
        return this;
    }

    @NotNull
    public RoundingMode getNumberRoundingMode()
    {
        return numberRoundingMode;
    }

    @Config("oracle.number.rounding-mode")
    public OracleConfig setNumberRoundingMode(RoundingMode numberRoundingMode)
    {
        this.numberRoundingMode = numberRoundingMode;
        return this;
    }

    @Min(4000)
    public int getVarcharMaxSize()
    {
        return varcharMaxSize;
    }

    @Config("oracle.varchar.max-size")
    public OracleConfig setVarcharMaxSize(int varcharMaxSize)
    {
        this.varcharMaxSize = varcharMaxSize;
        return this;
    }

    @Min(0)
    @Max(9)
    public int getTimestampDefaultPrecision()
    {
        return timestampDefaultPrecision;
    }

    @Config("oracle.timestamp.precision")
    public OracleConfig setTimestampDefaultPrecision(int timestampDefaultPrecision)
    {
        this.timestampDefaultPrecision = timestampDefaultPrecision;
        return this;
    }

    @NotNull
    public boolean isConnectionPoolEnabled()
    {
        return connectionPoolEnabled;
    }

    @Config("oracle.connection-pool.enabled")
    public OracleConfig setConnectionPoolEnabled(boolean enabled)
    {
        this.connectionPoolEnabled = enabled;
        return this;
    }

    @Min(0)
    public int getConnectionPoolMinSize()
    {
        return connectionPoolMinSize;
    }

    @Config("oracle.connection-pool.min-size")
    public OracleConfig setConnectionPoolMinSize(int connectionPoolMinSize)
    {
        this.connectionPoolMinSize = connectionPoolMinSize;
        return this;
    }

    @Min(1)
    public int getConnectionPoolMaxSize()
    {
        return connectionPoolMaxSize;
    }

    @Config("oracle.connection-pool.max-size")
    public OracleConfig setConnectionPoolMaxSize(int connectionPoolMaxSize)
    {
        this.connectionPoolMaxSize = connectionPoolMaxSize;
        return this;
    }

    @NotNull
    public Duration getInactiveConnectionTimeout()
    {
        return inactiveConnectionTimeout;
    }

    @Config("oracle.connection-pool.inactive-timeout")
    @ConfigDescription("How long a connection in the pool can remain idle before it is closed")
    public OracleConfig setInactiveConnectionTimeout(Duration inactiveConnectionTimeout)
    {
        this.inactiveConnectionTimeout = inactiveConnectionTimeout;
        return this;
    }

    @NotNull
    public Duration getConnectionPoolWaitDuration()
    {
        return connectionPoolWaitDuration;
    }

    @Config("oracle.connection-pool.wait-duration")
    @ConfigDescription("Maximum amount of time a request will wait to obtain an available connection from the pool if all connections are currently in use")
    public OracleConfig setConnectionPoolWaitDuration(Duration connectionPoolWaitDuration)
    {
        this.connectionPoolWaitDuration = connectionPoolWaitDuration;
        return this;
    }

    public Optional<@Min(0) Integer> getFetchSize()
    {
        return Optional.ofNullable(fetchSize);
    }

    @Config("oracle.fetch-size")
    public OracleConfig setFetchSize(Integer fetchSize)
    {
        this.fetchSize = fetchSize;
        return this;
    }

    @AssertTrue(message = "Pool min size cannot be larger than max size")
    public boolean isPoolSizedProperly()
    {
        return getConnectionPoolMaxSize() >= getConnectionPoolMinSize();
    }
}
