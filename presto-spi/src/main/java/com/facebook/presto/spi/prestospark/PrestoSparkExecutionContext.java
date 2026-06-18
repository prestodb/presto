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
package com.facebook.presto.spi.prestospark;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class PrestoSparkExecutionContext
{
    private final PhysicalResourceSettings physicalResourceSettings;

    @JsonCreator
    public PrestoSparkExecutionContext(@JsonProperty("physicalResourceSettings") PhysicalResourceSettings physicalResourceSettings)
    {
        this.physicalResourceSettings = requireNonNull(physicalResourceSettings, "physicalResourceSettings is null");
    }

    @JsonProperty
    public PhysicalResourceSettings getPhysicalResourceSettings()
    {
        return physicalResourceSettings;
    }

    public static PrestoSparkExecutionContext create(
            int hashPartitionCount,
            int maxExecutorCount,
            boolean hashPartitionCountAutoTuned,
            boolean maxExecutorCountAutoTuned)
    {
        PhysicalResourceSettings physicalResourceSettings = new PhysicalResourceSettings(
                hashPartitionCount,
                maxExecutorCount,
                hashPartitionCountAutoTuned,
                maxExecutorCountAutoTuned);
        return new PrestoSparkExecutionContext(physicalResourceSettings);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrestoSparkExecutionContext that = (PrestoSparkExecutionContext) o;
        return physicalResourceSettings.equals(that.physicalResourceSettings);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(physicalResourceSettings);
    }
}
