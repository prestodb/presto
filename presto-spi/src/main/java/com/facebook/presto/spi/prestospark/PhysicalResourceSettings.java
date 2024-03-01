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

/**
 *  Class for storing physical resource settings.
 *  Resource settings could have conflicting overrides coming from either presto or spark.
 *  The class holds the final values that needs to be applied to the query.
 */
public class PhysicalResourceSettings
{
    private final int hashPartitionCount;
    private final int maxExecutorCount;
    private final boolean hashPartitionCountAutoTuned;
    private final boolean maxExecutorCountAutoTuned;

    @JsonCreator
    public PhysicalResourceSettings(
            @JsonProperty("hashPartitionCount") int hashPartitionCount,
            @JsonProperty("maxExecutorCount") int maxExecutorCount,
            @JsonProperty("hashPartitionCountAutoTuned") boolean hashPartitionCountAutoTuned,
            @JsonProperty("maxExecutorCountAutoTuned") boolean maxExecutorCountAutoTuned)
    {
        this.hashPartitionCount = hashPartitionCount;
        this.maxExecutorCount = maxExecutorCount;
        this.hashPartitionCountAutoTuned = hashPartitionCountAutoTuned;
        this.maxExecutorCountAutoTuned = maxExecutorCountAutoTuned;
    }

    @JsonProperty
    public int getHashPartitionCount()
    {
        return hashPartitionCount;
    }

    @JsonProperty
    public int getMaxExecutorCount()
    {
        return maxExecutorCount;
    }

    @JsonProperty
    public boolean isHashPartitionCountAutoTuned()
    {
        return hashPartitionCountAutoTuned;
    }

    @JsonProperty
    public boolean isMaxExecutorCountAutoTuned()
    {
        return maxExecutorCountAutoTuned;
    }

    @Override
    public String toString()
    {
        return "PhysicalResourceSettings{" +
                "hashPartitionCount=" + hashPartitionCount +
                ", maxExecutorCount=" + maxExecutorCount +
                ", hashPartitionCountAutoTuned=" + hashPartitionCountAutoTuned +
                ", maxExecutorCountAutoTuned=" + maxExecutorCountAutoTuned +
                '}';
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
        PhysicalResourceSettings that = (PhysicalResourceSettings) o;
        return hashPartitionCount == that.hashPartitionCount &&
                maxExecutorCount == that.maxExecutorCount &&
                hashPartitionCountAutoTuned == that.hashPartitionCountAutoTuned &&
                maxExecutorCountAutoTuned == that.maxExecutorCountAutoTuned;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(hashPartitionCount, maxExecutorCount, hashPartitionCountAutoTuned, maxExecutorCountAutoTuned);
    }
}
