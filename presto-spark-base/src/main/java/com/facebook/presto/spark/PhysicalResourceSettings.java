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
package com.facebook.presto.spark;

import java.util.OptionalInt;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

/**
 *  Class for storing physical resource settings.
 *  Resource settings could have conflicting overrides coming from either presto or spark.
 *  The class holds the final values that needs to be applied to the query.
 */
public class PhysicalResourceSettings
{
    private final OptionalInt maxExecutorCount;
    private final int hashPartitionCount;

    public PhysicalResourceSettings(int hashPartitionCount, OptionalInt maxExecutorCount)
    {
        checkArgument(maxExecutorCount.orElse(0) >= 0 && hashPartitionCount >= 0, "executorCount and hashPartitionCount should be positive");
        this.maxExecutorCount = maxExecutorCount;
        this.hashPartitionCount = hashPartitionCount;
    }

    public int getHashPartitionCount()
    {
        return hashPartitionCount;
    }

    /**
     * maxExecutorCount is an optional field, the value is based on resource allocation tuning property
     * An empty value, means resource tuning is disabled and values from SparkConf will be used.
     * When not empty, resource tuning is enabled and calculated by {@link PrestoSparkPhysicalResourceCalculator} based on the query plan
     */
    public OptionalInt getMaxExecutorCount()
    {
        return maxExecutorCount;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("maxExecutorCount", maxExecutorCount)
                .add("hashPartitionCount", hashPartitionCount)
                .toString();
    }
}
