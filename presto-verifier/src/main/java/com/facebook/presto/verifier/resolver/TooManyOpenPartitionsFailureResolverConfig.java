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
package com.facebook.presto.verifier.resolver;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.units.Duration;
import com.facebook.airlift.units.MinDuration;
import jakarta.validation.constraints.Min;

import static java.util.concurrent.TimeUnit.MINUTES;

public class TooManyOpenPartitionsFailureResolverConfig
{
    private int maxBucketsPerWriter = 100;
    private Duration clusterSizeExpiration = new Duration(30, MINUTES);

    @Min(1)
    public int getMaxBucketsPerWriter()
    {
        return maxBucketsPerWriter;
    }

    @Config("failure-resolver.max-buckets-per-writer")
    public TooManyOpenPartitionsFailureResolverConfig setMaxBucketsPerWriter(int maxBucketsPerWriter)
    {
        this.maxBucketsPerWriter = maxBucketsPerWriter;
        return this;
    }

    @MinDuration("1m")
    public Duration getClusterSizeExpiration()
    {
        return clusterSizeExpiration;
    }

    @Config("failure-resolver.cluster-size-expiration")
    public TooManyOpenPartitionsFailureResolverConfig setClusterSizeExpiration(Duration clusterSizeExpiration)
    {
        this.clusterSizeExpiration = clusterSizeExpiration;
        return this;
    }
}
