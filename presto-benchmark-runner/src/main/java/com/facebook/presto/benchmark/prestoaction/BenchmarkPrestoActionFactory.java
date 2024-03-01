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
package com.facebook.presto.benchmark.prestoaction;

import com.facebook.presto.benchmark.framework.BenchmarkQuery;
import com.facebook.presto.benchmark.retry.RetryConfig;
import com.google.inject.Inject;

import static java.util.Objects.requireNonNull;

public class BenchmarkPrestoActionFactory
        implements PrestoActionFactory
{
    private final SqlExceptionClassifier exceptionClassifier;
    private final PrestoClusterConfig clusterConfig;
    private final RetryConfig networkRetryConfig;

    @Inject
    public BenchmarkPrestoActionFactory(
            SqlExceptionClassifier exceptionClassifier,
            PrestoClusterConfig clusterConfig,
            RetryConfig networkRetryConfig)
    {
        this.exceptionClassifier = requireNonNull(exceptionClassifier, "exceptionClassifier is null");
        this.clusterConfig = requireNonNull(clusterConfig, "clusterConfig is null");
        this.networkRetryConfig = requireNonNull(networkRetryConfig, "networkRetryConfig is null");
    }

    @Override
    public PrestoAction get(BenchmarkQuery benchmarkQuery)
    {
        return new JdbcPrestoAction(
                exceptionClassifier,
                benchmarkQuery,
                clusterConfig,
                networkRetryConfig);
    }
}
