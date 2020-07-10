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
package com.facebook.presto.verifier.prestoaction;

import com.facebook.presto.verifier.framework.QueryConfiguration;
import com.facebook.presto.verifier.framework.VerificationContext;
import com.facebook.presto.verifier.retry.RetryConfig;

import static java.util.Objects.requireNonNull;

public class JdbcPrestoActionFactory
        implements PrestoActionFactory
{
    private final SqlExceptionClassifier exceptionClassifier;
    private final PrestoActionConfig prestoActionConfig;
    private final RetryConfig networkRetryConfig;
    private final RetryConfig prestoRetryConfig;

    public JdbcPrestoActionFactory(
            SqlExceptionClassifier exceptionClassifier,
            PrestoActionConfig prestoActionConfig,
            RetryConfig networkRetryConfig,
            RetryConfig prestoRetryConfig)
    {
        this.exceptionClassifier = requireNonNull(exceptionClassifier, "exceptionClassifier is null");
        this.prestoActionConfig = requireNonNull(prestoActionConfig, "prestoClusterConfig is null");
        this.networkRetryConfig = requireNonNull(networkRetryConfig, "networkRetryConfig is null");
        this.prestoRetryConfig = requireNonNull(prestoRetryConfig, "prestoRetryConfig is null");
    }

    @Override
    public JdbcPrestoAction create(QueryConfiguration queryConfiguration, VerificationContext verificationContext)
    {
        return new JdbcPrestoAction(
                exceptionClassifier,
                queryConfiguration,
                verificationContext,
                prestoActionConfig,
                networkRetryConfig,
                prestoRetryConfig);
    }
}
